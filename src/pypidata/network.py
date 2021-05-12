import time
from bisect import bisect_left, bisect_right
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from typing import Optional, Iterator, Tuple, List
from zipfile import BadZipfile, ZipFile

import httpx
from loguru import logger

# API
# ===
#
# get_url(url, client, method, stream, max_tries)
# with get_zip(url, client, chunk_size) as z:
#     ...

MAX_TRIES_TO_FETCH = 10
CONTENT_CHUNK_SIZE = 8192

@logger.catch
def get_url(
    url: str,
    client: Optional[httpx.Client] = None,
    method: str = 'get',
    stream: bool = False,
    max_tries: int = MAX_TRIES_TO_FETCH
) -> Optional[httpx.Response]:
    """Get a URL.

    By default, this will use the module-level get function
    from httpx, but if a Client instance is provided, it will
    use that.

    The 'method' parameter is the HTTP method name.
    If 'stream' is True, the response body is streamed rather
    than being loaded into memory at once.

    Returns the response object.

    Returns None on any form of error.
    Retries up to `max_tries` times if the connection times out.
    """

    if client is None:
        client = httpx
    if stream:
        fetcher = client.stream
    else:
        fetcher = client.request

    tries = 0
    while True:
        try:
            response = fetcher(method, url)
            # 404 (NOT FOUND) is OK, and means no data
            if response.status_code == 404:
                return None
            response.raise_for_status()
            # Treat any status other than 200 (OK) as an error
            if response.status_code != 200:
                logger.error(f"Status {response.status_code} was received while requesting {url!r}.")
                return None
            break
        except httpx.TimeoutException:
            tries += 1
            if tries >= max_tries:
                logger.warning(f"Failed to fetch {url}) - skipping...")
                return None
            time.sleep(1)
            logger.debug(f"Retrying {url!r} ({tries} failed attempt{'s' if tries==1 else ''})")
        except httpx.RequestError as exc:
            logger.error(f"An error {exc!r}occurred while requesting {exc.request.url!r}.")
            return None
        except httpx.HTTPStatusError as exc:
            logger.error(f"Error response {exc.response.status_code} while requesting {exc.request.url!r}.")
            return None
    return response


class LazyZipOverHTTP:
    """File-like object mapped to a ZIP file over HTTP.

    This uses HTTP range requests to lazily fetch the file's content,
    which is supposed to be fed to ZipFile.  If such requests are not
    supported by the server, raise HTTPRangeRequestUnsupported
    during initialization.
    """

    def __init__(
        self,
        url: str,
        client: Optional[httpx.Client] = None,
        chunk_size: int = CONTENT_CHUNK_SIZE
    ):
        self.client = client
        headers = self.get_head(url)
        self._url = url
        self._chunk_size = chunk_size
        self._length = int(headers['Content-Length'])
        self._file = NamedTemporaryFile()
        self.truncate(self._length)
        self._left: List[int] = []
        self._right: List[int] = []
        if 'bytes' not in headers.get('Accept-Ranges', 'none'):
            raise RuntimeError('range request is not supported')
        self._check_zip()


    # HTTP interface
    def get_head(self, url):
        response = get_url(url, method='HEAD', client=self.client)
        if response is None:
            logger.error("Failed to get headers for {url!r}", url=url)
            raise RuntimeError(f"Failed to get headers for {url!r}")
        return response.headers

    def get_body(self, url: str, start: int, end: int, chunk_size: int = CONTENT_CHUNK_SIZE):
        headers = {}
        headers['Range'] = f'bytes={start}-{end}'
        headers['Cache-Control'] = 'no-cache'
        with get_url("GET", url, client=self.client, stream=True, headers=headers) as r:
            for chunk in r.iter_bytes(chunk_size=chunk_size):
                yield chunk


    # File API
    @property
    def mode(self) -> str:
        """Opening mode, which is always rb."""
        return 'rb'

    @property
    def name(self) -> str:
        """Path to the underlying file."""
        return self._file.name

    def seekable(self) -> bool:
        """Return whether random access is supported, which is True."""
        return True

    def close(self) -> None:
        """Close the file."""
        self._file.close()

    @property
    def closed(self) -> bool:
        """Whether the file is closed."""
        return self._file.closed

    def read(self, size: int = -1) -> bytes:
        # type: (int) -> bytes
        """Read up to size bytes from the object and return them.

        As a convenience, if size is unspecified or -1,
        all bytes until EOF are returned.  Fewer than
        size bytes may be returned if EOF is reached.
        """
        download_size = max(size, self._chunk_size)
        start = self.tell()
        length = self._length
        stop = length if size < 0 else min(start+download_size, length)
        start = max(0, stop-download_size)
        self._download(start, stop-1)
        return self._file.read(size)

    def readable(self) -> bool:
        """Return whether the file is readable, which is True."""
        return True

    def seek(self, offset: int, whence: int = 0) -> int:
        """Change stream position and return the new absolute position.

        Seek to offset relative position indicated by whence:
        * 0: Start of stream (the default).  pos should be >= 0;
        * 1: Current position - pos may be negative;
        * 2: End of stream - pos usually negative.
        """
        return self._file.seek(offset, whence)

    def tell(self) -> int:
        """Return the current possition."""
        return self._file.tell()

    def truncate(self, size: Optional[int] = None) -> int:
        """Resize the stream to the given size in bytes.

        If size is unspecified resize to the current position.
        The current stream position isn't changed.

        Return the new file size.
        """
        return self._file.truncate(size)

    def writable(self) -> bool:
        """Return False."""
        return False


    # Context manager API
    def __enter__(self):
        # type: () -> LazyZipOverHTTP
        # Cannot use class name directly in a real annotation...
        self._file.__enter__()
        return self

    def __exit__(self, *exc) -> Optional[bool]:
        return self._file.__exit__(*exc)


    # Helper mkethods
    @contextmanager
    def _stay(self) -> Iterator[None]:
        """Return a context manager keeping the position.

        At the end of the block, seek back to original position.
        """
        pos = self.tell()
        try:
            yield
        finally:
            self.seek(pos)

    def _check_zip(self) -> None:
        """Check and download until the file is a valid ZIP."""
        end = self._length - 1
        for start in reversed(range(0, end, self._chunk_size)):
            self._download(start, end)
            with self._stay():
                try:
                    # For read-only ZIP files, ZipFile only needs
                    # methods read, seek, seekable and tell.
                    ZipFile(self)  # type: ignore
                except BadZipfile:
                    pass
                else:
                    break

    def _merge(self, start: int, end: int, left: int, right: int) -> Iterator[Tuple[int, int]]:
        """Return an iterator of intervals to be fetched.

        Args:
            start (int): Start of needed interval
            end (int): End of needed interval
            left (int): Index of first overlapping downloaded data
            right (int): Index after last overlapping downloaded data
        """
        lslice, rslice = self._left[left:right], self._right[left:right]
        i = start = min([start]+lslice[:1])
        end = max([end]+rslice[-1:])
        for j, k in zip(lslice, rslice):
            if j > i:
                yield i, j-1
            i = k + 1
        if i <= end:
            yield i, end
        self._left[left:right], self._right[left:right] = [start], [end]

    def _download(self, start: int, end: int) -> None:
        """Download bytes from start to end inclusively."""
        logger.debug("Downloading bytes {start}-{end} from {url}", start=start, end=end, url=self._url)
        with self._stay():
            left = bisect_left(self._right, start)
            right = bisect_right(self._left, end)
            for start, end in self._merge(start, end, left, right):
                logger.debug("Getting chunk {start}-{end} from {url}", start=start, end=end, url=self._url)
                self.seek(start)
                for chunk in self.get_body(self._url, start, end, self._chunk_size):
                    self._file.write(chunk)

@contextmanager
def get_zip(
    url: str,
    client: Optional[httpx.Client] = None,
    chunk_size: int = CONTENT_CHUNK_SIZE
):
    with LazyZipOverHTTP(url, client=client, chunk_size=chunk_size) as f:
        z = ZipFile(f)
        yield z
        z.close()

if __name__ == "__main__":
    resp = get_url("https://httpbin.org/get")
    assert resp is not None
    resp = get_url("https://invalid_website.err")
    assert resp is None
    resp = get_url("https://httpbin.org/delay/10", max_tries=1)
    assert resp is None
    resp = get_url("https://httpbin.org/status/201")
    assert resp is None
    resp = get_url("https://httpbin.org/status/500")
    assert resp is None
    resp = get_url(None)
    assert resp is None
    print("Tests completed")
