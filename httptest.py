import httpx
from concurrent.futures import ThreadPoolExecutor

with httpx.Client(limits=httpx.Limits(max_connections=5)) as client:
    with ThreadPoolExecutor() as ex:
        for response in ex.map(lambda i: client.get("https://httpbin.org"), range(1000)):
            print(response)