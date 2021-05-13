from .network import get_url
from .utils import normalize
import json
import re
import httpx
import xmlrpc.client

URLs = {
    "simple": "https://pypi.org/simple/{name}/",
    "json": "https://pypi.org/pypi/{name}/json",
}


def get_simple(name: str, client: httpx.Client):
    response = get_url(f"https://pypi.org/simple/{name}/")
    data = response.content
    if data is None:
        return None, None
    last_line = data.decode("utf-8").splitlines()[-1]
    m = re.fullmatch(r"<!--SERIAL (\d+)-->", last_line)
    if m:
        serial = int(m.group(1))
    else:
        #print(data)
        print("Oops: Last line does not match:", last_line)
        serial = None
    return data, serial

def get_json(name: str, client: httpx.Client):
    response = get_url(f"https://pypi.org/pypi/{name}/json")
    data = response.content
    serial = None
    if data:
        serial = json.loads(data)["last_serial"]
    return data, serial

def get_project(name: str, client: httpx.Client):
    simple_data, simple_serial = get_simple(name, client)
    json_data, json_serial = get_json(name, client)
    if json_serial != simple_serial:
        print("Mismatch")
        return None
    return name, json_serial, simple_data, json_data

def get_packages():
    # Get the data from XMLRPC
    XMLRPC = "https://pypi.org/pypi"
    pypi = xmlrpc.client.ServerProxy(XMLRPC)
    return { normalize(n): (n, s) for (n, s) in pypi.list_packages_with_serial().items() }

if __name__ == "__main__":
    import sys
    with httpx.Client() as c:
        for arg in sys.argv[1:]:
            name, serial, simple_data, json_data = get_project(arg, c)
            print(name, serial, len(simple_data), len(json_data))