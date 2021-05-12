from .network import get_url
import httpx

URLs = {
    "simple": "https://pypi.org/simple/{name}/",
    "json": "https://pypi.org/pypi/{name}/json",
}

def get_project(name: str, type: str, client: httpx.Client):
    template = URLs.get(type)
    if template is None:
        raise RuntimeError(f"Invalid data type: {type}")
    url = template.format(name=name)
    response = get_url(url, client)
    if not response:
        return None
    return name, type, response.content