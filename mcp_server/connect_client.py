import os
import requests

CONNECT_REST_URL = os.getenv("CONNECT_REST_URL", "http://connect:8083").rstrip("/")

def list_connectors(timeout_s: int = 5) -> dict:
    url = f"{CONNECT_REST_URL}/connectors"
    r = requests.get(url, timeout=timeout_s)
    r.raise_for_status()
    return {"connect_rest_url": CONNECT_REST_URL, "connectors": r.json()}

def connector_status(name: str, timeout_s: int = 5) -> dict:
    url = f"{CONNECT_REST_URL}/connectors/{name}/status"
    r = requests.get(url, timeout=timeout_s)
    r.raise_for_status()
    return {"connect_rest_url": CONNECT_REST_URL, "status": r.json()}
