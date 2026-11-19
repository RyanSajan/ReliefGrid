import hashlib
import os

import httpx
from fastapi import HTTPException


def env(name, default):
    return os.getenv(name, default)


def shard(key):
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % 3


async def call(method, url, *, timeout=10, **kwargs):
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            return await client.request(method, url, **kwargs)
    except httpx.RequestError as exc:
        raise HTTPException(503, 'upstream service unavailable') from exc

