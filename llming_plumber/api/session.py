"""Lightweight cookie-based session for the Plumber UI.

Assigns a stable user handle on first visit via a signed cookie.
No database required — identity lives entirely in the cookie.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from typing import Any

from fastapi import APIRouter, Request, Response
from pydantic import BaseModel

from llming_plumber.config import settings

router = APIRouter()

_COOKIE_NAME = "plumber_session"
_COOKIE_MAX_AGE = 60 * 60 * 24 * 365  # 1 year


def _sign(payload: str) -> str:
    """Create an HMAC signature for the payload."""
    key = (settings.secret_key or "plumber-dev-key").encode()
    sig = hmac.new(key, payload.encode(), hashlib.sha256).hexdigest()[:16]
    return f"{payload}.{sig}"


def _verify(token: str) -> str | None:
    """Verify a signed token, return payload or None."""
    if "." not in token:
        return None
    payload, sig = token.rsplit(".", 1)
    key = (settings.secret_key or "plumber-dev-key").encode()
    expected = hmac.new(key, payload.encode(), hashlib.sha256).hexdigest()[:16]
    if hmac.compare_digest(sig, expected):
        return payload
    return None


def _default_handle() -> str:
    """Return the dev user handle from env, or generate a random one."""
    dev_handle = os.environ.get("PLUMBER_DEV_USER_HANDLE")
    if dev_handle:
        return dev_handle
    return f"plumber-{secrets.token_hex(2)}"


def _dev_email() -> str:
    """Return the dev user email from env, or empty string."""
    return os.environ.get("PLUMBER_DEV_USER_EMAIL", "")


def get_user_handle(request: Request) -> str:
    """Extract user handle from request cookie, or return empty string."""
    token = request.cookies.get(_COOKIE_NAME, "")
    if token:
        payload = _verify(token)
        if payload:
            return payload
    return ""


@router.get("/me")
async def get_me(request: Request, response: Response) -> dict[str, Any]:
    """Return the current user's handle. Creates one on first visit."""
    token = request.cookies.get(_COOKIE_NAME, "")
    handle = ""
    is_new = False

    if token:
        handle = _verify(token) or ""

    if not handle:
        handle = _default_handle()
        is_new = True
        signed = _sign(handle)
        response.set_cookie(
            _COOKIE_NAME,
            signed,
            max_age=_COOKIE_MAX_AGE,
            httponly=True,
            samesite="lax",
            path="/",
        )

    return {"handle": handle, "email": _dev_email(), "new": is_new}


class UpdateHandle(BaseModel):
    handle: str


@router.put("/me")
async def update_me(
    body: UpdateHandle, request: Request, response: Response,
) -> dict[str, Any]:
    """Update the user's display handle."""
    handle = body.handle.strip()[:32]
    if not handle:
        handle = _generate_handle()

    signed = _sign(handle)
    response.set_cookie(
        _COOKIE_NAME,
        signed,
        max_age=_COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
        path="/",
    )
    return {"handle": handle}
