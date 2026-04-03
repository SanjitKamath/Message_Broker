"""AES-GCM encryption support for :mod:`lyik_messaging`."""

from __future__ import annotations

import base64
import binascii
import json
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from faststream.middlewares import BaseMiddleware
from faststream.message import StreamMessage
from faststream.response import BatchPublishCommand, PublishCommand

from .exceptions import ConfigurationError, EncryptionError

if TYPE_CHECKING:
    from faststream._internal.context.repository import ContextRepo


_NONCE_SIZE = 12
_VALID_KEY_SIZES = {16, 24, 32}
_ENCRYPTED_HEADER = "x-lyik-encrypted"


def normalize_aes_key(aes_key: str) -> bytes:
    """Normalize a user-supplied AES key string into raw key bytes."""

    key = aes_key.strip()
    if not key:
        raise ConfigurationError("aes_key must not be empty.")

    for decoder in (bytes.fromhex, _decode_base64, lambda value: value.encode("utf-8")):
        try:
            candidate = decoder(key)  # type: ignore[arg-type]
        except (ValueError, binascii.Error):
            continue
        if len(candidate) in _VALID_KEY_SIZES:
            return candidate

    raise ConfigurationError(
        "aes_key must decode to 16, 24, or 32 bytes using base64, hex, or UTF-8 text."
    )


def build_aesgcm_middleware_factory(aes_key: bytes) -> Callable[[object | None], "AesGcmMiddleware"]:
    """Create a FastStream middleware factory that closes over the AES key."""

    def factory(msg: object | None, /, *, context: "ContextRepo") -> AesGcmMiddleware:
        return AesGcmMiddleware(msg, context=context, aes_key=aes_key)

    return factory


class AesGcmMiddleware(BaseMiddleware[object, object]):
    """FastStream middleware that encrypts outbound payloads and decrypts inbound ones."""

    def __init__(self, msg: object | None, /, *, context: "ContextRepo", aes_key: bytes) -> None:
        super().__init__(msg, context=context)
        self._cipher = AESGCM(aes_key)

    async def on_publish(self, msg: PublishCommand) -> PublishCommand:
        _mark_encrypted(msg)
        correlation_id = _resolve_correlation_id(msg.headers, msg.correlation_id)

        if isinstance(msg, BatchPublishCommand):
            encrypted_bodies = tuple(
                self._encrypt_body(body, correlation_id=correlation_id)
                for body in msg.batch_bodies
            )
            msg.batch_bodies = encrypted_bodies
            return msg

        msg.body = self._encrypt_body(msg.body, correlation_id=correlation_id)
        return msg

    async def on_consume(self, msg: StreamMessage[object]) -> StreamMessage[object]:
        if not _is_encrypted(msg.headers):
            return msg

        if not isinstance(msg.body, (bytes, bytearray, memoryview)):
            raise EncryptionError("Encrypted messages must arrive as bytes.")

        correlation_id = _resolve_correlation_id(msg.headers, msg.correlation_id)
        decrypted = self._decrypt_bytes(bytes(msg.body), correlation_id=correlation_id)
        msg.body = decrypted
        msg.content_type = "application/json"
        msg.clear_cache()
        return msg

    def _encrypt_body(self, body: object, *, correlation_id: str | None) -> bytes:
        plaintext = _serialize_payload(body)
        nonce = os.urandom(_NONCE_SIZE)
        aad = correlation_id.encode("utf-8") if correlation_id else None
        ciphertext = self._cipher.encrypt(nonce, plaintext, aad)
        return nonce + ciphertext

    def _decrypt_bytes(self, payload: bytes, *, correlation_id: str | None) -> bytes:
        if len(payload) <= _NONCE_SIZE:
            raise EncryptionError("Encrypted payload is too short to contain a nonce.")

        nonce = payload[:_NONCE_SIZE]
        ciphertext = payload[_NONCE_SIZE:]
        aad = correlation_id.encode("utf-8") if correlation_id else None

        try:
            return self._cipher.decrypt(nonce, ciphertext, aad)
        except Exception as exc:  # pragma: no cover - cryptography raises typed exceptions
            raise EncryptionError("Failed to decrypt the incoming message payload.") from exc


def _mark_encrypted(command: PublishCommand) -> None:
    headers = dict(command.headers)
    headers[_ENCRYPTED_HEADER] = "true"
    command.headers = headers


def _is_encrypted(headers: Mapping[str, object]) -> bool:
    value = headers.get(_ENCRYPTED_HEADER)
    return isinstance(value, str) and value.lower() == "true"


def _resolve_correlation_id(headers: Mapping[str, object], fallback: str | None) -> str | None:
    value = headers.get("correlation_id")
    if isinstance(value, str) and value.strip():
        return value
    return fallback


def _serialize_payload(payload: object) -> bytes:
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, bytearray):
        return bytes(payload)
    if isinstance(payload, memoryview):
        return payload.tobytes()
    if isinstance(payload, str):
        return payload.encode("utf-8")

    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")


def _json_default(value: object) -> object:
    if isinstance(value, (datetime, date, UUID)):
        return value.isoformat()
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if isinstance(value, set):
        return sorted(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _decode_base64(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.b64decode(value + padding, altchars=b"-_", validate=True)