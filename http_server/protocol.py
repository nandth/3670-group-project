"""HTTP message parsing and serialization, independent of routing."""

from dataclasses import dataclass, field
from http import HTTPStatus
import re
import socket
from typing import Dict


MAX_HEADER_BYTES = 16 * 1024
MAX_BODY_BYTES = 1024 * 1024
TOKEN = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


class HTTPError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


@dataclass
class Request:
    method: str
    target: str
    version: str
    headers: Dict[str, str]
    body: bytes

    @property
    def keep_alive(self) -> bool:
        tokens = {value.strip().lower() for value in
                  self.headers.get("connection", "").split(",")}
        if "close" in tokens:
            return False
        return self.version == "HTTP/1.1" or "keep-alive" in tokens


@dataclass
class Response:
    status: int = 200
    body: bytes = b""
    headers: Dict[str, str] = field(default_factory=dict)


class ConnectionReader:
    """Keep unread bytes: one recv() may contain part of, or several, requests."""

    def __init__(self, connection: socket.socket):
        self.connection = connection
        self.buffer = bytearray()

    def read_request(self):
        while b"\r\n\r\n" not in self.buffer:
            if len(self.buffer) >= MAX_HEADER_BYTES:
                raise HTTPError(431, "Request headers too large")
            chunk = self.connection.recv(4096)
            if not chunk:
                if self.buffer:
                    raise HTTPError(400, "Incomplete request headers")
                return None
            self.buffer.extend(chunk)

        end = self.buffer.index(b"\r\n\r\n") + 4
        if end > MAX_HEADER_BYTES:
            raise HTTPError(431, "Request headers too large")
        lines = bytes(self.buffer[:end - 4]).decode("iso-8859-1").split("\r\n")
        del self.buffer[:end]
        parts = lines[0].split(" ")
        if len(parts) != 3:
            raise HTTPError(400, "Malformed request line")
        method, target, version = parts
        if not TOKEN.fullmatch(method) or not target.startswith("/") or any(
            ord(char) <= 32 or ord(char) >= 127 for char in target
        ):
            raise HTTPError(400, "Invalid method or request target")
        if version not in ("HTTP/1.0", "HTTP/1.1"):
            raise HTTPError(505, "Only HTTP/1.0 and HTTP/1.1 are supported")

        headers = {}
        for line in lines[1:]:
            name, separator, value = line.partition(":")
            if not separator or not TOKEN.fullmatch(name) or any(
                ord(char) < 32 and char != "\t" or ord(char) == 127
                for char in value
            ):
                raise HTTPError(400, "Malformed header")
            name = name.lower()
            value = value.strip(" \t")
            if name in headers:
                if name in ("content-length", "host"):
                    raise HTTPError(400, "Duplicate framing or Host header")
                value = headers[name] + ", " + value
            headers[name] = value
        if version == "HTTP/1.1" and not headers.get("host"):
            raise HTTPError(400, "HTTP/1.1 requires a Host header")
        if "transfer-encoding" in headers:
            raise HTTPError(501, "Transfer-Encoding is not supported")
        if "expect" in headers:
            raise HTTPError(417, "Expect is not supported")
        length = headers.get("content-length", "0")
        if not re.fullmatch(r"[0-9]+", length):
            raise HTTPError(400, "Invalid Content-Length")
        # Compare before int conversion, including absurdly long decimal strings.
        significant = length.lstrip("0") or "0"
        if len(significant) > len(str(MAX_BODY_BYTES)) or int(significant) > MAX_BODY_BYTES:
            raise HTTPError(413, "Request body too large")
        length = int(significant)
        while len(self.buffer) < length:
            chunk = self.connection.recv(min(4096, length - len(self.buffer)))
            if not chunk:
                raise HTTPError(400, "Incomplete request body")
            self.buffer.extend(chunk)
        body = bytes(self.buffer[:length])
        del self.buffer[:length]
        return Request(method, target, version, headers, body)


def encode_response(response: Response, version="HTTP/1.1", keep_alive=False,
                    head=False) -> bytes:
    """The server owns framing, so handlers cannot accidentally corrupt a stream."""
    status = HTTPStatus(response.status)
    if status.value < 200:
        raise ValueError("Informational responses are not supported")
    if status.value in (204, 205, 304) and response.body:
        raise ValueError("This status must not contain a body")
    headers = {"Content-Type": "text/plain; charset=utf-8"}
    for name, value in response.headers.items():
        if not TOKEN.fullmatch(name) or any(
            ord(char) < 32 or ord(char) == 127 for char in value
        ):
            raise ValueError("Invalid response header")
        if name.lower() in ("content-length", "connection", "transfer-encoding"):
            raise ValueError("Response framing headers belong to the server")
        for existing in list(headers):
            if existing.lower() == name.lower():
                del headers[existing]
        headers[name] = value
    if status.value not in (204, 304):
        headers["Content-Length"] = str(len(response.body))
    headers["Connection"] = "keep-alive" if keep_alive else "close"
    lines = [f"{version} {status.value} {status.phrase}"]
    lines.extend(f"{name}: {value}" for name, value in headers.items())
    return ("\r\n".join(lines) + "\r\n\r\n").encode("iso-8859-1") + (
        b"" if head else response.body
    )
