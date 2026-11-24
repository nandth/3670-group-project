# Socket HTTP server

A small Python HTTP/1.x server for a networking project. It uses TCP sockets
directly, with no web framework or third-party dependencies. Requires Python 3.9+.

## Run

From the repository root:

```sh
python3 -m http_server.server
```

The default address is `http://127.0.0.1:8080`. Options:

```sh
python3 -m http_server.server --host 0.0.0.0 --port 8080 --timeout 10 --max-clients 100
```

`0.0.0.0` binds all IPv4 interfaces, making the server reachable by other machines
when the network allows it. Press Ctrl+C to stop the listener and close client sockets.

```sh
curl -i http://127.0.0.1:8080/
curl -I http://127.0.0.1:8080/health
curl -i http://127.0.0.1:8080/missing
# curl can reuse a connection for these two URLs; -v shows reuse.
curl -v http://127.0.0.1:8080/health http://127.0.0.1:8080/
```

## How it works

1. The main thread creates an IPv4 TCP socket, binds, listens, and accepts clients.
2. Each accepted client gets a thread. Requests on that connection run sequentially;
   other clients can make progress independently. At the client limit, new sockets
   are closed immediately rather than queued in memory.
3. The reader collects bytes until the header delimiter, then reads exactly the
   `Content-Length` body. TCP does not preserve message boundaries: partial reads
   are assembled, and extra bytes are retained for the next request.
4. A route handler returns a `Response`. The serializer writes the status line,
   headers, and body using `sendall()`. It computes byte lengths automatically.
5. HTTP/1.1 connections persist by default. HTTP/1.0 connections close by default
   unless `Connection: keep-alive` is present. `Connection: close` takes precedence.
   Buffered pipelined requests receive responses in order.

## Extend it

| File | Responsibility |
| --- | --- |
| `http_server/server.py` | Socket lifecycle, client threads, timeouts, shutdown |
| `http_server/protocol.py` | Request parsing, message boundaries, response serialization |
| `http_server/routes.py` | Routing and application behavior |
| `tests/test_server.py` | Raw TCP integration tests and parser checks |

Add a function in `routes.py`, then register its `(method, path)` in `ROUTES`:

```python
def echo(request: Request) -> Response:
    return Response(200, request.body, {"Content-Type": "application/octet-stream"})

# Add to ROUTES:
("POST", "/echo"): echo,
```

`Request` provides `method`, `target` (including the query string), `version`,
lowercase `headers`, and raw `body` bytes. Route matching ignores the query string.
`Response` accepts a numeric status, bytes body, and optional headers. The server
owns `Content-Length`, `Connection`, and transfer framing. HEAD uses the GET handler
but sends only headers, with the corresponding GET body length. Handlers may run
simultaneously, so synchronize any shared mutable state. A custom dispatcher can
also be passed as `HTTPServer(handler=your_handler)`.

The sample routes return 200, unknown paths return 404, and unsupported methods on
known paths return 405 with `Allow`. Parsing failures return appropriate errors
(400, 413, 417, 431, 501, or 505) and close the connection. Handler failures return
500 and close. Idle or stalled socket operations time out after 10 seconds by
default. Request headers are limited to 16 KiB and bodies to 1 MiB.

## Test

```sh
python3 -m unittest discover -s tests -v
```

Tests cover persistent and closing connections, HTTP/1.0, HEAD, status codes,
fragmented requests, pipelining, body boundaries, malformed input, timeouts,
handler failures, and simultaneous clients while another handler is blocked.