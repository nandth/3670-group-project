"""Add endpoints here. Handlers receive a complete request and return a response."""

from .protocol import Request, Response


def index(request: Request) -> Response:
    return Response(body=b"Hello from the socket HTTP server!\n")


def health(request: Request) -> Response:
    return Response(body=b"ok\n")


ROUTES = {
    ("GET", "/"): index,
    ("GET", "/health"): health,
}


def dispatch(request: Request) -> Response:
    path = request.target.partition("?")[0]
    # HEAD uses GET's status and headers; the socket layer omits the body.
    method = "GET" if request.method == "HEAD" else request.method
    handler = ROUTES.get((method, path))
    if handler:
        return handler(request)
    allowed = {method for method, route_path in ROUTES if route_path == path}
    if allowed:
        if "GET" in allowed:
            allowed.add("HEAD")
        return Response(405, b"Method not allowed\n", {"Allow": ", ".join(sorted(allowed))})
    return Response(404, b"Not found\n")
