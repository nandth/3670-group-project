import concurrent.futures
import socket
import threading
import time
import unittest

from http_server.protocol import ConnectionReader, HTTPError, Response
from http_server.routes import dispatch
from http_server.server import HTTPServer


class ServerTests(unittest.TestCase):
    def setUp(self):
        self.entered = threading.Event()
        self.release = threading.Event()

        def handler(request):
            if request.target == "/blocked":
                self.entered.set()
                self.release.wait(3)
                return Response(body=b"released\n")
            if request.target == "/echo":
                return Response(body=request.body)
            if request.target == "/error":
                raise RuntimeError("Deliberate test failure")
            return dispatch(request)

        self.server = HTTPServer(port=0, handler=handler, client_timeout=0.5)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        deadline = time.monotonic() + 3
        while not self.server.port:
            if time.monotonic() > deadline:
                self.fail("Server did not start")
            time.sleep(0.005)

    def tearDown(self):
        self.release.set()
        self.server.shutdown()
        self.thread.join(3)
        self.assertFalse(self.thread.is_alive())

    def connect(self):
        return socket.create_connection(("127.0.0.1", self.server.port), timeout=2)

    def exchange(self, raw, allow_reset=False):
        with self.connect() as connection:
            connection.sendall(raw)
            data = bytearray()
            while True:
                try:
                    chunk = connection.recv(4096)
                except ConnectionResetError:
                    # Rejecting oversized input can leave unread TCP bytes.
                    if not allow_reset:
                        raise
                    return bytes(data)
                if not chunk:
                    return bytes(data)
                data.extend(chunk)

    def test_status_codes(self):
        for method, path, status in [("GET", "/", 200), ("GET", "/missing", 404),
                                      ("POST", "/", 405)]:
            with self.subTest(status=status):
                result = self.exchange(
                    f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".encode()
                )
                self.assertTrue(result.startswith(f"HTTP/1.1 {status} ".encode()))
                if status == 405:
                    self.assertIn(b"Allow: GET, HEAD\r\n", result)

    def test_sequential_keep_alive_and_head(self):
        with self.connect() as connection:
            stream = connection.makefile("rb")
            self.addCleanup(stream.close)
            for method in ("HEAD", "GET"):
                connection.sendall(f"{method} /health HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
                self.assertEqual(stream.readline(), b"HTTP/1.1 200 OK\r\n")
                headers = {}
                while (line := stream.readline()) != b"\r\n":
                    self.assertTrue(line)
                    key, value = line.decode().split(":", 1)
                    headers[key] = value.strip()
                self.assertEqual(headers["Content-Length"], "3")
                self.assertEqual(headers["Connection"], "keep-alive")
                if method != "HEAD":
                    self.assertEqual(stream.read(3), b"ok\n")

    def test_pipelining_preserves_request_body_boundary(self):
        data = self.exchange(
            b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nhello"
            b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        )
        self.assertEqual(data.count(b"HTTP/1.1 200 OK"), 2)
        self.assertIn(b"\r\n\r\nhelloHTTP/1.1", data)
        self.assertTrue(data.endswith(b"ok\n"))

    def test_http_10_default_close_and_explicit_keep_alive(self):
        data = self.exchange(b"GET /health HTTP/1.0\r\n\r\n")
        self.assertIn(b"Connection: close\r\n", data)
        data = self.exchange(
            b"GET /health HTTP/1.0\r\nConnection: keep-alive\r\n\r\n"
            b"GET /health HTTP/1.0\r\nConnection: close\r\n\r\n"
        )
        self.assertEqual(data.count(b"HTTP/1.0 200 OK"), 2)

    def test_bad_requests_close_connection(self):
        for headers, status in [
            (b"", 400),
            (b"Host: localhost\r\nContent-Length: -1\r\n", 400),
            (b"Host: localhost\r\nContent-Length: 0\r\nContent-Length: 0\r\n", 400),
            (b"Host: localhost\r\nTransfer-Encoding: chunked\r\n", 501),
            (b"Host: localhost\r\nExpect: 100-continue\r\n", 417),
            (b"Host: localhost\r\nContent-Length: 1048577\r\n", 413),
            (b"Host: localhost\r\nX-Large: " + b"a" * 17000 + b"\r\n", 431),
        ]:
            with self.subTest(status=status, prefix=headers[:50]):
                data = self.exchange(b"GET / HTTP/1.1\r\n" + headers + b"\r\n",
                                     allow_reset=True)
                self.assertTrue(data.startswith(f"HTTP/1.1 {status} ".encode()))
                self.assertIn(b"Connection: close\r\n", data)
                head, body = data.split(b"\r\n\r\n", 1)
                length = next(line.split(b": ", 1)[1] for line in head.split(b"\r\n")
                              if line.startswith(b"Content-Length:"))
                self.assertEqual(len(body), int(length))

    def test_idle_connection_times_out(self):
        with self.connect() as connection:
            self.assertEqual(connection.recv(1), b"")

    def test_handler_error_returns_500(self):
        with self.assertLogs("http_server.server", level="ERROR"):
            data = self.exchange(b"GET /error HTTP/1.1\r\nHost: localhost\r\n\r\n")
        self.assertTrue(data.startswith(b"HTTP/1.1 500 "))

    def test_other_clients_progress_while_one_handler_is_blocked(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=9) as pool:
            slow = pool.submit(self.exchange,
                               b"GET /blocked HTTP/1.0\r\n\r\n")
            self.assertTrue(self.entered.wait(2))
            clients = [pool.submit(self.exchange, b"GET /health HTTP/1.0\r\n\r\n")
                       for _ in range(8)]
            try:
                for client in clients:
                    self.assertTrue(client.result(timeout=2).endswith(b"ok\n"))
                self.assertFalse(slow.done())
            finally:
                self.release.set()
            self.assertTrue(slow.result(timeout=2).endswith(b"released\n"))


class ParserTests(unittest.TestCase):
    def test_fragmented_receives(self):
        class FragmentedSocket:
            def __init__(self):
                self.chunks = iter([
                    b"POST /echo HT", b"TP/1.1\r\nHo", b"st: localhost\r\n",
                    b"Content-Length: 5\r\n\r\nhe", b"llo", b"",
                ])

            def recv(self, size):
                return next(self.chunks)

        reader = ConnectionReader(FragmentedSocket())
        self.assertEqual(reader.read_request().body, b"hello")
        self.assertIsNone(reader.read_request())

    def test_truncated_body(self):
        left, right = socket.socketpair()
        with left, right:
            right.sendall(b"POST / HTTP/1.0\r\nContent-Length: 5\r\n\r\nhi")
            right.shutdown(socket.SHUT_WR)
            with self.assertRaises(HTTPError) as context:
                ConnectionReader(left).read_request()
            self.assertEqual(context.exception.status, 400)


if __name__ == "__main__":
    unittest.main()
