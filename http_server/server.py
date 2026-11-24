"""TCP lifecycle and concurrency. Each client gets a sequential request loop."""

import argparse
import logging
import socket
import threading

from .protocol import ConnectionReader, HTTPError, Response, encode_response
from .routes import dispatch


LOGGER = logging.getLogger(__name__)


class HTTPServer:
    def __init__(self, host="127.0.0.1", port=8080, handler=dispatch,
                 client_timeout=10.0, max_clients=100):
        if client_timeout <= 0 or max_clients < 1:
            raise ValueError("Timeout and maximum clients must be positive")
        self.host = host
        self.port = port
        self.handler = handler
        self.client_timeout = client_timeout
        self._slots = threading.BoundedSemaphore(max_clients)
        self._stopping = threading.Event()
        self._lock = threading.Lock()
        self._clients = set()
        self._workers = set()

    def serve_forever(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.host, self.port))
            listener.listen(128)
            listener.settimeout(0.5)
            self.port = listener.getsockname()[1]
            LOGGER.info("Listening on http://%s:%s", self.host, self.port)
            try:
                while not self._stopping.is_set():
                    try:
                        connection, address = listener.accept()
                    except socket.timeout:
                        continue
                    connection.settimeout(self.client_timeout)
                    if not self._slots.acquire(blocking=False):
                        # Do not block the accept loop on an overloaded peer.
                        connection.close()
                        continue
                    worker = threading.Thread(target=self._serve_client,
                                              args=(connection, address), daemon=True)
                    with self._lock:
                        self._clients.add(connection)
                        self._workers.add(worker)
                    worker.start()
            finally:
                self.shutdown()
                with self._lock:
                    workers = list(self._workers)
                for worker in workers:
                    worker.join()

    def shutdown(self):
        """May be called from another thread; wakes clients blocked in recv()."""
        self._stopping.set()
        with self._lock:
            for connection in self._clients:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def _serve_client(self, connection, address):
        try:
            with connection:
                reader = ConnectionReader(connection)
                while not self._stopping.is_set():
                    try:
                        request = reader.read_request()
                        if request is None:
                            return
                    except HTTPError as error:
                        connection.sendall(encode_response(
                            Response(error.status, (str(error) + "\n").encode())
                        ))
                        return  # A framing error makes reuse unsafe.
                    except socket.timeout:
                        return  # Also closes idle persistent connections.
                    keep_alive = request.keep_alive
                    try:
                        response = self.handler(request)
                        message = encode_response(response, request.version, keep_alive,
                                                  request.method == "HEAD")
                    except Exception:
                        LOGGER.exception("Request handler failed for %s", address)
                        keep_alive = False
                        message = encode_response(Response(500, b"Internal server error\n"),
                                                  request.version, head=request.method == "HEAD")
                    connection.sendall(message)
                    LOGGER.info("%s %s %s", address, request.method, request.target)
                    if not keep_alive:
                        return
        except OSError:
            LOGGER.debug("Client disconnected: %s", address)
        finally:
            with self._lock:
                self._clients.discard(connection)
                self._workers.discard(threading.current_thread())
            self._slots.release()


def main():
    parser = argparse.ArgumentParser(description="A minimal threaded socket HTTP server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8080, type=int)
    parser.add_argument("--timeout", default=10.0, type=float,
                        help="Timeout in seconds for each client socket operation")
    parser.add_argument("--max-clients", default=100, type=int)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    server = HTTPServer(args.host, args.port, client_timeout=args.timeout,
                        max_clients=args.max_clients)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("Server stopped")


if __name__ == "__main__":
    main()
