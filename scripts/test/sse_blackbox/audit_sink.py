"""Receiver for audit webhook deliveries: a small HTTP server in a thread of the driver.

It keeps every delivery in memory, raw and parsed, in arrival order. Deliveries are asynchronous, so a case waits
for the entry it expects with `wait_for`; the wait is by request id, never by sleeping a fixed time.
"""

from __future__ import annotations

import http.server
import json
import threading
import time
from pathlib import Path

import harness as h
import scan

PATH = "/audit"
DELIVERY_TIMEOUT = 30.0


class Sink:
    def __init__(self, port: int) -> None:
        self.port = port
        self.payloads: list[bytes] = []
        self._entries: list[dict] = []
        self._lock = threading.Lock()
        self._server: http.server.ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}{PATH}"

    @property
    def origin(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        if self._server is not None:
            return
        if h.port_in_use(self.port):
            raise h.EnvError(f"audit receiver port {self.port} is already in use; "
                             f"find the holder with: lsof -nP -iTCP:{self.port} -sTCP:LISTEN")
        sink = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("content-length") or 0)
                sink.receive(self.rfile.read(length))
                self.send_response(200)
                self.send_header("content-length", "0")
                self.end_headers()

            def log_message(self, *_args) -> None:
                return

        try:
            self._server = http.server.ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        except OSError as err:
            raise h.EnvError(f"audit receiver cannot listen on {self.port}: {err}") from err
        self._thread = threading.Thread(target=self._server.serve_forever, name="audit-sink", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        self._server = None

    def receive(self, payload: bytes) -> None:
        try:
            parsed = json.loads(payload)
        except ValueError:
            parsed = None
        records = parsed.get("Records") if isinstance(parsed, dict) and isinstance(parsed.get("Records"), list) else [parsed]
        with self._lock:
            self.payloads.append(payload)
            self._entries.extend(entry for entry in records if isinstance(entry, dict))

    def entries(self) -> list[dict]:
        with self._lock:
            return list(self._entries)

    def wait_for(self, request_id: str, timeout: float = DELIVERY_TIMEOUT) -> dict | None:
        """The entry of the request with this id, once it has been delivered; None when it never arrives."""
        deadline = time.monotonic() + timeout
        while True:
            for entry in self.entries():
                if entry.get("requestID") == request_id:
                    return entry
            if time.monotonic() > deadline:
                return None
            time.sleep(0.1)

    def write(self, path: Path, registry: scan.Registry = scan.REGISTRY) -> None:
        """Keep what was received next to the archive, with any canary replaced by its marker."""
        with self._lock:
            payloads = list(self.payloads)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(json.dumps(registry.mask(payload).decode("latin-1")) + "\n" for payload in payloads))


SINK = Sink(h.AUX_PORT)
