#!/usr/bin/env python3
"""
Presenter launcher for the Beam Summit deck.

Run:   python present.py

It serves the deck locally (with video range support, so the embedded demo.mp4
plays and seeks correctly) and opens it in your browser. Present full screen
with F. The Live Demo slide now plays the recorded demo video directly.

You can also just double-click docs/Beam_Summit_Deck.html to open it as a file;
the video plays there too.
"""
import http.server, socketserver, subprocess, os, re

ROOT = os.path.dirname(os.path.abspath(__file__))
PORT = 8000
DECK = "docs/Beam_Summit_Deck.html"


class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_HEAD(self):
        self._serve(head=True)

    def do_GET(self):
        self._serve(head=False)

    def _serve(self, head):
        path = self.translate_path(self.path.split("?", 1)[0])
        if not os.path.isfile(path):
            return super().do_HEAD() if head else super().do_GET()

        size = os.path.getsize(path)
        ctype = self.guess_type(path)
        start, end, status = 0, size - 1, 200

        rng = self.headers.get("Range")
        if rng:
            m = re.match(r"bytes=(\d*)-(\d*)", rng.strip())
            if m:
                if m.group(1):
                    start = int(m.group(1))
                if m.group(2):
                    end = int(m.group(2))
                end = min(end, size - 1)
                if start <= end:
                    status = 206
                else:
                    start, end = 0, size - 1

        length = end - start + 1
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Accept-Ranges", "bytes")
        if status == 206:
            self.send_header("Content-Range", "bytes %d-%d/%d" % (start, end, size))
        self.send_header("Content-Length", str(length))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        if head:
            return

        with open(path, "rb") as f:
            f.seek(start)
            remaining = length
            while remaining > 0:
                chunk = f.read(min(65536, remaining))
                if not chunk:
                    break
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    break
                remaining -= len(chunk)


os.chdir(ROOT)
url = f"http://127.0.0.1:{PORT}/{DECK}"
print("Presenting at:", url)
print("Open it, press F to go full screen. The Live Demo slide plays demo.mp4.")
try:
    subprocess.Popen(["cmd", "/c", "start", "", url])
except Exception:
    pass
with socketserver.TCPServer(("127.0.0.1", PORT), Handler) as httpd:
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
