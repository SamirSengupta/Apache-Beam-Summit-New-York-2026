#!/usr/bin/env python3
"""
Live Fraud Dashboard server  --  backed by the REAL pipeline.

This is NOT a fake frontend. Each transaction is run through the actual logic
from realtime_fraud_rag_beam.py:
  * real RAG retrieval (HashingEmbedder + FAISS/numpy vector index)
  * a real LLM call to your OpenAI-compatible proxy (RagClassifier.build_prompt
    + call_llm + parse_verdict), with real measured latency
  * the same rule-based fallback if the model is unreachable
Results stream to the browser over Server-Sent Events and drive the dashboard.

Run:
    cd C:\\Samir\\ApacheBeam
    .venv\\Scripts\\activate            (so numpy / faiss are available)
    python dashboard_server.py
then open  http://127.0.0.1:8800

The LLM endpoint / model are read from the same env vars as the demo
(LLM_BASE_URL, LLM_MODEL, LLM_API_KEY), defaulting to your antigravity proxy.
"""
import json, os, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from realtime_fraud_rag_beam import (
    INCOMING_TRANSACTIONS, RagClassifier, call_llm, parse_verdict, rule_based_fallback,
)

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.environ.get("DASH_PORT", "8800"))
MAX  = int(os.environ.get("DASH_MAX", "15"))   # stop after this many transactions
STATE = {"fallback": False}

# Load the model/index ONCE per process -- the cost pattern, for real.
CLF = RagClassifier()


def pattern_name(doc):
    return doc.split(":")[0].strip()[:34]


def sse(payload, event):
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n".encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _head(self, ctype):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

    def do_GET(self):
        if self.path.startswith("/events"):
            self._head("text/event-stream")
            self.stream()
        elif self.path.startswith("/toggle"):
            STATE["fallback"] = not STATE["fallback"]
            self._head("application/json")
            self.wfile.write(json.dumps({"fallback": STATE["fallback"]}).encode())
        else:
            self._head("text/html; charset=utf-8")
            with open(os.path.join(HERE, "docs", "Fraud_Dashboard.html"), "rb") as fh:
                self.wfile.write(fh.read())

    def stream(self):
        i = 0
        try:
            # tell the client which vector backend is live
            self.wfile.write(sse({"backend": CLF.index.backend, "limit": MAX}, "hello")); self.wfile.flush()
            while i < MAX:
                tx = INCOMING_TRANSACTIONS[i % len(INCOMING_TRANSACTIONS)]; i += 1

                # 1) transaction arrives
                self.wfile.write(sse({
                    "id": tx.get("id"), "amount": tx["amount"], "merchant": tx["merchant"],
                    "location": tx["location"], "note": tx["note"],
                }, "tx")); self.wfile.flush()
                time.sleep(0.55)

                # 2) REAL RAG retrieval
                contexts = CLF.retrieve(tx)
                pats = [{"name": pattern_name(d), "sim": round(float(s), 2)} for d, s in contexts]
                self.wfile.write(sse({"patterns": pats}, "rag")); self.wfile.flush()
                time.sleep(0.45)

                # 3) REAL LLM inference (or fallback)
                if STATE["fallback"]:
                    v = rule_based_fallback(tx, contexts); src = "fallback"; lat = 8
                    time.sleep(0.3)
                else:
                    t0 = time.time()
                    v = parse_verdict(call_llm(CLF.build_prompt(tx, contexts)))
                    lat = int((time.time() - t0) * 1000)
                    if v is None:
                        v = rule_based_fallback(tx, contexts); src = "fallback"
                    else:
                        src = "LLM"

                print("  %-7s %-20s %-6s risk %3d  [%s] %dms" % (
                    tx.get("id", "--"), tx["merchant"][:20], v["verdict"], int(v["risk"]), src, lat), flush=True)
                self.wfile.write(sse({
                    "verdict": v["verdict"], "risk": int(v["risk"]), "reason": v["reason"],
                    "source": src, "latency_ms": lat,
                }, "verdict")); self.wfile.flush()
                time.sleep(1.0)
            self.wfile.write(sse({"count": i}, "done")); self.wfile.flush()
            print("  -- stream complete: %d transactions --" % i, flush=True)
        except (BrokenPipeError, ConnectionResetError):
            return


if __name__ == "__main__":
    print("=" * 58)
    print(" Live Fraud Dashboard  (real RAG + real LLM)")
    print(" vector backend :", CLF.index.backend)
    print(" open           :  http://127.0.0.1:%d" % PORT)
    print("=" * 58)
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
