#!/usr/bin/env python3
"""
=============================================================================
 Real-Time AI Pipelines at Scale
 Embedding LLMs into Apache Beam for Live Inference  --  LIVE DEMO
=============================================================================

WHAT THIS DEMO SHOWS (in one sentence):
    A stream of credit-card transactions flows through an Apache Beam
    pipeline. For each transaction we (1) turn it into a vector, (2) use a
    vector database (RAG) to pull up the most similar known fraud patterns,
    (3) hand the transaction + those patterns to an LLM, which decides
    FRAUD / REVIEW / LEGIT and explains why -- all in real time.

WHY THIS IS A GOOD STAGE DEMO:
    * It runs on a laptop, offline-friendly, no GPU, no cloud.
    * The LLM is called through your local OpenAI-compatible proxy.
    * If the LLM is ever unreachable, a built-in rule-based fallback keeps
      the demo running so it NEVER hard-fails in front of an audience.

THE BEAM PIPELINE (read top to bottom = data flowing left to right):

    Create([None])              # one "seed" element to kick things off
        |
    StreamTransactions          # emits 1 transaction every DELAY seconds
        |                       #   (simulates a live Kafka / Pub/Sub stream)
    ParDo(RagRetrieve)          # embed tx -> FAISS top-k similar fraud cases
        |
    ParDo(LlmClassify)          # build prompt -> call LLM -> parse verdict
        |
    ParDo(PrintResult)          # the "sink": pretty-print to the console

Run it:           python realtime_fraud_rag_beam.py
Test logic only:  python realtime_fraud_rag_beam.py --selftest   (no Beam needed)
=============================================================================
"""

import argparse
import json
import os
import sys
import time
import urllib.request

import numpy as np

# -----------------------------------------------------------------------------
# 0. CONFIG  --  everything you might want to change lives here.
# -----------------------------------------------------------------------------
# These default to your local antigravity proxy. Override with env vars if you
# move things around, e.g.  LLM_BASE_URL=... python realtime_fraud_rag_beam.py
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "http://127.0.0.1:8317/v1")
LLM_MODEL    = os.environ.get("LLM_MODEL",    "claude-opus-4-6-thinking")
LLM_API_KEY  = os.environ.get("LLM_API_KEY",  "dummy")

STREAM_DELAY_SECONDS = float(os.environ.get("STREAM_DELAY", "2.0"))  # gap between events
TOP_K = 2          # how many fraud patterns RAG retrieves per transaction
EMBED_DIM = 256    # size of our toy embedding vectors


# -----------------------------------------------------------------------------
# 1. THE KNOWLEDGE BASE  --  what RAG searches over.
# -----------------------------------------------------------------------------
# In production this lives in a real vector DB (Pinecone / FAISS / Weaviate)
# and holds millions of historical cases & fraud-policy documents. Here we use
# a handful so the demo is easy to follow on a slide.
FRAUD_KNOWLEDGE_BASE = [
    "Card testing: many small purchases in quick succession, often a few "
    "dollars each, are used by fraudsters to validate stolen card numbers "
    "before a big purchase.",

    "Impossible travel: the same card used in two countries within minutes "
    "is physically impossible and strongly indicates a cloned card.",

    "Structuring: transactions deliberately kept just under a reporting "
    "threshold, such as 9900 dollars, can indicate money laundering.",

    "High-risk merchant category: large purchases of gift cards, crypto, or "
    "prepaid debit cards are commonly associated with scam payouts.",

    "Dormant account spike: a sudden high-value purchase on an account that "
    "has been inactive for months is a classic account-takeover signal.",

    "Normal behavior: recurring small purchases at grocery stores, transit, "
    "and coffee shops in the cardholder's home city are typical and low risk.",

    "High-velocity cash-out: several large ATM withdrawals or transfers within "
    "minutes drains a compromised account before it can be frozen.",

    "Crypto and prepaid: large crypto or prepaid-debit purchases from a new device "
    "are a common way to move stolen funds quickly.",

    "Foreign first-use: a first-ever, high-value purchase in a new country can "
    "indicate a stolen card being tested abroad.",
]


# -----------------------------------------------------------------------------
# 2. THE LIVE STREAM  --  the transactions that "arrive" over time.
# -----------------------------------------------------------------------------
# Each dict is one event. In production these stream in from Kafka / Pub/Sub.
INCOMING_TRANSACTIONS = [
    {"id": "TX-1001", "amount": 4.00,    "location": "New York, US",
     "merchant": "Online Store",
     "note": "Five separate 4 dollar charges in two minutes on a new card."},

    {"id": "TX-1002", "amount": 5.25,    "location": "Brooklyn, US",
     "merchant": "Joe's Coffee",
     "note": "Morning coffee, same shop the cardholder visits most days."},

    {"id": "TX-1003", "amount": 980.00,  "location": "Lagos, NG",
     "merchant": "GiftCardHub",
     "note": "Card used in New York 4 minutes ago, now buying gift cards abroad."},

    {"id": "TX-1004", "amount": 9900.00, "location": "Miami, US",
     "merchant": "Cash Transfer Co",
     "note": "Wire just under the 10000 dollar reporting threshold."},

    {"id": "TX-1005", "amount": 42.10,   "location": "New York, US",
     "merchant": "MetroCard",
     "note": "Subway top-up in the cardholder's home city."},

    {"id": "TX-1006", "amount": 7300.00, "location": "Austin, US",
     "merchant": "Luxury Electronics",
     "note": "Big purchase on an account with no activity for 8 months."},

    {"id": "TX-1007", "amount": 86.40, "location": "Seattle, US",
     "merchant": "Whole Foods",
     "note": "Weekly groceries, in-person chip read in the home city."},

    {"id": "TX-1008", "amount": 4500.00, "location": "Unknown",
     "merchant": "CoinRamp",
     "note": "Crypto purchase from a brand-new device and unfamiliar IP."},

    {"id": "TX-1009", "amount": 12.99, "location": "New York, US",
     "merchant": "Streamly",
     "note": "Monthly subscription renewal at the usual amount."},

    {"id": "TX-1010", "amount": 14200.00, "location": "Geneva, CH",
     "merchant": "Maison Horloge",
     "note": "Luxury watch, first ever purchase abroad on a foreign IP."},

    {"id": "TX-1011", "amount": 48.20, "location": "Dallas, US",
     "merchant": "Shell",
     "note": "Fuel purchase at a station the cardholder uses often."},

    {"id": "TX-1012", "amount": 3000.00, "location": "Houston, US",
     "merchant": "ATM Network",
     "note": "Three large ATM withdrawals within nine minutes."},

    {"id": "TX-1013", "amount": 120.00, "location": "New York, US",
     "merchant": "Trattoria Bella",
     "note": "Dinner paid in person at a local restaurant."},

    {"id": "TX-1014", "amount": 2000.00, "location": "Newark, US",
     "merchant": "PrepaidPlus",
     "note": "Three prepaid debit loads of 2000 dollars back to back."},
]


# -----------------------------------------------------------------------------
# 3. EMBEDDER  --  turns text into a vector.
# -----------------------------------------------------------------------------
# We use a tiny, dependency-free "hashing" embedder so the demo always runs.
# It maps words into a fixed-size vector. It is good enough to retrieve the
# right patterns when text shares vocabulary.
#
# >>> TO USE REAL SEMANTIC EMBEDDINGS IN PRODUCTION: replace this class with
#     sentence-transformers ("all-MiniLM-L6-v2") or your provider's embedding
#     API. Nothing else in the pipeline has to change. <<<
class HashingEmbedder:
    def __init__(self, dim=EMBED_DIM):
        self.dim = dim

    def embed(self, text: str) -> np.ndarray:
        vec = np.zeros(self.dim, dtype="float32")
        for token in str(text).lower().replace(",", " ").split():
            token = "".join(ch for ch in token if ch.isalnum())
            if not token:
                continue
            # Two hashed slots per token softens collisions a little.
            vec[hash(token) % self.dim] += 1.0
            vec[hash(token + "#2") % self.dim] += 0.5
        norm = np.linalg.norm(vec)
        return vec / norm if norm > 0 else vec  # normalize -> cosine = dot product


# -----------------------------------------------------------------------------
# 4. VECTOR INDEX  --  the "vector database" that powers RAG.
# -----------------------------------------------------------------------------
# Uses FAISS if it is installed (the production path). If FAISS is not present,
# it silently falls back to a pure-numpy search so the demo still works.
class VectorIndex:
    def __init__(self, embedder: HashingEmbedder, documents):
        self.embedder = embedder
        self.documents = list(documents)
        self.matrix = np.vstack([embedder.embed(d) for d in self.documents])
        self.backend = "numpy"
        self._faiss = None
        try:
            import faiss  # noqa
            self._faiss = faiss.IndexFlatIP(embedder.dim)  # inner product = cosine
            self._faiss.add(self.matrix)
            self.backend = "faiss"
        except Exception:
            pass  # numpy fallback is fine for the demo

    def search(self, text: str, k=TOP_K):
        q = self.embedder.embed(text).reshape(1, -1)
        if self._faiss is not None:
            scores, idxs = self._faiss.search(q, k)
            idxs, scores = idxs[0], scores[0]
        else:
            sims = (self.matrix @ q[0])
            idxs = np.argsort(-sims)[:k]
            scores = sims[idxs]
        return [(self.documents[i], float(s)) for i, s in zip(idxs, scores)]


# -----------------------------------------------------------------------------
# 5. LLM CALL  --  talks to your local OpenAI-compatible proxy via stdlib only.
# -----------------------------------------------------------------------------
# No 'openai' package required -- we use urllib so there is nothing extra to
# install. Returns the raw assistant text, or None on any failure.
def call_llm(prompt: str, timeout=30) -> str | None:
    body = json.dumps({
        "model": LLM_MODEL,
        "messages": [
            {"role": "system",
             "content": "You are a fraud-detection assistant. Reply with ONLY "
                        "a JSON object: {\"verdict\":\"FRAUD|REVIEW|LEGIT\","
                        "\"risk\":0-100,\"reason\":\"one short sentence\"}."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{LLM_BASE_URL.rstrip('/')}/chat/completions",
        data=body,
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {LLM_API_KEY}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"   [!] LLM call failed ({e}); using rule-based fallback.",
              file=sys.stderr)
        return None


def parse_verdict(text: str) -> dict | None:
    """Pull the JSON object out of the model's reply (handles code fences)."""
    if not text:
        return None
    s = text.strip()
    start, end = s.find("{"), s.rfind("}")
    if start == -1 or end == -1:
        return None
    try:
        obj = json.loads(s[start:end + 1])
        return {
            "verdict": str(obj.get("verdict", "REVIEW")).upper(),
            "risk": int(obj.get("risk", 50)),
            "reason": str(obj.get("reason", "")),
            "source": "LLM",
        }
    except Exception:
        return None


def rule_based_fallback(tx: dict, contexts) -> dict:
    """Keeps the demo alive if the LLM is unreachable. Deterministic and
    transparent: it inspects the transaction the way a hand-written rules engine
    would, so "pull the plug" still produces sensible verdicts on every case."""
    note = tx["note"].lower()
    merch = tx["merchant"].lower()
    text = note + " " + merch
    country = tx["location"].split(",")[-1].strip()
    amount = tx["amount"]
    risk = 10
    reasons = []
    # impossible travel: same card in another country within minutes
    if "minute" in note and ("country" in note or "abroad" in note or country not in ("US",)):
        risk = max(risk, 92); reasons.append("impossible travel")
    # high-risk payout instruments: gift cards, crypto, prepaid debit
    if "gift card" in text or "giftcard" in merch or "crypto" in text or "prepaid" in text:
        risk = max(risk, 80); reasons.append("high-risk payout instrument")
    # high-velocity cash-out: several large ATM withdrawals/transfers in minutes
    if ("atm" in text or "withdrawal" in note or "cash-out" in note) and \
       ("minute" in note or "within" in note or "back to back" in note or "three" in note):
        risk = max(risk, 80); reasons.append("high-velocity cash-out")
    # structuring: amount deliberately just under the 10k reporting threshold
    if 9000 <= amount < 10000:
        risk = max(risk, 75); reasons.append("just under reporting threshold")
    # dormant-account spike: big charge on a long-inactive account
    if "no activity" in note or "inactive" in note or "dormant" in note:
        risk = max(risk, 70); reasons.append("dormant-account spike")
    # foreign first-use: first-ever high-value purchase in a new country
    if "first" in note and ("abroad" in note or "foreign" in note or "new country" in note) and country not in ("US",):
        risk = max(risk, 70); reasons.append("foreign first-use")
    # unrecognized device / IP
    if "new device" in note or "unfamiliar ip" in note or "foreign ip" in note:
        risk = max(risk, 70); reasons.append("unrecognized device or IP")
    # card testing: many small charges in quick succession
    if "separate" in note and "card" in note:
        risk = max(risk, 65); reasons.append("possible card testing")
    verdict = "FRAUD" if risk >= 80 else "REVIEW" if risk >= 50 else "LEGIT"
    return {"verdict": verdict, "risk": risk,
            "reason": (", ".join(reasons) or "no strong risk signals"),
            "source": "fallback"}


# -----------------------------------------------------------------------------
# 6. THE RAG + LLM "BRAIN"  --  plain Python so it is easy to test & explain.
# -----------------------------------------------------------------------------
class RagClassifier:
    def __init__(self):
        self.embedder = HashingEmbedder()
        self.index = VectorIndex(self.embedder, FRAUD_KNOWLEDGE_BASE)

    def retrieve(self, tx: dict):
        query = f"{tx['merchant']} {tx['location']} {tx['note']}"
        return self.index.search(query, k=TOP_K)

    def build_prompt(self, tx: dict, contexts) -> str:
        ctx = "\n".join(f"- {doc}" for doc, _ in contexts)
        return (
            f"Transaction:\n"
            f"  amount: ${tx['amount']:.2f}\n"
            f"  location: {tx['location']}\n"
            f"  merchant: {tx['merchant']}\n"
            f"  note: {tx['note']}\n\n"
            f"Most similar known patterns (retrieved by vector search):\n{ctx}\n\n"
            f"Classify this transaction."
        )

    def classify(self, tx: dict, contexts) -> dict:
        prompt = self.build_prompt(tx, contexts)
        verdict = parse_verdict(call_llm(prompt))
        if verdict is None:
            verdict = rule_based_fallback(tx, contexts)
        return verdict


# -----------------------------------------------------------------------------
# 7. CONSOLE OUTPUT  --  the bit your audience actually watches.
# -----------------------------------------------------------------------------
def render(tx, contexts, verdict) -> str:
    colors = {"FRAUD": "\033[91m", "REVIEW": "\033[93m", "LEGIT": "\033[92m"}
    reset = "\033[0m"
    c = colors.get(verdict["verdict"], "")
    bar = "#" * (verdict["risk"] // 10) + "." * (10 - verdict["risk"] // 10)
    top = contexts[0][0].split(":")[0] if contexts else "n/a"
    return (
        f"\n  {tx['id']}  ${tx['amount']:,.2f}  {tx['merchant']} ({tx['location']})\n"
        f"     note     : {tx['note']}\n"
        f"     RAG match : {top}  (similarity {contexts[0][1]:.2f})\n"
        f"     risk      : [{bar}] {verdict['risk']}/100\n"
        f"     verdict   : {c}{verdict['verdict']}{reset}  "
        f"({verdict['source']}) -- {verdict['reason']}"
    )


# -----------------------------------------------------------------------------
# 8. THE APACHE BEAM PIPELINE
# -----------------------------------------------------------------------------
# Beam is imported lazily inside run_pipeline() so that --selftest works even
# on a machine where apache-beam is not installed.
def run_pipeline():
    import apache_beam as beam

    classifier = RagClassifier()
    print(f"[setup] vector index backend = {classifier.index.backend}")
    print(f"[setup] LLM endpoint        = {LLM_BASE_URL}  (model: {LLM_MODEL})")
    print(f"[setup] streaming {len(INCOMING_TRANSACTIONS)} transactions, "
          f"1 every {STREAM_DELAY_SECONDS}s ...\n")

    # --- DoFn 1: the streaming source --------------------------------------
    # Emits one transaction every STREAM_DELAY_SECONDS. This stands in for a
    # real unbounded source. In production swap this for:
    #     beam.io.ReadFromPubSub(...)   or   ReadFromKafka(...)
    class StreamTransactions(beam.DoFn):
        def process(self, _seed):
            for tx in INCOMING_TRANSACTIONS:
                time.sleep(STREAM_DELAY_SECONDS)
                yield tx

    # --- DoFn 2: RAG retrieval ---------------------------------------------
    # setup() runs ONCE per worker -- this is where you load models / connect
    # to the vector DB so the cost is paid once, not per element.
    class RagRetrieve(beam.DoFn):
        def setup(self):
            self.clf = RagClassifier()

        def process(self, tx):
            contexts = self.clf.retrieve(tx)
            yield {"tx": tx, "contexts": contexts}

    # --- DoFn 3: LLM inference ---------------------------------------------
    class LlmClassify(beam.DoFn):
        def setup(self):
            self.clf = RagClassifier()

        def process(self, element):
            tx, contexts = element["tx"], element["contexts"]
            verdict = self.clf.classify(tx, contexts)
            yield {"tx": tx, "contexts": contexts, "verdict": verdict}

    # --- DoFn 4: the sink (print) ------------------------------------------
    class PrintResult(beam.DoFn):
        def process(self, element):
            print(render(element["tx"], element["contexts"], element["verdict"]))
            yield element  # pass through in case you add more steps later

    with beam.Pipeline() as p:
        (
            p
            | "Seed"      >> beam.Create([None])
            | "Stream"    >> beam.ParDo(StreamTransactions())
            | "RAG"       >> beam.ParDo(RagRetrieve())
            | "LLM"       >> beam.ParDo(LlmClassify())
            | "Print"     >> beam.ParDo(PrintResult())
        )

    print("\n[done] pipeline finished. ")


# -----------------------------------------------------------------------------
# 9. SELF-TEST  --  exercises the brain WITHOUT Beam (for quick sanity checks).
# -----------------------------------------------------------------------------
def run_selftest():
    print("Running self-test (no Beam, no live LLM required)...\n")
    clf = RagClassifier()
    print(f"vector index backend = {clf.index.backend}\n")
    for tx in INCOMING_TRANSACTIONS:
        contexts = clf.retrieve(tx)
        verdict = clf.classify(tx, contexts)
        print(render(tx, contexts, verdict))
    print("\nSelf-test complete.")


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--selftest", action="store_true",
                    help="run the RAG+LLM logic without Apache Beam")
    args = ap.parse_args()
    if args.selftest:
        run_selftest()
    else:
        run_pipeline()
