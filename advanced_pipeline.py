#!/usr/bin/env python3
"""
=============================================================================
 Real-Time AI Pipelines at Scale  --  BEAM-NATIVE SHOWCASE
=============================================================================

This is the "production-shaped" version of the demo. It does the SAME fraud
detection as realtime_fraud_rag_beam.py, but wired up the way a real Beam
pipeline would be -- using the parts of Beam an experienced audience cares
about:

  1. RunInference + a custom ModelHandler   -- the Beam-native way to run a
     model in a pipeline (instead of a raw call inside a DoFn).
  2. Batched inference                       -- elements are grouped into
     batches before hitting the model (throughput / cost lever).
  3. Fixed-window aggregation                -- a rolling fraud-rate computed
     per time window, the classic streaming pattern.
  4. Beam Metrics                            -- counters + a latency
     distribution, queried after the run (production instrumentation).
  5. Tagged outputs                          -- the stream is split into a
     FRAUD branch and a CLEARED branch, each of which could go to its own sink.

It reuses the embedder / vector index / LLM call / fallback from the simple
file, so the "brain" is identical and already proven.

  Run it:   python advanced_pipeline.py
  Simple/safe version for the live stage:   python realtime_fraud_rag_beam.py

NOTE: run this once before your talk to confirm it works in your environment.
The simple file remains your guaranteed fallback if anything misbehaves.
=============================================================================
"""

import time

import apache_beam as beam
from apache_beam import window
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.ml.inference.base import ModelHandler, RunInference, PredictionResult

# Reuse the already-working "brain" from the simple demo.
from realtime_fraud_rag_beam import (
    INCOMING_TRANSACTIONS,
    FRAUD_KNOWLEDGE_BASE,
    HashingEmbedder,
    VectorIndex,
    RagClassifier,
    call_llm,
    parse_verdict,
    rule_based_fallback,
    render,
    STREAM_DELAY_SECONDS,
    TOP_K,
)

NAMESPACE = "fraud"          # all metrics live under this namespace
EVENT_INTERVAL = 2           # synthetic event-time gap (seconds) between events
WINDOW_SIZE = 6              # fixed-window length (seconds) for the fraud rate


# -----------------------------------------------------------------------------
# 1. STREAMING SOURCE  --  emits transactions stamped with an EVENT TIME.
# -----------------------------------------------------------------------------
# We attach a deterministic event timestamp to each element with
# TimestampedValue. That timestamp is what windowing uses downstream, so the
# fraud-rate windows are reproducible no matter how fast the demo actually runs.
class StreamTransactions(beam.DoFn):
    def process(self, _seed):
        for i, tx in enumerate(INCOMING_TRANSACTIONS):
            time.sleep(STREAM_DELAY_SECONDS)
            # Carry the event time on the element. RunInference does not
            # preserve Beam timestamps, so we re-stamp just before windowing.
            yield {**tx, "_event_ts": float(i * EVENT_INTERVAL)}


# -----------------------------------------------------------------------------
# 2. RAG RETRIEVAL  --  a DoFn whose setup() loads the index ONCE per worker.
# -----------------------------------------------------------------------------
class RagRetrieve(beam.DoFn):
    def setup(self):
        # Loaded once per worker, reused for every element -- the key cost pattern.
        self.embedder = HashingEmbedder()
        self.index = VectorIndex(self.embedder, FRAUD_KNOWLEDGE_BASE)
        self.retrievals = Metrics.counter(NAMESPACE, "rag_retrievals")

    def process(self, tx):
        query = f"{tx['merchant']} {tx['location']} {tx['note']}"
        contexts = self.index.search(query, k=TOP_K)
        self.retrievals.inc()
        yield {"tx": tx, "contexts": contexts}


# -----------------------------------------------------------------------------
# 3. LLM AS A BEAM ModelHandler  --  this is the marquee Beam-native piece.
# -----------------------------------------------------------------------------
# RunInference takes a ModelHandler. Beam calls load_model() once per worker,
# then hands run_inference() a BATCH of elements. That's how Beam standardises
# model serving: batching, model reuse, and metrics all come "for free".
class LlmModelHandler(ModelHandler):
    def load_model(self):
        # The "model" here is the prompt-builder/classifier. The LLM itself is a
        # remote endpoint, so loading is cheap -- but in production this is where
        # a local model would be loaded into GPU memory, exactly once.
        return RagClassifier()

    def run_inference(self, batch, model, inference_args=None):
        # 'batch' is a list of {tx, contexts}. Beam decided the batch size for us.
        latency = Metrics.distribution(NAMESPACE, "llm_latency_ms")
        fallbacks = Metrics.counter(NAMESPACE, "llm_fallbacks")
        batch_size = Metrics.distribution(NAMESPACE, "inference_batch_size")
        batch_size.update(len(batch))

        for element in batch:
            tx, contexts = element["tx"], element["contexts"]
            prompt = model.build_prompt(tx, contexts)
            t0 = time.time()
            verdict = parse_verdict(call_llm(prompt))
            latency.update(int((time.time() - t0) * 1000))
            if verdict is None:                      # graceful degradation
                verdict = rule_based_fallback(tx, contexts)
                fallbacks.inc()
            # RunInference wants (input, prediction) pairs.
            yield PredictionResult(element, verdict)

    def batch_elements_kwargs(self):
        # Tell Beam how to batch. In production these are tuned for GPU saturation.
        return {"min_batch_size": 2, "max_batch_size": 8}


# -----------------------------------------------------------------------------
# 4. ROUTE + COUNT  --  tagged outputs (FRAUD vs CLEARED) and metrics.
# -----------------------------------------------------------------------------
class RouteAndCount(beam.DoFn):
    def setup(self):
        self.total = Metrics.counter(NAMESPACE, "transactions_total")
        self.frauds = Metrics.counter(NAMESPACE, "frauds_detected")
        self.risk = Metrics.distribution(NAMESPACE, "risk_score")

    def process(self, pred):
        element, verdict = pred.example, pred.inference
        record = {"tx": element["tx"], "contexts": element["contexts"],
                  "verdict": verdict}
        self.total.inc()
        self.risk.update(int(verdict["risk"]))

        # Main output (used for the windowed fraud-rate).
        yield record
        # Side outputs: a real pipeline would send these to different sinks
        # (e.g. a fraud queue vs. an analytics table).
        if verdict["verdict"] == "FRAUD":
            self.frauds.inc()
            yield beam.pvalue.TaggedOutput("fraud", record)
        else:
            yield beam.pvalue.TaggedOutput("cleared", record)


# -----------------------------------------------------------------------------
# 5. WINDOWED FRAUD RATE  --  combine per fixed time window.
# -----------------------------------------------------------------------------
class CountFrauds(beam.CombineFn):
    """Sums (frauds, total) pairs within each window."""
    def create_accumulator(self):
        return (0, 0)

    def add_input(self, acc, pair):
        return (acc[0] + pair[0], acc[1] + pair[1])

    def merge_accumulators(self, accs):
        f = sum(a[0] for a in accs)
        t = sum(a[1] for a in accs)
        return (f, t)

    def extract_output(self, acc):
        return acc


class FormatWindow(beam.DoFn):
    # WindowParam gives us the time window boundaries for this aggregated value.
    def process(self, counts, win=beam.DoFn.WindowParam):
        frauds, total = counts
        if total:
            rate = 100.0 * frauds / total
            print(f"   [window {int(win.start)}s-{int(win.end)}s]  "
                  f"fraud rate = {frauds}/{total} ({rate:.0f}%)")
        yield counts


# -----------------------------------------------------------------------------
# 6. PRINT HELPERS for the two branches.
# -----------------------------------------------------------------------------
def print_fraud(record):
    print("  >> FRAUD ALERT" + render(record["tx"], record["contexts"], record["verdict"]))
    return record


def print_cleared(record):
    print(render(record["tx"], record["contexts"], record["verdict"]))
    return record


# -----------------------------------------------------------------------------
# 7. THE PIPELINE
# -----------------------------------------------------------------------------
def run():
    import logging
    logging.getLogger().setLevel(logging.ERROR)   # hush benign Beam warnings
    print(f"[setup] streaming {len(INCOMING_TRANSACTIONS)} transactions "
          f"through a Beam-native pipeline (RunInference + windowing + metrics)\n")

    pipeline = beam.Pipeline()

    routed = (
        pipeline
        | "Seed"       >> beam.Create([None])
        | "Stream"     >> beam.ParDo(StreamTransactions())
        | "RAG"        >> beam.ParDo(RagRetrieve())
        | "LLM"        >> RunInference(LlmModelHandler())     # <-- Beam-native ML
        | "Route"      >> beam.ParDo(RouteAndCount()).with_outputs(
                              "fraud", "cleared", main="main")
    )

    # Two branches -> two (potential) sinks.
    routed.fraud   | "PrintFraud"   >> beam.Map(print_fraud)
    routed.cleared | "PrintCleared" >> beam.Map(print_cleared)

    # Windowed fraud-rate over the main stream.
    (
        routed.main
        | "Stamp"    >> beam.Map(
              lambda r: window.TimestampedValue(r, r["tx"]["_event_ts"]))
        | "Window"   >> beam.WindowInto(window.FixedWindows(WINDOW_SIZE))
        | "ToPairs"  >> beam.Map(
              lambda r: (1 if r["verdict"]["verdict"] == "FRAUD" else 0, 1))
        | "Combine"  >> beam.CombineGlobally(CountFrauds()).without_defaults()
        | "Format"   >> beam.ParDo(FormatWindow())
    )

    result = pipeline.run()
    result.wait_until_finish()

    # --- Query Beam metrics after the run (production instrumentation) --------
    print("\n[metrics]")
    try:
        q = result.metrics().query(MetricsFilter().with_namespace(NAMESPACE))
        pick = lambda m: m.committed if m.committed is not None else m.attempted
        for c in q["counters"]:
            print(f"   {c.key.metric.name:22} = {pick(c)}")
        for d in q["distributions"]:
            dr = pick(d)
            if dr and dr.count:
                print(f"   {d.key.metric.name:22} = mean {dr.mean:.0f}, "
                      f"min {dr.min}, max {dr.max}, n {dr.count}")
    except Exception as e:
        print(f"   (metrics unavailable on this runner: {e})")

    print("\n[done] Beam-native pipeline finished.")


if __name__ == "__main__":
    run()
