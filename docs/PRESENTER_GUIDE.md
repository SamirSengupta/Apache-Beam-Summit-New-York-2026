# Presenter Guide & Speaker Notes

For: *Real-Time AI Pipelines at Scale: Embedding LLMs into Apache Beam for Live Inference*
Beam Summit 2026 · Hackberry · Mon 22 Jun, 2:30–3:20pm

This guide gets you from "I don't know Apache Beam" to confidently demoing and
answering questions. Read sections 1–3 once; rehearse with section 6.

---

## 1. Apache Beam in 90 seconds (so you can explain it)

**The one-liner:** Apache Beam is a way to write a data-processing pipeline
*once* and run it anywhere — on your laptop, on Spark, or on Google Cloud
Dataflow — without rewriting it. It treats **batch** and **streaming** data with
the *same* code.

Three words are all you need on stage:

- **PCollection** — a "pipe" of data flowing through your pipeline. It can be
  finite (a file) or infinite (a live stream of events). You never hold it all
  in memory; elements flow through.
- **PTransform** — a step that operates on a PCollection. You chain transforms
  with the `|` (pipe) operator, which reads left-to-right like a conveyor belt.
- **DoFn** ("do function") — your custom logic for one element, wrapped in a
  `ParDo` ("parallel do") transform. Beam runs many copies of a DoFn in
  parallel across cores/machines. **This is where we put the LLM call.**

**Why Beam for LLMs?** Two reasons you can say out loud:

1. *Same code, batch or stream.* You prototype on a batch of yesterday's data,
   then flip to a live stream with no rewrite.
2. *It scales the boring parts for you.* Parallelism, retries, batching,
   windowing, autoscaling on Dataflow — Beam handles them, so you focus on the
   model. There's even a built-in `RunInference` transform purpose-built for ML
   models (more on that in the Q&A section).

**The analogy that lands:** "Beam is a factory conveyor belt. Each transaction
is an item on the belt. Each station (`DoFn`) does one job — one station tags
the item with similar past cases, the next station asks the LLM 'is this
fraud?'. The belt runs the same whether one item or ten million come down it."

---

## 2. What the demo does (your narrative)

A live stream of credit-card transactions arrives, one every couple of seconds.
For each transaction the pipeline:

1. **Embeds** it — turns the transaction text into a vector (numbers).
2. **Retrieves (RAG)** — searches a vector database for the most similar known
   fraud patterns. This is Retrieval-Augmented Generation: we give the LLM
   relevant context instead of expecting it to know everything.
3. **Infers** — sends the transaction + retrieved patterns to the LLM, which
   returns a verdict (FRAUD / REVIEW / LEGIT), a risk score, and a one-line
   reason.
4. **Outputs** — prints the decision, color-coded, with a risk bar.

The fraud-detection framing is deliberate: it's the exact use case in your
abstract, and the audience instantly gets why low-latency, per-event inference
matters (you can't make someone wait 10 seconds at checkout).

---

## 3. The code, mapped to the pipeline (open the file alongside this)

The whole demo is `realtime_fraud_rag_beam.py`. It reads top to bottom in the
same order data flows. Here's what each numbered section does and the line you
can say when you point at it:

- **§1 Knowledge base** — the documents RAG searches over (fraud patterns).
  *"In production this is millions of historical cases in Pinecone or FAISS;
  here it's six so it fits on a slide."*

- **§2 Incoming transactions** — the events that stream in. *"These would arrive
  from Kafka or Pub/Sub; I'm generating them so the demo is self-contained."*

- **§3 Embedder** — turns text into a vector. *"A lightweight embedder so it
  runs anywhere; swapping in sentence-transformers is a one-line change and the
  rest of the pipeline doesn't care."*

- **§4 Vector index** — the vector database powering RAG. *"Uses FAISS if it's
  installed — the production path — and falls back to numpy otherwise."*

- **§5 LLM call** — talks to the model over an OpenAI-compatible API using only
  Python's standard library. *"Real inference, real model, no SDK required."*
  Note the **`rule_based_fallback`**: *"If the model is ever unreachable, the
  pipeline degrades gracefully instead of crashing — exactly what you want in
  production, and handy on a conference network."*

- **§6 RagClassifier** — the brain: retrieve → build prompt → call LLM → parse.
  Kept as plain Python so it's testable and easy to read.

- **§8 The Beam pipeline** — the part to dwell on. Point out:
  - The pipeline is just transforms chained with `|`.
  - **`DoFn.setup()` runs once per worker** — *"this is the key production
    pattern: you load the model and connect to the vector DB once, then reuse
    them across thousands of elements. You never pay that cost per transaction."*
  - Each `ParDo` is one conveyor station and Beam parallelizes it for you.

The bottom (`with beam.Pipeline() as p:`) is the whole thing in five lines —
a great "this is all it takes" moment.

---

## 4. Running it live — exact steps

Before you walk on:

1. Start your antigravity proxy (the demo expects `http://127.0.0.1:8317/v1`).
2. In the project folder, activate the venv: `source .venv/bin/activate`
   (or `.venv\Scripts\activate` on Windows).
3. Optional dry run so the terminal is warm: `python realtime_fraud_rag_beam.py --selftest`
4. Make your terminal font BIG.

The live run:

```bash
python realtime_fraud_rag_beam.py
```

Talk over it as transactions appear. Good beats to call out as they scroll:

- TX-1002 (coffee) → **LEGIT**: *"normal behavior, low risk — the model isn't
  trigger-happy."*
- TX-1003 (gift cards abroad, minutes after a US charge) → **FRAUD**: *"RAG
  pulled up 'impossible travel' and 'high-risk merchant', and the LLM connected
  the dots."*
- TX-1004 ($9,900 wire) → **REVIEW**: *"just under the reporting threshold —
  structuring. The LLM caught a pattern a simple amount-rule would miss."*

If the network dies, the output simply shows `(fallback)` and keeps going — you
can even point that out as a feature.

Tip: `STREAM_DELAY=3 python realtime_fraud_rag_beam.py` slows it to one event
every 3 seconds if you want more time to narrate.

---

## 5. Tying it back to your abstract

Your abstract promises HuggingFace/vLLM, Pinecone/FAISS, batching, quantization,
AWS Bedrock/SageMaker, and 10M+ events. The demo is the *skeleton* of that — be
explicit that production swaps in the heavy machinery, and the Beam structure is
identical:

| Abstract claim | In the demo | What changes in production |
|---|---|---|
| LLM in a Beam transform | `LlmClassify` DoFn | Same DoFn, calls vLLM/Bedrock |
| RAG with vector DB | `VectorIndex` (FAISS/numpy) | Pinecone/FAISS at scale |
| Streaming source | `StreamTransactions` timer | `ReadFromKafka` / `ReadFromPubSub` |
| Cost/throughput control | (mention) | `BatchElements`, quantized models, GPU |
| Deploy at scale | `DirectRunner` (laptop) | `DataflowRunner` on Bedrock/SageMaker + K8s |

The honest "this is the same code that scales" message is more credible than
pretending the laptop is running 10M events.

---

## 6. Likely audience questions (and solid answers)

**"Why not just use Beam's `RunInference` transform?"**
> Great question — `RunInference` is the production-grade way and I'd use it for
> a hosted model. I used a plain `DoFn` here because it's the clearest way to
> *see* what's happening for a talk. `RunInference` adds model handlers, batching
> and metrics on top of exactly this pattern.

**"How do you keep latency low with an LLM in the path?"**
> Three levers: batch elements so the GPU stays busy, quantize the model
> (GGUF/GPTQ) to shrink compute, and load the model once per worker in
> `setup()` — which you saw — so you never pay cold-start per event.

**"Isn't calling an LLM per transaction expensive?"**
> Yes if naive. You batch, you cache embeddings, you route only ambiguous cases
> to the big model and let cheap rules handle the obvious ones — which is why
> the demo has both a model path and a rule path.

**"How does RAG actually help here?"**
> The model doesn't have to memorize every fraud policy. We retrieve the
> relevant patterns at query time and put them in the prompt, so the model
> reasons over current, specific context. Update the vector DB and behavior
> changes with no retraining.

**"What about ordering / exactly-once / late data?"**
> Beam's windowing and triggers handle event-time and late data, and runners
> like Dataflow give you exactly-once. I kept windowing out of the demo to stay
> readable, but that's a strength of doing this in Beam rather than a bare
> script.

**"Why Beam over Spark Structured Streaming or Flink?"**
> Portability — the same pipeline runs on Flink, Spark, or Dataflow. You pick
> the runner without rewriting. And `RunInference` makes ML a first-class
> citizen.

**If you get a question you don't know:** *"That's a great one — I haven't hit
that in production yet, let's talk after."* Totally fine; never bluff specifics.

---

## 7. Two-sentence safety net (memorize this)

> "What you're seeing is a real Apache Beam streaming pipeline doing per-event
> RAG retrieval and live LLM inference. The same five transforms run unchanged
> on Dataflow against millions of events — the laptop just proves the pattern."

If everything else leaves your head, that's the talk.
