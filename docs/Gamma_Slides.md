# Real-Time AI Pipelines at Scale

Embedding LLMs into Apache Beam for live inference

Samir Sengupta · AI/ML Engineer, SyGenticAI
Beam Summit 2026

Visual: dark, technical, a glowing data stream flowing through connected nodes

---

## Building the model is the easy part

The hard part is running it on live data, at scale, in real time.

- **Latency** — an LLM in the request path can't make a customer wait at checkout
- **Cost** — calling a big model on every event gets expensive fast
- **Reliability** — a stream never stops; the system must degrade, not crash

This talk: how to put an LLM *inside* a streaming pipeline — and what it takes to make it real.

Visual: three icon cards for latency, cost, reliability

---

## What is Apache Beam?

A framework for data pipelines that run anywhere, at any scale.

- **Write once, run anywhere** — same code on a laptop (DirectRunner) or the cloud (Dataflow, Flink, Spark)
- **Batch and streaming, one model** — bounded data (a file) and unbounded data (a live stream) use the same code
- Beam handles the hard parts: parallelism, retries, batching, autoscaling

Prototype on historical data, then point the identical pipeline at the live stream.

Visual: one blueprint feeding a small machine and a large machine

---

## The vocabulary (you'll see these in the code)

- **PCollection** — the data flowing between stages (assumed huge, distributed)
- **PTransform** — one stage; takes a PCollection in, produces one out
- **ParDo / DoFn** — your per-element logic, run in parallel — *the LLM lives here*
- **Runner** — what executes it: DirectRunner (laptop) → DataflowRunner (cloud)
- **Source / Sink** — data in (Kafka, Pub/Sub) and results out (DB, alert queue)

Visual: a labeled left-to-right pipeline diagram

---

## The demo: real-time fraud detection

A stream of card transactions, classified live — four Beam transforms.

1. **Stream source** — transactions arrive (prod: Kafka / Pub/Sub)
2. **RAG retrieve** — find the most similar known fraud patterns
3. **LLM classify** — verdict + risk score + reason
4. **Output sink** — FRAUD / REVIEW / LEGIT

The whole pipeline is five readable lines of code.

Visual: 4-stage horizontal flow with icons, arrows between stages

---

## RAG: give the model context, don't make it memorize

Retrieval-Augmented Generation = look up relevant knowledge at decision time.

- **Embedding** — turn text into a vector that captures meaning
- **Vector database** — searchable store of known fraud patterns (FAISS, Pinecone)
- **Vector search** — retrieve the closest matches, put them in the prompt

Update the database to change behavior — no model retraining.

Visual: a transaction vector matching against a catalog of pattern vectors

---

## Inference in the pipeline — the one pattern that matters

Load the model **once per worker**, not once per event.

- A DoFn's `setup()` runs once when a worker starts — load the model + vector DB here
- `process()` runs per event and reuses what's already loaded
- Reloading per event = the pipeline grinds to a halt

This single distinction separates a toy from production.

Visual: a model loaded once, serving many events flowing past

---

## The Beam-native way: RunInference + ModelHandler

Don't call the model by hand — use the framework's standard fitting.

- **RunInference** — Beam's built-in transform for running models in a pipeline
- **ModelHandler** — adapter that says how to load and run your model
- You get batching, model reuse, and metrics for free
- Models become swappable: sklearn, PyTorch, or a remote LLM — same socket

Visual: different model "plugs" fitting one standard socket

---

## Batching, windowing, metrics, routing

The features that make it production-grade.

- **Batched inference** — group events so the GPU stays busy (the cost lever)
- **Windowing** — a rolling fraud rate per time window, by event time (handles late data)
- **Metrics** — counters + latency/risk distributions, queried after the run
- **Tagged outputs** — fraud → alert queue; cleared → analytics

Visual: a dashboard with gauges plus a forking pipeline

---

## Live demo

Transactions classified in real time.

- $5.25 coffee, home city → **LEGIT** (risk 2/100)
- $980 gift cards abroad, minutes after a US charge → **FRAUD** (risk 97/100)
- $7,300 on a dormant account → **FRAUD** (risk 90/100)
- Window fraud rate: 2/3 per 6-second window · batch size: 2 · metrics live

Pull the model's plug → it falls back to rules and keeps running.

Visual: terminal-style output with color-coded verdicts

---

## From this demo to production

The same transforms run unchanged — only the components grow.

- Timed generator → **Kafka / Pub/Sub**
- FAISS / numpy index → **Pinecone / FAISS at scale**
- Local endpoint → **vLLM / AWS Bedrock + SageMaker**
- DirectRunner (laptop) → **DataflowRunner + Kubernetes**

The laptop proves the pattern; the cloud runs it big.

Visual: side-by-side "demo" vs "production" columns

---

## Taming latency, cost, and reliability

- **Latency** — keep the model loaded, batch the work, use a quantized model
- **Cost** — batch on the GPU, cache embeddings, route only ambiguous cases to the big model
- **Reliability** — graceful degradation: if the model is unreachable, fall back and keep flowing
- **Quantization** — lower numerical precision = faster and cheaper, little quality loss

Visual: three dials labeled latency, cost, reliability

---

## Takeaways

- Beam unifies batch and streaming — one codebase, laptop to cloud
- Load the model once per worker — the key cost pattern
- RAG gives live context — update data, not weights
- Run it the Beam-native way: RunInference, batching, windowing, metrics
- Design for graceful degradation — a stream must never hard-fail

The model was never the hard part. The pipeline around it is — and that's what Beam is for.

Visual: clean summary card, five short points

---

## Thank you

Questions?

Samir Sengupta · SyGenticAI
github.com/SamirSengupta · samcodeman.com
Code: github.com/SamirSengupta/Apache-Beam-Summit-New-York-2026

Visual: dark closing slide with a feedback QR placeholder
