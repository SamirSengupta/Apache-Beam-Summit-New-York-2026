# Talk Script — Real-Time AI Pipelines at Scale (Beam Summit 2026)

Plain, no analogies. First-person, ready to rehearse and deliver. Everything in **[ ]**
is a stage direction, not spoken. **[ASK]** = audience-interaction beat. Timings are
cumulative against a 50-minute slot (≈40 min talk + 10 min Q&A).

Legend: **[CLICK]** = advance slide · **[DEMO]** = switch to terminal · **[PAUSE]** =
let it land · **[ASK]** = engage the room.

---

## 0:00 — Open (Slide 1: Title)

[Stand center. Smile. Don't rush.]

Hi everyone — I'm Samir. I build AI/ML systems, and today's talk is about the part of
AI nobody brags about: getting a model to run on live data, at scale, without falling
over.

**[ASK]** Quick show of hands — who bought a coffee on the way here this morning?
[hands] When you tapped your card, the terminal said "approved" in under a second. In
that one second, a system somewhere decided your payment wasn't fraud. That decision —
made instantly, on live data, millions of times a minute — is exactly what this talk
is about.

The title is "Real-Time AI Pipelines at Scale": embedding large language models into
Apache Beam for live inference. Let me start with why it's hard.

---

## 2:00 — The real problem (Slide 2)

[CLICK]

My core point: **building the model is the easy part now.** You can train or grab a
strong model in an afternoon. AI isn't the finish line — it's the starting line.

The hard work starts when that model has to run not for ten users, not a thousand, but
for everyone, all the time, on data arriving live that never stops. And the moment you
put a model on a live stream, three problems hit you at once.

[CLICK — three points]

**One — latency.** The decision has to come back fast. Your coffee got approved in
milliseconds, not minutes. If a model sits in that path, it can't be slow.

**Two — cost.** If I run a heavy model on every single transaction — including a
five-dollar coffee — I burn money on inference all day. At scale, that bill alone can
sink the project.

**Three — reliability.** A live stream never stops, so the system can't either. If the
model goes down, the bank doesn't stop approving everyone's payments. There's a backup
that keeps things running until the model is back.

[PAUSE] Latency, cost, reliability. Everything I show today serves those three. The
tool that lets me handle all three is Apache Beam.

---

## 6:00 — What Apache Beam is (Slide 3)

[CLICK]

Apache Beam, in one line: **you write a data pipeline once, and run it anywhere, at any
scale.** A pipeline is just the sequence of steps your data flows through.

Two ideas make Beam powerful.

**First — the runner.** The runner is the engine that executes your pipeline. While I
develop, I use the **DirectRunner**, which runs the whole thing locally on my laptop —
fast to test. For production I change one setting — the runner — to **Dataflow** on
Google Cloud, or Flink, or Spark, and the *same code* runs distributed across a cluster
that scales to millions of events. I don't rewrite my logic; I swap the engine.
[Be precise:] the DirectRunner is for local development — the real scale comes from the
production runner.

**Second — one model for batch and streaming.** Finite, historical data Beam calls
*bounded*. An endless live feed it calls *unbounded*. Same code for both. So I can test
on a batch of yesterday's transactions, then point the identical pipeline at the live
stream. That's why I use Beam instead of writing a one-off script.

---

## 10:00 — The core vocabulary (Slide 4)

[CLICK]

Four terms and you can read any Beam pipeline.

**PCollection** — your data moving through the pipeline. The "P" is for parallel; Beam
assumes it's huge and spread across machines, and it never loads the whole thing into
memory — data flows through.

**PTransform** — one step. It takes a PCollection in and produces a new one out. You
chain steps together.

**DoFn** — "do function." This is *your* logic — the code that runs on each element.
The LLM call lives inside a DoFn.

**ParDo** — "parallel do." It applies your DoFn to every element, running many copies
in parallel across workers. Scaling those workers up and down is the runner's job, not
something you code.

And two methods inside a DoFn that matter a lot:

**setup()** runs **once per worker**, before any data — that's where I load the model
and connect to the database. **process()** runs **once per event** and reuses what
setup already loaded. [Slow down:] Load the model in the wrong place — per event — and
the pipeline crawls. Load once, reuse for millions of events. If you remember one
technical thing today, make it that.

---

## 13:00 — What we built (Slide 5)

[CLICK]

Here's the pipeline. It screens credit-card transactions for fraud in real time. Four
steps:

[CLICK through]

- **Step 1 — Source.** Transactions arrive. In production this reads from a live feed
  like Kafka or Pub/Sub.
- **Step 2 — Retrieve.** For each transaction, look up the most similar known fraud
  patterns. This runs *before* the model and feeds it context.
- **Step 3 — Classify.** The LLM makes the call: fraud, review, or legit — with a risk
  score and a one-line reason.
- **Step 4 — Route.** Send the result where it needs to go: cleared one way, flagged
  another.

Four steps, about five lines of pipeline code. Let me unpack steps two and three.

---

## 16:00 — Step 2: RAG (Slide 6)

[CLICK]

Step two is **RAG — Retrieval-Augmented Generation.** Plainly: instead of expecting the
model to have memorized every fraud rule, I fetch the relevant ones at decision time and
hand them to the model.

How it works: I turn each transaction into an **embedding** — a list of numbers that
captures its meaning, so similar things end up with similar numbers. I keep all my known
fraud patterns as embeddings in a **vector database** — a store you can search by
meaning. We use FAISS; in production you'd use something like Pinecone. When a
transaction comes in, I embed it, run a **vector search** for the closest patterns, and
attach them.

[Payoff:] When fraud patterns change, I update the database — not the model. No
retraining. I change behavior by changing data.

---

## 19:00 — Step 3: inference, loaded once (Slide 7)

[CLICK]

Step three runs the model on the transaction plus the retrieved patterns. Running a
model on an input is called **inference** — so this is live inference, per transaction,
inside the stream. And this is where setup() earns its keep: the model is loaded once
per worker and reused for every transaction.

---

## 21:00 — An honest word on latency (still Slide 7)

[Tone: candid, confident — this pre-empts the hardest question.]

Let me be straight with you, because you're a sharp crowd. Your coffee was approved in
milliseconds — but an LLM does **not** answer in milliseconds. In my demo you'll see a
few seconds per transaction.

So how do real systems handle that? They don't put the big LLM in the millisecond
path. The instant approve/decline is done by a fast, lightweight model. The LLM is the
**reasoning layer** — for the harder cases, for review, for explanations — and you make
it affordable with three levers I'll show: batching, a smaller or quantized model, and
routing only the ambiguous cases to it. Keep that honest and the architecture holds up.

Enough slides — let me show it running.

---

## 22:00 — DEMO (Slide 8)

[DEMO — terminal, big font, pre-warmed.]

This is a real Apache Beam pipeline. Transactions stream in; for each one you'll see it
retrieve patterns, ask the model, and print a verdict with a risk score and reason.

**[ASK]** Play along — I'll read each one, you call it: fraud or fine?

[Run it.]

[Coffee] $5.25 coffee, the shop this cardholder visits every morning. [room] — **Legit**,
risk 2. It's not trigger-happy.

[Gift cards] $980 of gift cards in Lagos, four minutes after the same card was used in
New York. [room] — **Fraud**, risk 97. It caught two patterns — impossible travel and a
high-risk merchant — and connected them.

[Wire] $9,900 — just under the $10,000 reporting threshold. The model flags it and
explains "structuring." A plain amount rule misses why that exact number is suspicious;
the retrieved context is what makes it smart.

[Let the rest scroll. Point to the bottom.]

Now the part that makes this a real Beam pipeline, not a script:

- It ran the model in **batches** — two at a time, not one by one.
- It computed a **fraud rate per time window** — a live streaming metric.
- And these **metrics** — count processed, frauds caught, average latency — are built
  into the pipeline.

**[ASK]** What happens if the model just dies mid-stream?

[If comfortable: kill the endpoint, re-run a moment.]

The verdicts now say "fallback." The model's gone and the pipeline didn't crash — it
dropped to simple rules and kept making sensible calls. That's reliability, live.

[Back to slides.]

---

## 30:00 — How it's wired for production (Slide 9)

[CLICK]

Two pieces worth naming.

**RunInference** — Beam's built-in step for running a model in a pipeline. I don't
hand-code the model call; I give Beam a **ModelHandler** — a small adapter that says how
to load the model and how to run it on a batch. That gives me batching, load-once, and
metrics for free, and lets me swap models — sklearn, PyTorch, a remote LLM — without
touching the pipeline.

The **batching** you saw is the first cost lever: run several transactions together so
the hardware stays busy. That's different from **routing** — the second lever — where
cheap rules clear the obvious cases and only the unsure ones reach the expensive model.

On the streaming side: **windowing** groups events into fixed time buckets — that's how
I get "fraud rate per six seconds." And Beam buckets by **event time** — when the
transaction actually happened — so late or out-of-order data still lands in the right
window. The fork at the end — fraud to alerts, cleared to analytics — is **tagged
outputs.**

---

## 34:00 — From laptop to production (Slide 11)

[CLICK]

How is a laptop demo not a toy? Because the pipeline doesn't change when you scale —
only the pieces behind each step.

[CLICK — swap table]

The demo source becomes **Kafka or Pub/Sub**. The small FAISS store becomes **Pinecone**
with millions of patterns. The local model becomes a high-throughput server like
**vLLM**, or a managed service like **Bedrock and SageMaker**. The DirectRunner becomes
**Dataflow** on an autoscaling cluster. Same four steps. The laptop proves the pattern;
the cloud runs it big.

[Benchmarks — only claim what you can back:] In the systems I've worked on, batching
plus a quantized model plus routing brought the cost per decision down a lot, and the
savings let us afford a stronger model on the cases that actually needed it.

---

## 38:00 — Takeaways (Slide 13)

[CLICK]

Five things to take home:

One — Beam unifies batch and streaming, so your LLM pipeline is one codebase, laptop to
cloud.

Two — load the model once per worker. The single biggest cost pattern.

Three — RAG gives the model live context; you update data, not weights.

Four — use the Beam-native path: RunInference, batching, windowing, metrics.

Five — design for graceful degradation. A live stream must never hard-fail.

[PAUSE] The model was never the hard part. The pipeline around it is — and that's what
Apache Beam is for.

Thank you.

[CLICK — Slide 14: Thank you / QR]

---

## 40:00 — Q&A (Slide 14)

[Anchors for the likely questions:]

- **"We've had tap since the early 2010s — where does AI even fit?"**
  "Tap is just the payment method, and banks have scored fraud with rules and classical
  ML for years — that instant approve/decline isn't the LLM's job. The LLM is the layer
  behind it: it reasons over messy context, gives a human-readable reason an analyst can
  act on, and — with RAG — adapts to a new scam by updating data instead of retraining.
  My real subject isn't 'AI replaces fraud scoring' — it's how you run *any* heavy model
  on a live stream at scale, and Beam is how."

- **"How is this real-time if the LLM takes seconds?"** The LLM is the reasoning layer,
  not the millisecond gate; a fast model does the instant call, and batching, quantization,
  and routing keep the LLM viable.

- **"Why RunInference instead of a plain DoFn?"** It standardizes load-once, batching, and
  metrics, and makes models swappable.

- **"Late data / ordering / exactly-once?"** Beam's event-time windowing handles late data;
  runners like Dataflow give exactly-once.

- **"Why Beam over Spark/Flink directly?"** Portability — same code, pick the runner — and
  RunInference makes ML first-class.

- If you don't know: "Great question — I haven't hit that in production; let's talk after."
  Never bluff.

[End a minute early if you can. Thank them again.]

---

### Timing checkpoints (glance at the room clock)

- ~13 min in → starting the pipeline overview (Slide 5).
- ~22 min in → launching the demo.
- ~40 min in → in Q&A.
- If behind: compress the vocabulary (Slide 4) and RAG (Slide 6). Never rush the demo or
  the takeaways.
