# Final Podcast Script, aligned to the deck

This conversation mirrors the 29 slide deck section by section, in the same order and
with the same facts and wording. Use it as the single source for your final revision:
read a segment, check it against the matching slide, and confirm your spoken talk says
the same thing. No analogies. Plain language. Two voices.

Hosts:
- MAYA, the host, asks what the audience is thinking.
- DEV, the engineer, explains. This is the voice you can borrow on stage.

Approximate read time: 18 to 20 minutes.

---

## Open  (Slides 1 to 2: Title and Roadmap)

MAYA: Today we are talking about real time AI pipelines at scale, embedding large
language models into Apache Beam for live inference. Dev, set it up.

DEV: The one line version is this. Building the model is the easy part now. The hard
part is running it on live data, at scale, in real time, without falling over. We will
walk through the production gap, the three pressures of latency, cost, and reliability,
what Apache Beam actually is, the building blocks, our fraud pipeline, RAG, the cost
pattern, the Beam native build, how it scales, and a live demo.

MAYA: So the model is not the headline.

DEV: The model is the starting line. Everything after it is the work.

---

## The production gap  (Slide 3)

MAYA: Why is the model the easy part now?

DEV: Open weights, hosted APIs, and fine tuning put a capable model within reach of any
team. You can stand one up in an afternoon. The value, and the difficulty, is everything
that happens after. Serving it on live data, for every user, all the time, inside a
tight latency and cost budget. Projects do not die in the notebook. They die in
production, where the data never stops and nothing is allowed to break.

---

## Pressure one, latency  (Slide 4)

MAYA: First pressure is latency.

DEV: When you tap your card for coffee, the terminal answers in well under a second. A
model sitting in that path cannot take its time. The authorization budget for a card
transaction is tens of milliseconds, not seconds. Industry systems like Visa Advanced
Authorization score risk in under 100 milliseconds using gradient boosted trees and
neural networks, and mature systems hold a p95 latency around 60 to 80 milliseconds.
Anything you add to that path has to respect the same clock.

---

## Pressure two, cost  (Slide 5)

MAYA: Second is cost.

DEV: Running a model once is cheap. Running a heavy model on every event, millions of
times a day, is not. If you send a five dollar coffee to a large reasoning model the
same way you send a suspicious wire transfer, you pay full price for a decision that
simple rules could have made for free. At scale that waste alone can sink the economics.
The fix is leanness. Spend the expensive compute only where it earns its keep, with
three levers: batch, quantize, and route.

---

## Pressure three, reliability  (Slide 6)

MAYA: And third, reliability.

DEV: A batch job can be retried tomorrow. A live stream cannot. It is always on. If the
model goes down, a bank does not stop approving everyone's payments. There is a backup,
a set of predefined rules that keep the line moving until the model recovers. Designing
for that moment, where a dependency fails but the system keeps making sensible calls, is
the difference between a demo and a production service.

---

## Where does the AI fit  (Slide 7)

MAYA: Here is the sharp question. We have had tap since 2010, and fraud scoring is old.
Where does the AI even fit?

DEV: Fair question, and the honest answer makes us stronger. Tap is just the payment
rail. Banks have scored fraud with classical machine learning, gradient boosted trees
like XGBoost, for years. That instant approve or decline is not the LLM's job. Think of
two tiers. The fast tier is a lightweight model making the millisecond call inside the
authorization window, proven, fast, and cheap. The reasoning tier is new: the LLM works
the harder cases, the review queue, and the explanations. It reasons over messy context
and, with RAG, it adapts to a new scam by updating data instead of retraining. This talk
is not about replacing fraud scoring. It is about how you run any heavy model, including
a slow and expensive LLM, on a live stream at scale. Apache Beam is how.

---

## What Apache Beam is  (Slide 8)

MAYA: So what is Apache Beam.

DEV: A unified programming model for data pipelines. You express the logic once, and a
runner executes it, locally or across a cloud cluster. A pipeline is just the sequence of
steps your data flows through. Because the model is unified, the same pipeline runs on
your laptop during development and on a production cluster at scale, with no rewrite. You
change where it runs, not what it does. Three words: unified, portable, and ML aware,
because RunInference makes serving a model a first class step.

---

## The runner  (Slide 9)

MAYA: You keep saying runner. Define it.

DEV: The runner is the engine that executes your pipeline, and you swap it without
changing your code. The DirectRunner runs locally on your machine. It is optimized for
correctness, not performance, and it holds data in memory, so it is for development and
testing. The DataflowRunner, or Flink, or Spark, runs distributed across a cluster with
autoscaling workers, and it is built for production scale. You develop on the
DirectRunner, then point the same code at Dataflow when you are ready.

---

## Batch and streaming  (Slide 10)

MAYA: And it handles both batch and streaming.

DEV: With the same code. Finite data, a file or yesterday's transactions, Beam calls
bounded. An endless live feed from Kafka or Pub/Sub, it calls unbounded. Same transforms,
one is just running forever. So you prototype on a batch of history, then point the
identical pipeline at the live stream. That property is the whole reason to use Beam
instead of a one off script.

---

## Building blocks, part one  (Slide 11)

MAYA: Give me the vocabulary I will see in the code.

DEV: Start with two. A PCollection is your data in motion. The P is for parallel. Beam
assumes it is huge, distributed, and never fully loaded into memory. A PTransform is one
step. It takes a PCollection in and produces a new one out, and you chain steps with the
pipe operator. The whole pipeline is about five lines: source, retrieve, classify, route.

---

## Building blocks, part two  (Slide 12)

MAYA: And the other two.

DEV: A DoFn is your per element logic, the code that runs on each item. The LLM call
lives inside a DoFn. A ParDo applies that DoFn to every element, running many copies in
parallel across workers. And the key point: who scales those workers up and down? The
runner does. You do not write that code.

---

## The cost pattern  (Slide 13)

MAYA: You called one pattern the most important. Which one.

DEV: Load once, infer millions of times. A DoFn has a method called setup that runs once
when a worker starts, and a method called process that runs for every event. Load the
model in setup and you pay the expensive cost once, then reuse it for millions of events.
Load it in process and you reload a giant model on every transaction, and the pipeline
crawls. That one distinction separates a toy from production. If listeners remember one
technical thing, make it this.

---

## The four steps  (Slide 14)

MAYA: Walk me through the pipeline you built.

DEV: It screens credit card transactions for fraud in real time, in four steps. Step one
is the source, transactions arrive, and in production that is Kafka or Pub/Sub. Step two
is retrieve, we find similar known fraud patterns, that is RAG. Step three is classify,
the LLM gives a verdict, a risk score, and a reason. Step four is route, fraud goes to
alerts, cleared goes to analytics. Four transforms, about five lines. The interesting
work is in steps two and three.

---

## RAG, the idea  (Slide 15)

MAYA: Unpack step two.

DEV: RAG is Retrieval Augmented Generation. It means fetching the relevant knowledge at
decision time instead of expecting the model to have memorized it. Rather than cramming
every fraud rule into the model, we keep our known patterns in a searchable store and
pull the closest matches for each transaction. The model then reasons over the
transaction plus that fresh, specific context. Three moves: embed, search, augment.

---

## RAG, under the hood  (Slide 16)

MAYA: And the mechanics.

DEV: We turn the transaction into an embedding, a list of numbers that captures meaning,
so similar meanings sit close together. We keep all our known patterns as embeddings in a
vector database, a store you search by meaning. We use FAISS in the demo, and you would
use something like Pinecone in production. For each transaction we embed it, run a vector
search for the nearest patterns, and attach them. The payoff line: when fraud patterns
change, you update the data, not the model. No retraining.

---

## Inference  (Slide 17)

MAYA: Step three, the model itself.

DEV: We build a prompt from the transaction and its retrieved patterns, and call the
model. Running the model on an input is called inference, so this is live inference, per
transaction, inside the stream. The model returns a structured verdict: a label of FRAUD,
REVIEW, or LEGIT, a risk score, and a short reason. That reason is what makes the output
useful to a human analyst, a dispute team, or an auditor.

---

## Latency, honestly  (Slide 18)

MAYA: Be honest about speed.

DEV: An LLM does not answer in milliseconds, and pretending otherwise invites a fair
challenge. So the LLM is the reasoning layer, not the millisecond gate. A fast,
lightweight model makes the instant approve or decline. The LLM handles the harder cases,
the review queue, and the explanations, and you make it viable with three levers. Batch,
run many transactions together so the GPU stays saturated. Quantize, run at lower
precision for faster and cheaper with little quality loss. Route, let cheap rules clear
the obvious so only the unsure cases reach the LLM.

---

## The demo  (Slide 19)

MAYA: Then you show it live.

DEV: Transactions stream in and each one gets classified in real time. A five dollar and
twenty five cent coffee at a familiar shop comes back LEGIT, risk 2. Nine hundred and
eighty dollars of gift cards in Lagos, minutes after the same card was used in New York,
comes back FRAUD, risk 97, because retrieval pulled up impossible travel and a high risk
merchant. A ninety nine hundred dollar wire, just under the ten thousand dollar reporting
threshold, comes back FRAUD, risk 97, flagged as structuring. A dormant account spike
comes back FRAUD, risk 90. At the bottom you see three things: it batches the model calls,
two at a time, it computes a fraud rate per time window, two out of three in the first
window, and the metrics are built in. Then I pull the model's plug mid stream, and it
falls back to rules and keeps running.

---

## RunInference and ModelHandler  (Slide 20)

MAYA: What makes the production version Beam native.

DEV: Two pieces. RunInference is Beam's built in transform for serving a model in a
pipeline, and it ships with core Beam. You give it a ModelHandler, a small adapter that
says how to load the model and how to run it on a batch. For free you get load once per
worker, batching, metrics, and model sharing across threads. And the model becomes
swappable, sklearn, PyTorch, Hugging Face, or a remote LLM, same socket.

---

## Batched inference  (Slide 21)

MAYA: Batching is a cost lever.

DEV: RunInference groups elements into batches before they hit the model, using
batch_elements_kwargs to set the bounds, two to eight in our demo. Hardware that runs
models is far more efficient on a batch than on one item at a time, so batching is the
main throughput and cost lever. It is a different idea from routing. Batching is about
running several together efficiently. Routing is about not sending the easy cases to the
expensive model at all. Good systems use both.

---

## Windowing by event time  (Slide 22)

MAYA: Then windowing.

DEV: Beyond judging each transaction, we group events into fixed time buckets and compute
a rolling fraud rate per window, six seconds in the demo. The important detail is that
Beam buckets by event time, when the transaction actually happened, not when our code got
around to it. So when data arrives late or out of order, it still lands in the correct
window. Late and out of order data is exactly where a naive script breaks, and it is a
core Beam strength.

---

## Metrics and tagged outputs  (Slide 23)

MAYA: And the last two pieces.

DEV: Metrics are in pipeline instrumentation. Counters, like transactions processed,
frauds detected, and fallbacks. Distributions, like latency and risk score. You query
them after the run, and it is exactly what you wire into monitoring. Tagged outputs are
one step that emits to multiple labeled branches. Fraud cases go to an alert queue,
cleared transactions go to analytics. Real pipelines fan out, they are not a straight
line. Together these make the pipeline observable and routable, the parts you need the
day after you ship.

---

## From laptop to production  (Slide 24)

MAYA: How is a laptop demo not a toy.

DEV: Because the pipeline does not change when you scale it, only the components behind
each step grow. The timed generator becomes Kafka or Pub/Sub. The small FAISS index
becomes Pinecone or FAISS at scale. The local endpoint becomes vLLM, or AWS Bedrock and
SageMaker. The DirectRunner becomes the DataflowRunner with Kubernetes. Same four steps.
The laptop proves the pattern, the cloud runs it big.

---

## Graceful degradation  (Slide 25)

MAYA: Come back to reliability.

DEV: If the model endpoint is unreachable, the pipeline does not crash. It drops to a
transparent set of rules, tags the output as a fallback, and keeps flowing. On stage you
can pull the plug on the model and the stream keeps making sensible decisions, labeled
clearly so no one is misled. The pattern is detect, fall back, recover. The line never
stops. That is what separates a demo from a system.

---

## Takeaways  (Slide 26)

MAYA: Five things to remember.

DEV: One, Beam unifies batch and streaming, so your LLM pipeline is one codebase from
laptop to cloud. Two, load the model once per worker, the key cost pattern. Three, RAG
gives live context, you update data, not weights. Four, use the Beam native path:
RunInference, batching, windowing, metrics. Five, design for graceful degradation, a
stream must never hard fail. The model was never the hard part. The pipeline around it
is, and that is what Apache Beam is for.

---

## Resources  (Slide 27)

MAYA: Where can people get it.

DEV: Everything is open source and runs on a laptop, no GPU and no cloud required. Clone
the repo, install requirements, and run the simple version or the Beam native version. It
is at github.com/SamirSengupta/Apache-Beam-Summit-New-York-2026, and there is a QR code on
the slide.

---

## Thanks to Apache Beam  (Slide 28)

DEV: Before the close, a real thank you to the Apache Beam community. None of this exists
without the unified model, the runners, and RunInference, all built and maintained in the
open. Thank you to the contributors who made serving a model a first class step in a
pipeline, to the Beam Summit organizers and volunteers for the room and the time, and to
everyone who keeps this project alive. It is a privilege to build on your work.

---

## Close  (Slide 29)

MAYA: Land it.

DEV: The same pipeline that screens a handful of transactions on my laptop runs unchanged
against millions in the cloud. The laptop just proves it works. I am Samir Sengupta, AI
and ML engineer at SyGentAI. You can find me at samcodeman.com, on GitHub at
SamirSengupta, and on LinkedIn. Thank you, and I am happy to take questions.
