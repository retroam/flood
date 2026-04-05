# Quake SQL — Loom Script

**Target length:** ~3 minutes
**Tone:** Casual but sharp. You're showing someone technical what you built and why it's interesting.

---

## INTRO (screen: browser with app open, Query tab visible)

> "This is Quake SQL — a natural-language interface to live USGS earthquake data. You ask a question in plain English, and it generates ClickHouse SQL, runs it, and gives you the results. The interesting part isn't the chat-to-SQL idea — it's *how* the SQL gets generated, and how I evaluate whether it actually works."

---

## DEMO: QUERY TAB (~45 seconds)

**Action:** Click the example chip "How many earthquakes happened in the last 24 hours?"

> "Let's start simple."

**Action:** Wait for results. Point out the Generated SQL panel.

> "The model returned a COUNT with a time filter — ran against a live ClickHouse table with a month of USGS earthquake data. You can see the SQL right here."

**Action:** Open the Run Metrics panel.

> "We track latency and estimated API cost per query. This came back in about a second for a fraction of a cent."

**Action:** Type or click "Show the top 10 regions by average magnitude in the last 7 days."

> "Something harder — aggregation with GROUP BY and ORDER BY."

**Action:** Wait for results.

> "Correct result set, ten rows, sorted by average magnitude. What's doing the heavy lifting is that the model's output is *grammar-constrained* — it literally cannot produce invalid SQL syntax. Let me show you what that means."

---

## ARCHITECTURE TAB (~60 seconds)

**Action:** Click the Architecture tab.

> "Here's the system architecture."

**Action:** Point to the system diagram.

> "The question goes to FastAPI, then to the OpenAI Responses API with a Lark grammar attached as a custom tool. The grammar defines exactly what valid ClickHouse SQL looks like — allowed columns by type, valid aggregates, time functions, operators. The model's decoder is forced to follow that grammar at every token. Syntactically valid SQL by construction, not by hoping."

**Action:** Point to the validator node and the retry arrow.

> "After generation, there's a semantic validator that checks things the grammar is intentionally permissive about — like whether a column name exists in the schema, or whether a LIMIT was actually requested. If validation fails, we retry up to three times with exponential backoff and increasing temperature to escape stuck patterns."

**Action:** Point to the LIMIT cap in the grammar comment.

> "We also enforce LIMIT at the grammar level — capped at 500. That constraint lives in the grammar itself, not just in post-hoc validation. Every expected SQL in the eval suite is verified to parse against the grammar, so we know the grammar can actually produce what we're testing for."

**Action:** Scroll to the comparison diagram (Traditional vs. Constrained).

> "This is the key difference. Traditional text-to-SQL parses output after the fact and hopes for the best. Here, malformed SQL is structurally impossible. SQL injection through prompt manipulation doesn't work because the grammar won't allow it — we have eight adversarial eval cases that test exactly that."

---

## EVALS TAB (~60 seconds)

**Action:** Click the Eval Results tab.

> "The eval suite has 38 benchmark cases. It runs them twice — once with the grammar constraint, once without — so we're doing a direct ablation on the same model."

**Action:** Point to the grouped bar chart (Result Accuracy, SQL Equivalence, Hallucination).

> "Three different ways to measure correctness. Result accuracy asks: did the query return the same data? SQL equivalence asks: did the query have the same WHERE, GROUP BY, ORDER BY, and LIMIT — ignoring column selection and aliases? And hallucination control asks: did the model stay within the schema?"

> "This matters because the old scorer penalized queries that returned extra columns. If the model wrote SELECT * instead of three specific columns, it was marked wrong even though the data was correct. Now we can see the full picture."

**Action:** Point to the Accuracy vs. Cost scatter.

> "This is the tradeoff view — accuracy versus cost per query."

**Action:** Open the Per-Sample Results table.

> "You can also drill into individual cases. Each one shows whether the result matched, whether the SQL structure matched, and whether it hallucinated. The adversarial cases — things like UNION injection via natural language, prompt override attempts, requests for fake columns — those should all be UNSUPPORTED. If no-CFG returns real SQL for those, it gets marked wrong."

**Action:** Point to a few adversarial rows.

> "And they do get marked wrong. That's the grammar earning its keep — it prevents the model from answering questions it shouldn't."

---

## TESTS (~15 seconds)

**Action:** Switch to terminal, run `pytest tests/ -v --tb=short` (or show pre-recorded output).

> "113 unit tests, all runnable without an API key or database. They cover the grammar — what it accepts, what it rejects — the SQL validator, the data transforms, schema consistency, and the eval helpers like column projection and SQL structural comparison. These are the tests the take-home should have had from the start."

---

## CLOSE (~15 seconds)

**Action:** Switch back to the Query tab.

> "That's Quake SQL — grammar-constrained generation so the model can't produce bad SQL, a two-layer validator for semantic checks, a 38-case eval pipeline that separates SQL correctness from result matching, and a test suite that proves the grammar does what it claims. Thanks for watching."

---

## RECORDING TIPS

- **Resolution:** 1920x1080, browser zoom at 100%.
- **Tab order:** Query → Architecture → Evals → Terminal → Query. Follows the narrative arc: *what it does → how it works → how well it works → proof it's tested*.
- **Pacing:** Pause briefly after each query returns so the viewer can read the SQL and results. Don't narrate while typing.
- **Cursor:** Use your cursor to point at the specific chart/diagram element you're discussing. Loom highlights cursor movement.
- **Mistakes:** If a query returns an error or unexpected result, don't cut — acknowledge it. ("Looks like that one hit the latency budget — you can see it flagged in the metrics.") It shows the system is real.
- **Time budget:** Intro 15s + Demo 45s + Architecture 60s + Evals 60s + Tests 15s + Close 15s = ~3:30. Trim the architecture section if you're running long.
