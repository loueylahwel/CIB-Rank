# CIB-Rank

**Coordinated Inauthentic Behavior Rank**: a NoSQL (Apache HBase) pipeline that ranks Twitter topics and niches by how much of their engagement (likes, replies, retweets) comes from fake or bot-like accounts.

> **Status: working prototype.** The full stack (Kafka → HBase → Prometheus → Grafana) runs end to end with one command, and the logic is covered by unit tests. The bundled sample data is **synthetic**, so its results show that the code works, not that any real topic is manipulated.

## How it works

```
CSV / Twitter data ─► ingest.py ─► Kafka topic "engagements"
                                      │
                                      ▼
                         consumer.py: score accounts (scoring.py),
                         extract topics (topics.py), sentiment (sentiment.py),
                         write to HBase
                                      │                 │
                                      ▼                 ▼
                              HBase tables        Prometheus metrics ─► Grafana dashboard
                                      │
                                      ▼
                         report.py: topics ranked by suspicious ratio
```

| Layer | File | Role |
|---|---|---|
| Automation | `docker-compose.yml`, `scripts/demo.sh` | HBase (+ZooKeeper, Thrift), Kafka, Prometheus, Grafana; one-command demo |
| Ingestion | `cib_rank/ingest.py` | Reads engagement events from CSV, publishes to Kafka |
| Analysis | `cib_rank/consumer.py`, `pipeline.py` | Scores each account, extracts topics and sentiment, stores results |
| Scoring | `cib_rank/scoring.py` | Heuristic bot score in [0, 1] from 7 signals |
| Topics | `cib_rank/topics.py` | Hashtags plus keywords mapped to niches (`bitcoin` → `crypto`) |
| Sentiment | `cib_rank/sentiment.py` | Lexicon-based score in [-1, 1] |
| Storage | `cib_rank/store.py`, `keys.py` | HBase backend (plus an in-memory backend for tests) |
| Ranking | `cib_rank/ranking.py`, `report.py` | Topics sorted by suspicious-engagement ratio |
| Visualization | `grafana/` | Pre-provisioned Prometheus data source and dashboard |
| Evaluation | `cib_rank/evaluate.py` | Precision and recall against labelled data |

### Bot score signals

| Signal | Weight | Suspicious when |
|---|---|---|
| New account | 0.25 | younger than 30 days |
| Follower / following ratio | 0.20 | followers under 10% of following |
| Posting rate | 0.20 | more than 10 (or 50) posts per day on average |
| Default avatar | 0.10 | no custom profile picture |
| Username digits | 0.10 | 20% or more of the username is digits |
| Duplicate text | 0.10 | the same text posted by other accounts |
| Fast reply | 0.05 | reply within 5 seconds of the original tweet |

An engagement is "suspicious" when its actor's score is at least `0.6` (`SUSPICIOUS_THRESHOLD`). A topic's **suspicious ratio** is suspicious engagements divided by all engagements.

These are heuristics. A high score is evidence, not proof, that an account is fake.

## HBase tables

Row keys are designed around the queries.

| Table | Row key | Column families | Purpose |
|---|---|---|---|
| `accounts` | `user_id` | `p` (profile), `s` (score) | account features and bot score |
| `tweets` | `tweet_id` | `t` (text), `n` (niches, sentiment) | tweet text, extracted topics, sentiment |
| `engagements` | `topic\|reversed_ts\|event_id` | `e` | recent events per topic in one scan |
| `topic_stats` | `topic\|yyyymmddhh` | `s` | hourly counters (`total`, `suspicious`, `sentiment_milli`) using HBase atomic increments |

Counters are only incremented when an engagement row is new, so replaying Kafka messages does not double-count.

## Quick start

### Demo in one command (needs Docker)

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
scripts/demo.sh
```

The script starts the stack, waits for HBase and Kafka, resets the tables, starts the consumer, publishes `data/sample_engagements.csv`, waits for processing, and prints the ranking. Pass another CSV as the first argument to use your own data. It wipes the HBase tables and the Kafka topic each run.

Then open:

| Service | URL |
|---|---|
| Grafana dashboard | http://localhost:3030/d/cib-rank (`admin` / `admin`, local only) |
| Prometheus | http://localhost:9091 |
| HBase master UI | http://localhost:16010 |

The consumer keeps running so the dashboard stays live. Stop everything with:

```bash
kill "$(cat .consumer.pid)"
docker compose down
```

### 1. Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Run the tests (no infrastructure needed)

```bash
python -m pytest -q
```

### 3. Generate and evaluate the synthetic data

```bash
python -m scripts.generate_sample --out data/sample_engagements.csv
python -m cib_rank.evaluate data/sample_engagements.csv
```

On the synthetic data the score separates bots from humans perfectly. That is expected, because the generator was written to produce the patterns the heuristics look for. **It is not evidence that the heuristics work on real Twitter data.** For that, see [Results on real data](#results-on-real-data-fox8-23) below.

### 4. Run the full stack step by step (needs Docker)

```bash
docker compose up -d
python -m cib_rank.admin wait                   # until HBase and Kafka answer
python -m cib_rank.consumer --create-tables     # in a second terminal
python -m cib_rank.ingest data/sample_engagements.csv
python -m cib_rank.report --min-engagements 20  # after events are processed
```

The consumer serves Prometheus metrics on port 8000: `cib_topic_suspicious_ratio`, `cib_topic_engagements_total` and `cib_topic_avg_sentiment`, each labelled by `topic`. Prometheus scrapes them and the provisioned Grafana dashboard shows them.

### Ports

| Port | Service |
|---|---|
| 9090 | HBase Thrift gateway (used by `happybase`) |
| 9091 | Prometheus (moved off 9090) |
| 9092 | Kafka |
| 3030 | Grafana (set `CIB_GRAFANA_PORT` to change) |
| 8000 | Consumer metrics |
| 16010 | HBase master UI |

## Results on real data (fox8-23)

### Dataset

[fox8-23](https://doi.org/10.5281/zenodo.8035289) (Yang & Menczer, 2023, CC BY 4.0) has 1,140 ChatGPT-powered bots from one coordinated network ("fox8", promoting crypto and news sites) and 1,140 human accounts from four older labelled datasets. Each account has up to 200 tweets with full profiles, 368,028 tweets in total. Paper: [Anatomy of an AI-powered malicious social botnet](https://arxiv.org/abs/2307.16336).

```bash
mkdir -p data/raw/fox8
curl -L -o data/raw/fox8/fox8_23_dataset.ndjson.gz \
  "https://zenodo.org/records/8035290/files/fox8_23_dataset.ndjson.gz?download=1"
md5sum data/raw/fox8/fox8_23_dataset.ndjson.gz   # 15fe7f6fbd610084283587f97984792b
python -m scripts.convert_fox8 data/raw/fox8/fox8_23_dataset.ndjson.gz --out data/fox8_events.csv
python -m cib_rank.evaluate data/fox8_events.csv
```

The converter turns each tweet into one engagement event (retweet, reply or post). Add `--max-tweets-per-user 20` for a 45k-event file that is quick to run through the live stack. The raw and converted files are not committed (`data/raw/`, `data/fox8_events*.csv`).

### Profile heuristics alone mostly miss modern bots

| Threshold | Precision | Recall |
|---|---|---|
| 0.6 (default) | 0.857 | **0.016** |
| 0.4 | 0.913 | 0.232 |
| 0.25 | 0.858 | 0.307 |
| 0.2 | 0.750 | 0.383 |

AUC = 0.667, where 0.5 is random.

At the default threshold the score catches 18 of 1,140 bots. What it flags is mostly right, but almost every bot passes. Average signal values for bots against humans show why:

| Signal | Bots | Humans |
|---|---|---|
| New account | 0.28 | 0.03 |
| Default avatar | 0.23 | 0.01 |
| Username digits | 0.18 | 0.06 |
| Posting rate | 0.23 | 0.19 |
| Follower ratio | 0.16 | 0.12 |
| Duplicate text | 0.02 | 0.00 |
| Fast reply | 0.00 | 0.00 |

The fox8 bots have ordinary-looking profiles, and because ChatGPT writes each post, they almost never repeat text. This matches the paper's finding that state-of-the-art detectors fail on these accounts.

### Coordination separates them, with a caveat

A **co-engagement** measure looks at the share of an account's retweets and replies that land on tweets at least 3 accounts engaged with. It separates the groups almost perfectly: bots average 0.60 and humans 0.01. At a 0.2 cutoff, precision is 0.998 and recall 0.999 (AUC 1.0).

Treat that number with caution. The bots are one network active in the same months. The humans were sampled independently from four datasets collected years apart, so they almost never engage with the same tweets. The benchmark's construction inflates the gap. Co-engagement is still the right *kind* of signal for coordinated inauthentic behaviour: CIB shows in what accounts do together, not in any single profile. Measuring it fairly would need humans sampled from the same time and conversations as the bots.

The analysis scripts used for these numbers are not part of the pipeline yet. A co-engagement signal in `scoring.py` is the natural next step.

## Configuration

Environment variables (defaults in `cib_rank/config.py`):

| Variable | Default |
|---|---|
| `CIB_KAFKA_BOOTSTRAP` | `localhost:9092` |
| `CIB_KAFKA_TOPIC` | `engagements` |
| `CIB_KAFKA_GROUP` | `cib-rank-analysis` |
| `CIB_HBASE_HOST` | `localhost` |
| `CIB_HBASE_PORT` | `9090` |
| `CIB_METRICS_PORT` | `8000` |

## Input format

One row per engagement. Columns are listed in `cib_rank/ingest.py` (`EVENT_FIELDS`):
`event_id, timestamp, event_type, tweet_id, tweet_text, author_id, actor_id, actor_username, actor_created_at, actor_followers, actor_following, actor_statuses, actor_default_avatar, reply_latency_seconds`.

`is_bot_truth` (0/1) is optional. It is used only by `cib_rank.evaluate`, never by the pipeline.

## Known limitations

- **Duplicate-text signal is per process.** The consumer keeps the text counts in memory, so a restart resets them.
- **Simple topics.** Topics are hashtags plus a hand-written keyword list (`NICHE_KEYWORDS`). There is no automatic clustering.
- **Simple sentiment.** The lexicon counts positive and negative words, so it misses negation and sarcasm. VADER is a stronger drop-in.
- **Profile heuristics miss modern bots.** On fox8-23 they catch 1.6% of bots at the default threshold (see above). The pipeline has no co-engagement signal yet.
- **Single-node demo stack.** `harisekhon/hbase` runs HBase in one container. Fine for a demo, not for production.
- **Replay safety assumes one writer per event.** Two consumers writing the same event at the same instant could still double-count.

## Ethics and scope

- Use public or dataset data only. Do not scrape accounts, and do not store more personal detail than the analysis needs.
- Report topics and aggregate ratios, not named individuals.
- This is for research and education. Do not use it to target or harass accounts.

## License

MIT. See [LICENSE](LICENSE).
