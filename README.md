# pulsar_to_sqlite

Read messages from an Apache Pulsar topic (using the Pulsar **Reader** API)
and persist them into a local SQLite database.

The Reader API has no subscription/ack model — nothing on the broker tracks
your position. To make restarts resumable, the script stores the last-read
message id in the same SQLite database (table `reader_state`) and resumes
from there next time it's run against the same topic + db file.

## Requirements

- Python 3.7+
- The `pulsar-client` package:

```bash
pip install pulsar-client
```

## Quick start

First run — start from the earliest message on the topic and store
everything into `messages.db`:

```bash
python pulsar_to_sqlite.py \
  --service-url pulsar://localhost:6650 \
  --topic persistent://public/default/my-topic \
  --from-earliest \
  --db messages.db
```

Later runs against the same `messages.db` automatically resume from the last
message id that was stored for that topic — no `--from-earliest` needed:

```bash
python pulsar_to_sqlite.py \
  --topic persistent://public/default/my-topic \
  --db messages.db
```

By default the script tails the topic forever (like `tail -f`). Add
`--idle-timeout` if you want it to exit once there's been no new message for
a while (handy for one-off backfills / cron jobs):

```bash
python pulsar_to_sqlite.py \
  --topic persistent://public/default/my-topic \
  --from-earliest \
  --idle-timeout 10 \
  --db messages.db
```

## TLS + token authentication

```bash
python pulsar_to_sqlite.py \
  --service-url pulsar+ssl://broker.example.com:6651 \
  --topic persistent://public/default/my-topic \
  --token-file ~/.pulsar/token.jwt \
  --tls-trust-certs-file /etc/ssl/certs/ca-bundle.crt \
  --db messages.db
```

- Use `--token-file <path>` (recommended) or `--token <value>` to authenticate
  with a Pulsar JWT.
- `--tls-trust-certs-file` points at the CA bundle used to verify the
  broker's certificate. Remember the service URL must use a TLS scheme
  (`pulsar+ssl://...`).
- `--tls-allow-insecure` and `--tls-no-hostname-verification` are escape
  hatches for testing against self-signed/dev brokers — don't use them in
  production.

## All options

| Flag | Default | Description |
|---|---|---|
| `--service-url` | `pulsar://localhost:6650` | Pulsar broker service URL |
| `--topic` | *(required)* | Topic to read from, e.g. `persistent://public/default/my-topic` |
| `--reader-name` | *(auto)* | Optional explicit reader name (visible in broker stats) |
| `--db` | `pulsar_messages.db` | Path to the SQLite database file |
| `--from-earliest` | off | On first run only (no stored state yet for this topic in `--db`), start from the earliest message instead of latest |
| `--batch-size` | `100` | Commit to SQLite and persist reader position every N messages |
| `--receive-timeout-ms` | `1000` | Poll timeout per read attempt |
| `--idle-timeout` | *(none)* | Exit after this many seconds with no new messages; omit to run forever |
| `--max-messages` | *(none)* | Stop after storing this many messages |
| `--token` | — | Pulsar JWT auth token (inline) |
| `--token-file` | — | Path to a file containing the Pulsar JWT auth token (preferred over `--token`) |
| `--tls-trust-certs-file` | — | CA bundle / trust certs file for TLS |
| `--tls-allow-insecure` | off | Skip TLS certificate verification (testing only) |
| `--tls-no-hostname-verification` | off | Disable TLS hostname verification (testing only) |

## SQLite schema

**`messages`** — one row per Pulsar message:

| Column | Notes |
|---|---|
| `id` | Autoincrement primary key |
| `message_id` | Hex-encoded serialized Pulsar MessageId, `UNIQUE` (dedupes on re-run) |
| `topic` | Source topic |
| `key` | Partition key, if any |
| `data_text` | Payload decoded as UTF-8 text, if valid |
| `data_base64` | Payload as base64, used when it isn't valid UTF-8 text |
| `properties` | Message properties, stringified |
| `publish_time` | ISO-8601 UTC, from the broker |
| `event_time` | ISO-8601 UTC, if the producer set one |
| `received_at` | ISO-8601 UTC, when this script stored the row |

**`reader_state`** — one row per topic, tracking resume position:

| Column | Notes |
|---|---|
| `topic` | Primary key |
| `last_message_id` | Hex-encoded serialized Pulsar MessageId of the last message stored |
| `updated_at` | ISO-8601 UTC of the last update |

## Notes & caveats

- **Resumability lives in the db file.** Because the Reader API has no
  broker-side cursor, if you point the script at a different (or fresh) db
  file for the same topic, it starts over from `latest`/`earliest` rather
  than picking up where a previous db left off.
- **At-least-once, not exactly-once.** If the process is killed between
  storing rows and the next scheduled flush (every `--batch-size` messages),
  those unflushed messages will be re-read on the next run. The
  `message_id UNIQUE` constraint on `messages` makes re-inserts harmless
  no-ops.
- Stop the script gracefully with `Ctrl+C` (SIGINT) or `SIGTERM` — it flushes
  pending rows and the reader position before exiting.
