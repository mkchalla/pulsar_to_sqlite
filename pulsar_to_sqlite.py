#!/usr/bin/env python3
"""
pulsar_to_sqlite.py

Read messages from an Apache Pulsar topic (using the Reader API) and persist
them into a SQLite database.

The Reader API has no subscription/ack model — there's nothing on the broker
tracking your position. To make restarts resumable, this script stores the
last-read MessageId in the same SQLite database (table `reader_state`) and
starts from there next time it's run against the same topic + db file.

Requires:
    pip install pulsar-client

Examples:
    # First run: start from the earliest message, storing everything
    python pulsar_to_sqlite.py --service-url pulsar://localhost:6650 \\
        --topic persistent://public/default/my-topic --from-earliest \\
        --db messages.db

    # Later runs: automatically resumes from the last message id stored
    # in messages.db for this topic (no --from-earliest needed)
    python pulsar_to_sqlite.py --topic persistent://public/default/my-topic \\
        --db messages.db --idle-timeout 10

    # TLS + token authentication
    python pulsar_to_sqlite.py \\
        --service-url pulsar+ssl://broker.example.com:6651 \\
        --topic persistent://public/default/my-topic \\
        --token-file ~/.pulsar/token.jwt \\
        --tls-trust-certs-file /etc/ssl/certs/ca-bundle.crt \\
        --db messages.db
"""

import argparse
import base64
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone

try:
    import pulsar
except ImportError:
    sys.exit(
        "The 'pulsar-client' package is required.\n"
        "Install it with: pip install pulsar-client"
    )


SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id    TEXT UNIQUE,
    topic         TEXT,
    key           TEXT,
    data_text     TEXT,
    data_base64   TEXT,
    properties    TEXT,
    publish_time  TEXT,
    event_time    TEXT,
    received_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_publish_time ON messages(publish_time);

CREATE TABLE IF NOT EXISTS reader_state (
    topic           TEXT PRIMARY KEY,
    last_message_id TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
"""


def utc_iso(ms: int) -> str:
    """Convert a Pulsar epoch-millis timestamp to an ISO-8601 UTC string."""
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


def decode_payload(raw: bytes):
    """Try to decode payload as UTF-8 text; otherwise keep it as base64."""
    try:
        return raw.decode("utf-8"), None
    except UnicodeDecodeError:
        return None, base64.b64encode(raw).decode("ascii")


def resolve_token(args):
    """Return the auth token string from --token or --token-file, if given."""
    if args.token:
        return args.token
    if args.token_file:
        with open(args.token_file, "r") as f:
            return f.read().strip()
    return None


def get_last_message_id(conn, topic):
    row = conn.execute(
        "SELECT last_message_id FROM reader_state WHERE topic = ?", (topic,)
    ).fetchone()
    if row and row[0]:
        return bytes.fromhex(row[0])
    return None


def save_last_message_id(conn, topic, msg_id_bytes):
    conn.execute(
        """
        INSERT INTO reader_state (topic, last_message_id, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(topic) DO UPDATE SET
            last_message_id = excluded.last_message_id,
            updated_at = excluded.updated_at
        """,
        (topic, msg_id_bytes.hex(), datetime.now(timezone.utc).isoformat()),
    )


def build_client_and_reader(args, conn):
    client_kwargs = {}

    token = resolve_token(args)
    if token:
        client_kwargs["authentication"] = pulsar.AuthenticationToken(token)

    if args.tls_trust_certs_file:
        client_kwargs["tls_trust_certs_file_path"] = args.tls_trust_certs_file
    if args.tls_allow_insecure:
        client_kwargs["tls_allow_insecure_connection"] = True
    if args.tls_no_hostname_verification:
        client_kwargs["tls_validate_hostname"] = False

    client = pulsar.Client(args.service_url, **client_kwargs)

    stored_id_bytes = get_last_message_id(conn, args.topic)
    reader_kwargs = {}
    if args.reader_name:
        reader_kwargs["reader_name"] = args.reader_name

    if stored_id_bytes:
        # Resume right after the last message we successfully stored.
        start_message_id = pulsar.MessageId.deserialize(stored_id_bytes)
        inclusive = False
        print(f"Resuming '{args.topic}' after last stored message id "
              f"({stored_id_bytes.hex()})")
    elif args.from_earliest:
        start_message_id = pulsar.MessageId.earliest
        inclusive = True
        print(f"No prior state for '{args.topic}', starting from earliest")
    else:
        start_message_id = pulsar.MessageId.latest
        inclusive = True
        print(f"No prior state for '{args.topic}', starting from latest "
              f"(use --from-earliest to backfill history)")

    reader = client.create_reader(
        topic=args.topic,
        start_message_id=start_message_id,
        start_message_id_inclusive=inclusive,
        **reader_kwargs,
    )
    return client, reader


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--service-url", default="pulsar://localhost:6650",
                         help="Pulsar broker service URL (default: %(default)s)")
    parser.add_argument("--topic", required=True,
                         help="Topic to read from, e.g. persistent://public/default/my-topic")
    parser.add_argument("--reader-name",
                         help="Optional explicit reader name (visible in broker stats)")
    parser.add_argument("--db", default="pulsar_messages.db",
                         help="Path to the SQLite database file (default: %(default)s)")
    parser.add_argument("--from-earliest", action="store_true",
                         help="On first run (no stored state yet for this topic in --db), "
                              "start from the earliest available message instead of latest")
    parser.add_argument("--batch-size", type=int, default=100,
                         help="Commit to SQLite and persist reader position every N messages "
                              "(default: %(default)s)")
    parser.add_argument("--receive-timeout-ms", type=int, default=1000,
                         help="How long to wait for a message before checking idle-timeout "
                              "(default: %(default)s)")
    parser.add_argument("--idle-timeout", type=float, default=None,
                         help="Exit after this many seconds with no new messages. "
                              "Omit to run forever (tail mode).")
    parser.add_argument("--max-messages", type=int, default=None,
                         help="Stop after storing this many messages")

    auth_group = parser.add_argument_group("authentication / TLS")
    token_group = auth_group.add_mutually_exclusive_group()
    token_group.add_argument("--token",
                              help="Pulsar JWT auth token (prefer --token-file to avoid "
                                   "leaking it via shell history/process list)")
    token_group.add_argument("--token-file",
                              help="Path to a file containing the Pulsar JWT auth token")
    auth_group.add_argument("--tls-trust-certs-file",
                             help="Path to a CA bundle / trust certs file for TLS "
                                  "(use pulsar+ssl:// in --service-url)")
    auth_group.add_argument("--tls-allow-insecure", action="store_true",
                             help="Skip TLS certificate verification (testing only, insecure)")
    auth_group.add_argument("--tls-no-hostname-verification", action="store_true",
                             help="Disable TLS hostname verification (testing only, insecure)")

    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.executescript(SCHEMA)
    conn.commit()

    client, reader = build_client_and_reader(args, conn)

    stop = {"flag": False}

    def handle_signal(signum, frame):
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        stop["flag"] = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    total_stored = 0
    uncommitted = 0
    last_msg_id_bytes = None
    last_message_at = time.monotonic()

    insert_sql = """
        INSERT OR IGNORE INTO messages
            (message_id, topic, key, data_text, data_base64, properties,
             publish_time, event_time, received_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def flush():
        """Commit pending message rows and persist the reader position together."""
        nonlocal uncommitted
        if last_msg_id_bytes is not None:
            save_last_message_id(conn, args.topic, last_msg_id_bytes)
        conn.commit()
        uncommitted = 0

    print(f"Reading from '{args.topic}' -> '{args.db}'")

    try:
        while not stop["flag"]:
            try:
                msg = reader.read_next(timeout_millis=args.receive_timeout_ms)
            except Exception as e:
                # pulsar-client raises a timeout exception when receive_timeout_ms elapses
                if "TimeOut" in type(e).__name__ or "Timeout" in str(e):
                    if args.idle_timeout is not None and \
                            (time.monotonic() - last_message_at) >= args.idle_timeout:
                        print(f"Idle for {args.idle_timeout}s, stopping.")
                        break
                    continue
                raise

            last_message_at = time.monotonic()

            data_text, data_b64 = decode_payload(msg.data())
            properties = msg.properties() or {}

            conn.execute(
                insert_sql,
                (
                    msg.message_id().serialize().hex(),
                    args.topic,
                    msg.partition_key() if msg.partition_key() else None,
                    data_text,
                    data_b64,
                    str(properties) if properties else None,
                    utc_iso(msg.publish_timestamp()),
                    utc_iso(msg.event_timestamp()),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            last_msg_id_bytes = msg.message_id().serialize()
            uncommitted += 1
            total_stored += 1

            if uncommitted >= args.batch_size:
                flush()
                print(f"Stored {total_stored} messages so far...")

            if args.max_messages is not None and total_stored >= args.max_messages:
                print(f"Reached --max-messages={args.max_messages}, stopping.")
                break

        if uncommitted:
            flush()

    finally:
        conn.commit()
        conn.close()
        reader.close()
        client.close()
        print(f"Done. Total messages stored: {total_stored}")


if __name__ == "__main__":
    main()
