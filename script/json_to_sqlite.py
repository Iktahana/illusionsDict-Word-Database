#!/usr/bin/env python3
"""
JSON データを SQLite データベースに変換するスクリプト

data/ 以下の全 JSON ファイルを読み込み、単一の SQLite DB にまとめる。
"""

import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
_DATA_DIR = _REPO_ROOT / "data"


def _git(args: list[str]) -> str:
    """Run a git command and return stripped stdout, or empty string on failure."""
    try:
        return subprocess.check_output(
            ["git", *args], cwd=_REPO_ROOT, stderr=subprocess.DEVNULL, text=True
        ).strip()
    except Exception:
        return ""


def create_metadata(conn: sqlite3.Connection, entry_count: int) -> None:
    """Create and populate the build metadata table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _metadata (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    version = os.environ.get("GENJI_VERSION") or _git(["describe", "--tags", "--always"])
    commit = os.environ.get("GENJI_COMMIT") or _git(["rev-parse", "HEAD"])
    commit_short = commit[:8] if commit else ""
    branch = os.environ.get("GENJI_BRANCH") or _git(["rev-parse", "--abbrev-ref", "HEAD"])
    repo = os.environ.get("GENJI_REPO") or _git(["remote", "get-url", "origin"])
    build_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows = [
        ("version", version),
        ("commit", commit),
        ("commit_short", commit_short),
        ("branch", branch),
        ("repository", repo),
        ("build_date", build_date),
        ("entry_count", str(entry_count)),
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO _metadata (key, value) VALUES (?, ?)", rows
    )
    print(f"Metadata: version={version} commit={commit_short} branch={branch}")


def create_fts(conn: sqlite3.Connection) -> None:
    """全文検索（FTS5）テーブルを作成する"""
    print("FTS5 テーブルを作成中...")
    conn.executescript("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_entries USING fts5(
            uuid UNINDEXED,
            entry,
            reading_primary,
            tokenize='unicode61'
        );

        INSERT INTO fts_entries(uuid, entry, reading_primary)
        SELECT uuid, entry, reading_primary FROM entries;

        CREATE VIRTUAL TABLE IF NOT EXISTS fts_definitions USING fts5(
            entry_uuid UNINDEXED,
            gloss,
            tokenize='unicode61'
        );

        INSERT INTO fts_definitions(entry_uuid, gloss)
        SELECT entry_uuid, gloss FROM definitions WHERE gloss IS NOT NULL;
    """)
    print("FTS5 テーブル作成完了")


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS entries (
            uuid            TEXT PRIMARY KEY,
            entry           TEXT NOT NULL,
            reading_primary TEXT,
            reading_alternatives TEXT,  -- JSON array
            is_heteronym    INTEGER DEFAULT 0,
            pos             TEXT,       -- JSON array
            ctype           TEXT,
            inflections     TEXT,       -- JSON
            relations       TEXT,       -- JSON
            meta            TEXT,       -- JSON
            raw_json        TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS definitions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_uuid      TEXT NOT NULL REFERENCES entries(uuid),
            def_index       INTEGER,
            gloss           TEXT,
            register        TEXT,
            nuance          TEXT,
            scenarios       TEXT,       -- JSON array
            sensory_tags    TEXT,       -- JSON
            collocations    TEXT,       -- JSON array
            examples        TEXT,       -- JSON
            UNIQUE(entry_uuid, def_index)
        );

        CREATE INDEX IF NOT EXISTS idx_entries_entry ON entries(entry);
        CREATE INDEX IF NOT EXISTS idx_entries_reading ON entries(reading_primary);
        CREATE INDEX IF NOT EXISTS idx_definitions_uuid ON definitions(entry_uuid);
    """)


def insert_entry(conn: sqlite3.Connection, item: dict) -> None:
    reading = item.get("reading", {})
    grammar = item.get("grammar", {})

    conn.execute(
        """INSERT OR REPLACE INTO entries
           (uuid, entry, reading_primary, reading_alternatives, is_heteronym,
            pos, ctype, inflections, relations, meta, raw_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            item.get("uuid"),
            item.get("entry"),
            reading.get("primary"),
            json.dumps(reading.get("alternatives", []), ensure_ascii=False),
            1 if reading.get("is_heteronym") else 0,
            json.dumps(grammar.get("pos", []), ensure_ascii=False),
            grammar.get("ctype"),
            json.dumps(grammar.get("inflections"), ensure_ascii=False) if grammar.get("inflections") else None,
            json.dumps(item.get("relations", {}), ensure_ascii=False),
            json.dumps(item.get("meta", {}), ensure_ascii=False),
            json.dumps(item, ensure_ascii=False),
        ),
    )

    for defn in item.get("definitions", []):
        conn.execute(
            """INSERT OR REPLACE INTO definitions
               (entry_uuid, def_index, gloss, register, nuance,
                scenarios, sensory_tags, collocations, examples)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                item.get("uuid"),
                defn.get("index"),
                defn.get("gloss"),
                defn.get("register"),
                defn.get("nuance"),
                json.dumps(defn.get("scenarios", []), ensure_ascii=False),
                json.dumps(defn.get("sensory_tags", {}), ensure_ascii=False),
                json.dumps(defn.get("collocations", []), ensure_ascii=False),
                json.dumps(defn.get("examples", {}), ensure_ascii=False),
            ),
        )


def main() -> None:
    output_path = _REPO_ROOT / "genji.db"
    if output_path.exists():
        output_path.unlink()

    json_files = sorted(_DATA_DIR.rglob("*.json"))
    if not json_files:
        print("No JSON files found in data/", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(json_files)} JSON files")

    conn = sqlite3.connect(str(output_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    create_schema(conn)

    count = 0
    errors = 0
    conn.execute("BEGIN")
    for i, f in enumerate(json_files, 1):
        try:
            items = json.loads(f.read_text(encoding="utf-8"))
            if isinstance(items, dict):
                items = [items]
            for item in items:
                insert_entry(conn, item)
                count += 1
        except Exception as e:
            errors += 1
            if errors <= 10:
                print(f"Warning: {f}: {e}", file=sys.stderr)

        if i % 10000 == 0:
            conn.execute("COMMIT")
            conn.execute("BEGIN")
            print(f"  Processed {i}/{len(json_files)} files ({count} entries)")

    conn.execute("COMMIT")
    create_metadata(conn, count)
    create_fts(conn)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("VACUUM")
    conn.close()

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"Done: {count} entries written to {output_path.name} ({size_mb:.1f} MB)")
    if errors:
        print(f"  ({errors} files had errors)")


if __name__ == "__main__":
    main()
