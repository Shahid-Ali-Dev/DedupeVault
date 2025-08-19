#!/usr/bin/env python3
"""
dedupe_backup.py

A fast, reliable, dependency-free, content-addressable backup tool with chunk
deduplication, snapshotting, verification, and garbage collection (prune).

Repository layout:
  repo/
    db.sqlite3
    chunks/
      ab/
        abcd... (SHA-256 files)
      cd/
        ...
Snapshots store file-to-chunk mappings in SQLite for quick restores and integrity checks.

License: MIT
Author: Shahid Ali
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import dataclasses
import datetime as dt
import hashlib
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Tuple

# ------------------------------- Config ------------------------------------ #

CHUNK_SIZE_DEFAULT = 4 * 1024 * 1024  # 4 MiB
DB_FILENAME = "db.sqlite3"
CHUNKS_DIRNAME = "chunks"

# ------------------------------- Utilities --------------------------------- #


def iso_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def human_bytes(n: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    size = float(n)
    for u in units:
        if size < 1024 or u == units[-1]:
            return f"{size:.2f} {u}"
        size /= 1024.0


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(chunk_size), b""):
            h.update(b)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


# ------------------------------- Database ---------------------------------- #

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    root_path TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    rel_path TEXT NOT NULL,
    size INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    mode INTEGER NOT NULL,
    file_sha256 TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_files_unique
    ON files(snapshot_id, rel_path);

CREATE TABLE IF NOT EXISTS chunks (
    hash TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    ref_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS file_chunks (
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    chunk_hash TEXT NOT NULL REFERENCES chunks(hash),
    PRIMARY KEY (file_id, seq)
);
"""


class RepoDB:
    def __init__(self, repo: Path):
        self.repo = repo
        self.db_path = repo / DB_FILENAME
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.row_factory = sqlite3.Row

    def init(self) -> None:
        with self.conn:
            self.conn.executescript(SCHEMA)

    def begin(self):
        return self.conn

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.conn.close()

    # Snapshot operations
    def create_snapshot(self, root_path: Path) -> int:
        with self.conn:
            cur = self.conn.execute(
                "INSERT INTO snapshots(created_at, root_path) VALUES (?, ?)",
                (iso_now(), str(root_path)),
            )
            return int(cur.lastrowid)

    def list_snapshots(self) -> List[sqlite3.Row]:
        cur = self.conn.execute("SELECT id, created_at, root_path FROM snapshots ORDER BY id DESC;")
        return list(cur.fetchall())

    def snapshot_exists(self, snap_id: int) -> bool:
        cur = self.conn.execute("SELECT 1 FROM snapshots WHERE id = ?;", (snap_id,))
        return cur.fetchone() is not None

    # Chunk operations
    def get_chunk(self, h: str) -> Optional[sqlite3.Row]:
        cur = self.conn.execute("SELECT hash, size, ref_count FROM chunks WHERE hash = ?;", (h,))
        return cur.fetchone()

    def upsert_chunk(self, h: str, size: int, delta_ref: int) -> None:
        with self.conn:
            cur = self.conn.execute("SELECT ref_count FROM chunks WHERE hash = ?;", (h,))
            row = cur.fetchone()
            if row:
                self.conn.execute(
                    "UPDATE chunks SET ref_count = ref_count + ? WHERE hash = ?;",
                    (delta_ref, h),
                )
            else:
                self.conn.execute(
                    "INSERT INTO chunks(hash, size, ref_count) VALUES (?, ?, ?);",
                    (h, size, max(0, delta_ref)),
                )

    def set_chunk_refcount(self, h: str, ref_count: int) -> None:
        with self.conn:
            self.conn.execute("UPDATE chunks SET ref_count = ? WHERE hash = ?;", (ref_count, h))

    # File operations
    def add_file(
        self,
        snapshot_id: int,
        rel_path: str,
        size: int,
        mtime_ns: int,
        mode: int,
        file_sha256: str,
        chunks: List[str],
    ) -> int:
        with self.conn:
            cur = self.conn.execute(
                "INSERT INTO files(snapshot_id, rel_path, size, mtime_ns, mode, file_sha256) "
                "VALUES (?, ?, ?, ?, ?, ?);",
                (snapshot_id, rel_path, size, mtime_ns, mode, file_sha256),
            )
            file_id = int(cur.lastrowid)
            self.conn.executemany(
                "INSERT INTO file_chunks(file_id, seq, chunk_hash) VALUES (?, ?, ?);",
                [(file_id, i, h) for i, h in enumerate(chunks)],
            )
            return file_id

    def files_in_snapshot(self, snapshot_id: int) -> List[sqlite3.Row]:
        cur = self.conn.execute(
            "SELECT id, rel_path, size, mtime_ns, mode, file_sha256 "
            "FROM files WHERE snapshot_id = ? ORDER BY rel_path;",
            (snapshot_id,),
        )
        return list(cur.fetchall())

    def chunks_for_file(self, file_id: int) -> List[str]:
        cur = self.conn.execute(
            "SELECT chunk_hash FROM file_chunks WHERE file_id = ? ORDER BY seq;",
            (file_id,),
        )
        return [r["chunk_hash"] for r in cur.fetchall()]

    def list_orphan_chunks(self) -> List[str]:
        cur = self.conn.execute(
            """
            SELECT c.hash
            FROM chunks c
            LEFT JOIN file_chunks fc ON c.hash = fc.chunk_hash
            WHERE fc.chunk_hash IS NULL OR c.ref_count <= 0;
            """
        )
        return [r["hash"] for r in cur.fetchall()]


# ------------------------------- Storage ----------------------------------- #


@dataclasses.dataclass(frozen=True)
class ChunkLocation:
    path: Path


class ChunkStore:
    def __init__(self, repo: Path):
        self.root = repo / CHUNKS_DIRNAME
        ensure_dir(self.root)

    def _chunk_path(self, h: str) -> Path:
        sub = h[:2]
        p = self.root / sub
        ensure_dir(p)
        return p / h

    def has(self, h: str) -> bool:
        return self._chunk_path(h).exists()

    def write(self, h: str, data: bytes) -> ChunkLocation:
        p = self._chunk_path(h)
        if p.exists():
            return ChunkLocation(p)
        # Write atomically
        with tempfile.NamedTemporaryFile(delete=False, dir=str(p.parent)) as tf:
            tf.write(data)
            tmpname = tf.name
        os.replace(tmpname, p)
        return ChunkLocation(p)

    def read(self, h: str) -> bytes:
        p = self._chunk_path(h)
        return p.read_bytes()

    def delete(self, h: str) -> None:
        p = self._chunk_path(h)
        with contextlib.suppress(FileNotFoundError):
            p.unlink()


# ------------------------------ Chunking ----------------------------------- #


def iter_file_chunks(path: Path, chunk_size: int) -> Iterator[bytes]:
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            yield b


@dataclasses.dataclass
class FileRecord:
    rel_path: str
    size: int
    mtime_ns: int
    mode: int
    file_sha256: str
    chunks: List[str]


def walk_files(root: Path) -> Iterator[Path]:
    for dirpath, _dirnames, filenames in os.walk(root):
        d = Path(dirpath)
        for name in filenames:
            p = d / name
            # Skip repo folders if backing up inside repo by mistake
            if p.name == DB_FILENAME or p.parent.name == CHUNKS_DIRNAME:
                continue
            if p.is_symlink():
                continue
            if not p.is_file():
                continue
            yield p


def relpath_for(root: Path, p: Path) -> str:
    return str(p.relative_to(root))



# ------------------------------- Backup ------------------------------------ #


def backup(
    repo: Path,
    source: Path,
    chunk_size: int = CHUNK_SIZE_DEFAULT,
    workers: int = max(2, os.cpu_count() or 2),
) -> int:
    """
    Creates a new snapshot for 'source' in 'repo', chunk-deduplicated.
    Returns the new snapshot id.
    """
    if not source.exists() or not source.is_dir():
        raise SystemExit(f"Source does not exist or is not a directory: {source}")

    db = RepoDB(repo)
    try:
        db.init()
        store = ChunkStore(repo)
        snapshot_id = db.create_snapshot(source.resolve())
        print(f"[+] Created snapshot {snapshot_id} at {iso_now()} for root {source}")

        # Stage 1: Hash files (full-file hash) and split into chunks
        total_files = 0
        total_bytes = 0

        def process_file(path: Path) -> Optional[FileRecord]:
            nonlocal total_files, total_bytes
            try:
                stat = path.stat()
                size = stat.st_size
                total_bytes += size
                total_files += 1
                file_hash = sha256_file(path)
                chunks: List[str] = []
                for b in iter_file_chunks(path, chunk_size):
                    h = sha256_bytes(b)
                    chunks.append(h)
                    if not store.has(h):
                        store.write(h, b)
                    # update DB refcounts as we go
                    db.upsert_chunk(h, len(b), delta_ref=1)
                rel = relpath_for(source, path)
                return FileRecord(
                    rel_path=rel,
                    size=size,
                    mtime_ns=stat.st_mtime_ns,
                    mode=stat.st_mode,
                    file_sha256=file_hash,
                    chunks=chunks,
                )
            except Exception as e:
                print(f"[!] Skipping {path}: {e}", file=sys.stderr)
                return None

        # Use ThreadPool for IO-bound chunking/writing
        files = list(walk_files(source))
        if not files:
            print("[i] No files discovered; snapshot will be empty.")
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            for rec in ex.map(process_file, files):
                if rec is None:
                    continue
                db.add_file(
                    snapshot_id=snapshot_id,
                    rel_path=rec.rel_path,
                    size=rec.size,
                    mtime_ns=rec.mtime_ns,
                    mode=rec.mode,
                    file_sha256=rec.file_sha256,
                    chunks=rec.chunks,
                )

        print(f"[+] Snapshot {snapshot_id} complete. Files: {total_files}, Bytes: {human_bytes(total_bytes)}")
        return snapshot_id
    finally:
        db.close()


# ------------------------------- Restore ----------------------------------- #


def restore(repo: Path, snapshot_id: int, target: Path) -> None:
    db = RepoDB(repo)
    try:
        if not db.snapshot_exists(snapshot_id):
            raise SystemExit(f"Snapshot {snapshot_id} not found.")
        store = ChunkStore(repo)
        ensure_dir(target)

        files = db.files_in_snapshot(snapshot_id)
        print(f"[+] Restoring snapshot {snapshot_id} to {target} ({len(files)} files)")
        for frow in files:
            rel = frow["rel_path"]
            out_path = target / rel
            ensure_dir(out_path.parent)
            chunks = db.chunks_for_file(int(frow["id"]))
            with tempfile.NamedTemporaryFile(delete=False, dir=str(out_path.parent)) as tf:
                for h in chunks:
                    tf.write(store.read(h))
                tmpname = tf.name
            os.replace(tmpname, out_path)
            os.chmod(out_path, frow["mode"])
            os.utime(out_path, ns=(frow["mtime_ns"], frow["mtime_ns"]))
        print("[+] Restore complete.")
    finally:
        db.close()


# -------------------------------- List ------------------------------------- #


def list_snapshots(repo: Path, snapshot_id: Optional[int], show_files: bool) -> None:
    db = RepoDB(repo)
    try:
        snaps = db.list_snapshots()
        if not snaps:
            print("No snapshots.")
            return

        if snapshot_id is None:
            print("Snapshots:")
            for s in snaps:
                print(f"  {s['id']:>4}  {s['created_at']}  {s['root_path']}")
        else:
            if not db.snapshot_exists(snapshot_id):
                raise SystemExit(f"Snapshot {snapshot_id} not found.")
            s = [x for x in snaps if x["id"] == snapshot_id][0]
            print(f"Snapshot {s['id']} @ {s['created_at']}  root={s['root_path']}")
            if show_files:
                files = db.files_in_snapshot(snapshot_id)
                for f in files:
                    print(f"  {f['rel_path']}  {human_bytes(f['size'])}  sha256={f['file_sha256']}")
    finally:
        db.close()


# ------------------------------ Verification -------------------------------- #


def verify(repo: Path) -> None:
    db = RepoDB(repo)
    try:
        store = ChunkStore(repo)
        # Verify that all chunks referenced by files exist and reconstructable file hash matches
        snaps = db.list_snapshots()
        print(f"[i] Verifying {len(snaps)} snapshots and their files...")
        problems = 0
        for s in snaps:
            sid = int(s["id"])
            files = db.files_in_snapshot(sid)
            for f in files:
                chunks = db.chunks_for_file(int(f["id"]))
                h = hashlib.sha256()
                for ch in chunks:
                    p = store._chunk_path(ch)  # internal, but fine for perf
                    if not p.exists():
                        print(f"[!] Missing chunk {ch} for {f['rel_path']} (snapshot {sid})", file=sys.stderr)
                        problems += 1
                        continue
                    with p.open("rb") as fp:
                        h.update(fp.read())
                if h.hexdigest() != f["file_sha256"]:
                    print(f"[!] Hash mismatch for {f['rel_path']} in snapshot {sid}", file=sys.stderr)
                    problems += 1

        if problems == 0:
            print("[+] Verification successful. No problems found.")
        else:
            print(f"[!] Verification completed with {problems} problem(s).", file=sys.stderr)
    finally:
        db.close()


# ------------------------------- Prune ------------------------------------- #


def prune(repo: Path) -> None:
    db = RepoDB(repo)
    try:
        store = ChunkStore(repo)
        # Recompute refcounts from file_chunks and drop orphans
        print("[i] Recomputing chunk refcounts...")
        with db.begin():
            cur = db.conn.execute("SELECT hash FROM chunks;")
            all_chunks = [r["hash"] for r in cur.fetchall()]
            for h in all_chunks:
                cur2 = db.conn.execute("SELECT COUNT(*) AS c FROM file_chunks WHERE chunk_hash = ?;", (h,))
                c = int(cur2.fetchone()["c"])
                db.set_chunk_refcount(h, c)

        orphans = db.list_orphan_chunks()
        print(f"[i] Found {len(orphans)} unreferenced chunk(s). Deleting from store...")
        deleted = 0
        for h in orphans:
            store.delete(h)
            with db.begin():
                db.conn.execute("DELETE FROM chunks WHERE hash = ?;", (h,))
            deleted += 1
        print(f"[+] Prune finished. Deleted {deleted} chunk(s).")
    finally:
        db.close()


# --------------------------------- CLI ------------------------------------- #


def cmd_init(repo: Path) -> None:
    ensure_dir(repo)
    db = RepoDB(repo)
    try:
        db.init()
        ensure_dir(repo / CHUNKS_DIRNAME)
        print(f"[+] Initialized repository at {repo}")
    finally:
        db.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dedupe_backup",
        description="A dependency-free, content-addressable, deduplicating backup tool.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("init", help="Initialize a new repository.")
    sp.add_argument("repo", type=Path)

    sp = sub.add_parser("backup", help="Create a new snapshot from a folder.")
    sp.add_argument("repo", type=Path)
    sp.add_argument("source", type=Path)
    sp.add_argument("--chunk-size", type=int, default=CHUNK_SIZE_DEFAULT, help="Chunk size in bytes (default: 4MiB)")
    sp.add_argument("--workers", type=int, default=max(2, os.cpu_count() or 2), help="Thread workers for IO (default: CPU count)")

    sp = sub.add_parser("list", help="List snapshots (and optionally files in one snapshot).")
    sp.add_argument("repo", type=Path)
    sp.add_argument("--snapshot", type=int, help="Snapshot id to inspect")
    sp.add_argument("--files", action="store_true", help="Show files in the given snapshot")

    sp = sub.add_parser("restore", help="Restore a snapshot to a target directory.")
    sp.add_argument("repo", type=Path)
    sp.add_argument("snapshot", type=int)
    sp.add_argument("target", type=Path)

    sp = sub.add_parser("verify", help="Verify all snapshots and chunks for integrity.")
    sp.add_argument("repo", type=Path)

    sp = sub.add_parser("prune", help="Garbage-collect unreferenced chunks.")
    sp.add_argument("repo", type=Path)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    cmd = args.cmd
    if cmd == "init":
        cmd_init(args.repo)
    elif cmd == "backup":
        return_code = 0
        try:
            backup(args.repo, args.source, chunk_size=args.chunk_size, workers=args.workers)
        except Exception as e:
            print(f"[!] Backup failed: {e}", file=sys.stderr)
            return_code = 1
        return return_code
    elif cmd == "list":
        list_snapshots(args.repo, args.snapshot, args.files)
    elif cmd == "restore":
        restore(args.repo, args.snapshot, args.target)
    elif cmd == "verify":
        verify(args.repo)
    elif cmd == "prune":
        prune(args.repo)
    else:
        parser.print_help()
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

