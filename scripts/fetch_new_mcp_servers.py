#!/usr/bin/env python3
"""Fetch MCP servers newly published to the official registry.

New entries are read from the
[Model Context Protocol registry](https://github.com/modelcontextprotocol/registry)
API. The registry publishes one record per version, so a server that only
bumped its version in the window cannot be distinguished from a new one by the
record alone. Instead, the manifest keeps the set of server names recorded in
past runs; only servers never seen before are listed as new.

Records are read through ``updated_since`` queries and followed through the
registry's cursor pagination until the window is covered. The end of the last
list is stored in the manifest so the next run resumes where the previous one
stopped.
"""

import argparse
import csv
import datetime as dt
import http.client
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REGISTRY_URL = "https://registry.modelcontextprotocol.io/v0/servers"
DEFAULT_USER_AGENT = (
    "new-mcp-servers/1.0 (https://github.com/GHLists/new-mcp-servers)"
)

PAGE_SIZE = 100
MAX_PAGES = 100
PAGE_DELAY_SECONDS = 0.5
DESCRIPTION_LIMIT = 300
REPOSITORY_LIMIT = 150
META_KEY = "io.modelcontextprotocol.registry/official"
CSV_HEADER = (
    "created_at",
    "server",
    "title",
    "version",
    "repository",
    "transports",
    "description",
)

TRANSIENT_ERRORS = (
    urllib.error.URLError,
    TimeoutError,
    json.JSONDecodeError,
    http.client.HTTPException,
    OSError,
)


def iso(moment):
    moment = moment.astimezone(dt.timezone.utc)
    if moment.microsecond:
        fraction = f"{moment.microsecond:06d}".rstrip("0")
        return moment.strftime("%Y-%m-%dT%H:%M:%S") + f".{fraction}Z"
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_timestamp(value):
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    moment = dt.datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=dt.timezone.utc)
    return moment.astimezone(dt.timezone.utc)


def timestamp_filename(moment):
    moment = moment.astimezone(dt.timezone.utc)
    stamp = moment.strftime("%Y-%m-%dT%H-%M-%S")
    if moment.microsecond:
        stamp += "-" + f"{moment.microsecond:06d}".rstrip("0")
    return stamp + "Z"


def fetch_json(url, user_agent, retries=3, backoff=5.0):
    last_error = None
    for attempt in range(1, retries + 1):
        request = urllib.request.Request(
            url,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.load(response)
        except TRANSIENT_ERRORS as error:
            last_error = error
        if attempt < retries:
            print(f"attempt {attempt} failed ({last_error}), retrying", file=sys.stderr)
            time.sleep(backoff * attempt)
    raise RuntimeError(f"failed to fetch {url}: {last_error}")


def fetch_updated_records(since, user_agent, retries, max_pages):
    """Return every registry record changed since ``since``."""
    page = 0
    cursor = None
    records = []
    while page < max_pages:
        query = urllib.parse.urlencode(
            {
                "limit": PAGE_SIZE,
                "updated_since": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
                **({"cursor": cursor} if cursor else {}),
            }
        )
        payload = fetch_json(f"{REGISTRY_URL}?{query}", user_agent, retries=retries)
        if not isinstance(payload, dict) or not isinstance(
            payload.get("servers"), list
        ):
            raise RuntimeError("registry response has an invalid servers field")
        records.extend(payload["servers"])
        cursor = (payload.get("metadata") or {}).get("nextCursor")
        page += 1
        if not cursor:
            return records, True
        time.sleep(PAGE_DELAY_SECONDS)
    return records, False


def official_meta(record):
    meta = (record.get("_meta") or {}).get(META_KEY)
    return meta if isinstance(meta, dict) else {}


def record_name(record):
    server = record.get("server")
    name = server.get("name") if isinstance(server, dict) else None
    return name if isinstance(name, str) else None


def earliest_record(records):
    """Return the record of a server group with the smallest publishedAt."""
    best = None
    best_key = None
    for record in records:
        published = (official_meta(record).get("publishedAt") or "").strip()
        if not published or not isinstance(published, str):
            continue
        if best_key is None or published < best_key:
            best, best_key = record, published
    return best


def clean_text(value, limit=DESCRIPTION_LIMIT):
    text = " ".join(str(value or "").split())
    if len(text) > limit:
        text = text[: limit - 1].rstrip() + "\u2026"
    return text


def build_row(record, created):
    server = record.get("server") or {}
    name = server.get("name") or ""
    repository = server.get("repository")
    repository_url = (
        repository.get("url") if isinstance(repository, dict) else None
    ) or ""
    transports = [
        str(remote.get("type") or "")
        for remote in (server.get("remotes") or [])
        if isinstance(remote, dict) and remote.get("type")
    ]
    transports_text = "; ".join(transports)
    return {
        "created_at": iso(created),
        "server": name,
        "title": server.get("title") or "",
        "version": server.get("version") or "",
        "repository": clean_text(repository_url, REPOSITORY_LIMIT),
        "transports": transports_text,
        "description": clean_text(server.get("description")),
    }


def write_csv(path, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADER)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def read_manifest_text(path):
    """Read the manifest from disk, or fall back to the committed copy.

    The workflow checks out only ``scripts`` from the repository, so the
    manifest can be missing from the working tree even though it is committed.
    """
    manifest_path = Path(path)
    try:
        return manifest_path.read_text(encoding="utf-8")
    except OSError:
        pass
    try:
        result = subprocess.run(
            ["git", "show", f"HEAD:{manifest_path.as_posix()}"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout


def load_manifest(path):
    text = read_manifest_text(path)
    if text is None:
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"manifest {path} is not valid JSON") from error
    if not isinstance(data, dict):
        raise RuntimeError(f"manifest {path} must contain a JSON object")
    version = data.get("state_version", 1)
    if version != 1:
        raise RuntimeError(f"manifest {path} has an unsupported state version")
    return data


def save_manifest(path, manifest):
    manifest_path = Path(path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = manifest_path.with_name(f".{manifest_path.name}.tmp")
    text = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, manifest_path)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--since",
        help="UTC start timestamp as ISO 8601 (default: end of the last list)",
    )
    parser.add_argument(
        "--until",
        help="UTC end timestamp as ISO 8601 (default: now)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=MAX_PAGES,
        help=f"maximum registry pages to walk (default: {MAX_PAGES})",
    )
    parser.add_argument("--output-dir", default="data")
    parser.add_argument("--manifest", default="latest.json")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument(
        "--lookback-hours",
        type=float,
        default=1.0,
        help="window length when no previous list exists (default: 1)",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    now = dt.datetime.now(dt.timezone.utc)
    until = parse_timestamp(args.until) if args.until else now
    manifest = load_manifest(args.manifest)

    if args.since:
        since = parse_timestamp(args.since)
        if "window" in manifest:
            stored_window = parse_timestamp(manifest["window"])
            if since < stored_window:
                raise RuntimeError(
                    "backfill would move the window backwards; "
                    f"the manifest window is {iso(stored_window)}"
                )
    elif "window" in manifest:
        since = parse_timestamp(manifest["window"])
    else:
        since = until - dt.timedelta(hours=args.lookback_hours)

    if since >= until:
        print(f"nothing to do ({iso(since)} >= {iso(until)})", file=sys.stderr)
        return 0

    records, exhausted = fetch_updated_records(
        since, args.user_agent, args.retries, max(1, args.max_pages)
    )

    # Group the raw registry records by server name. The registry publishes
    # one record per version, so a server may appear several times; the
    # earliest record describes the state at first submission.
    grouped = {}
    skipped = 0
    for record in records:
        if not isinstance(record, dict):
            skipped += 1
            continue
        name = record_name(record)
        if not name:
            skipped += 1
            continue
        published = official_meta(record).get("publishedAt")
        try:
            created = parse_timestamp(published)
        except (TypeError, ValueError):
            skipped += 1
            continue
        if created <= since or created > until:
            continue
        grouped.setdefault(name, []).append((created, record))
    if skipped:
        print(f"skipped {skipped} malformed records", file=sys.stderr)

    # A server counts as new only when it was never recorded in a previous
    # run; the registry cannot distinguish first-time publishes from version
    # bumps by itself.
    known = set(manifest.get("known") or [])
    groups = {}
    for name, entries in grouped.items():
        groups[name] = min(entries, key=lambda entry: entry[0])
    new_names = [name for name in sorted(groups) if name not in known]
    rows = []
    for name in new_names:
        created, record = groups[name]
        rows.append(build_row(record, created))
    rows.sort(key=lambda row: row["created_at"])

    known |= set(groups)
    manifest["known"] = sorted(known)

    manifest["window"] = iso(until)
    manifest["source_truncated"] = not exhausted
    if rows:
        output = (
            Path(args.output_dir) / f"new-mcp-servers-{timestamp_filename(until)}.csv"
        )
        write_csv(output, rows)
        manifest["list"] = {
            "path": output.as_posix(),
            "from": iso(since),
            "to": iso(until),
            "count": len(rows),
        }
        print(
            f"wrote {len(rows)} MCP servers published between {iso(since)} "
            f"and {iso(until)} to {output}"
        )
    else:
        print(f"no new MCP servers between {iso(since)} and {iso(until)}")
    if not exhausted:
        print(
            "registry page limit reached; the window may be incomplete",
            file=sys.stderr,
        )
    save_manifest(args.manifest, manifest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
