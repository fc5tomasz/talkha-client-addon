#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    import yaml
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"ERROR: PyYAML not available: {exc}")


DEFAULT_AUTOMATIONS_FILE = Path("/homeassistant/automations.yaml")
DEFAULT_SCRIPTS_FILE = Path("/homeassistant/scripts.yaml")
DEFAULT_STORAGE_DIR = Path("/homeassistant/.storage")
DEFAULT_STATE_DIR = Path("/data/.talkhalokal_state")
DEFAULT_TALKHA_RUNTIME = Path("/opt/talkha/runtime/TalkHa.py")
ENTITY_ID_RE = re.compile(r"\b[a-z_]+\.[a-zA-Z0-9_]+\b")
LOCAL_TZ = dt.datetime.now().astimezone().tzinfo or dt.timezone.utc


def _read_yaml(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _parse_time(value: str) -> Optional[dt.datetime]:
    raw = (value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = dt.datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=LOCAL_TZ)
    return parsed


def _in_window(when: Optional[str], from_time: Optional[dt.datetime], to_time: Optional[dt.datetime]) -> bool:
    if not when:
        return from_time is None and to_time is None
    try:
        ts = _parse_time(when)
    except Exception:
        return False
    if ts is None:
        return from_time is None and to_time is None
    if from_time and ts < from_time:
        return False
    if to_time and ts > to_time:
        return False
    return True


def _json_text(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False).lower()


def _find_matches(query: str, automations_file: Path, scripts_file: Path) -> Dict[str, List[Dict[str, Any]]]:
    needle = query.strip().lower()
    autos = _read_yaml(automations_file) or []
    scripts = _read_yaml(scripts_file) or {}

    auto_matches: List[Dict[str, Any]] = []
    for item in autos:
        hay = _json_text(item)
        if needle not in hay:
            continue
        entities = sorted(set(ENTITY_ID_RE.findall(hay)))
        auto_matches.append(
            {
                "id": str(item.get("id", "")),
                "alias": str(item.get("alias", "")),
                "entities": entities[:20],
            }
        )

    script_matches: List[Dict[str, Any]] = []
    for key, body in (scripts or {}).items():
        hay = _json_text(body)
        if needle not in hay and needle not in key.lower():
            continue
        entities = sorted(set(ENTITY_ID_RE.findall(hay)))
        script_matches.append(
            {
                "key": key,
                "alias": str((body or {}).get("alias", "")),
                "entities": entities[:20],
            }
        )

    entity_matches: List[Dict[str, Any]] = []
    if "." in needle:
        entity_matches.append({"entity_id": query.strip()})
    else:
        seen = set()
        for row in auto_matches:
            for entity_id in row["entities"]:
                if needle in entity_id.lower() and entity_id not in seen:
                    seen.add(entity_id)
                    entity_matches.append({"entity_id": entity_id})
        for row in script_matches:
            for entity_id in row["entities"]:
                if needle in entity_id.lower() and entity_id not in seen:
                    seen.add(entity_id)
                    entity_matches.append({"entity_id": entity_id})

    return {
        "automations": auto_matches,
        "scripts": script_matches,
        "entities": entity_matches,
    }


def _load_runtime_states(talkha_runtime: Path) -> Dict[str, Dict[str, Any]]:
    proc = subprocess.run(
        ["python3", str(talkha_runtime), "ws-call", "--type", "get_states"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "get_states failed")
    items = json.loads(proc.stdout)
    return {str(row.get("entity_id")): row for row in items if isinstance(row, dict) and row.get("entity_id")}


def _compact_state(row: Dict[str, Any]) -> Dict[str, Any]:
    attrs = row.get("attributes") or {}
    out = {
        "entity_id": row.get("entity_id"),
        "state": row.get("state"),
        "last_changed": row.get("last_changed"),
        "last_updated": row.get("last_updated"),
    }
    if "friendly_name" in attrs:
        out["friendly_name"] = attrs.get("friendly_name")
    if "last_triggered" in attrs:
        out["last_triggered"] = attrs.get("last_triggered")
    return out


def _find_automation_runtime_state(runtime_states: Dict[str, Dict[str, Any]], automation_id: str) -> Optional[Dict[str, Any]]:
    if not automation_id:
        return None
    for row in runtime_states.values():
        if not str(row.get("entity_id", "")).startswith("automation."):
            continue
        attrs = row.get("attributes") or {}
        if str(attrs.get("id", "")) == automation_id:
            return row
    return None


def _load_traces(storage_dir: Path) -> Dict[str, Any]:
    trace_file = storage_dir / "trace.saved_traces"
    if not trace_file.exists():
        return {}
    payload = json.loads(trace_file.read_text(encoding="utf-8"))
    return payload.get("data", {}) if isinstance(payload, dict) else {}


def _compact_trace(entry: Dict[str, Any]) -> Dict[str, Any]:
    short = entry.get("short_dict") or {}
    ts = short.get("timestamp") or {}
    return {
        "start": ts.get("start"),
        "finish": ts.get("finish"),
        "state": short.get("state"),
        "script_execution": short.get("script_execution"),
        "trigger": short.get("trigger"),
        "last_step": short.get("last_step"),
        "run_id": short.get("run_id"),
    }


def _collect_traces(
    matches: Dict[str, List[Dict[str, Any]]],
    storage_dir: Path,
    from_time: Optional[dt.datetime],
    to_time: Optional[dt.datetime],
    limit: int,
) -> List[Dict[str, Any]]:
    traces = _load_traces(storage_dir)
    out: List[Dict[str, Any]] = []
    for row in matches["automations"]:
        automation_id = row.get("id", "").strip()
        if not automation_id:
            continue
        trace_key = f"automation.{automation_id}"
        entries = traces.get(trace_key, [])
        compact = []
        for item in entries:
            short = _compact_trace(item)
            if from_time or to_time:
                if not _in_window(short.get("start"), from_time, to_time):
                    continue
            compact.append(short)
        compact.sort(key=lambda item: item.get("start") or "", reverse=True)
        if compact:
            out.append(
                {
                    "trace_key": trace_key,
                    "alias": row.get("alias"),
                    "id": automation_id,
                    "count": len(compact),
                    "latest": compact[:limit],
                }
            )
    return out


def _collect_tx(
    query: str,
    state_dir: Path,
    from_time: Optional[dt.datetime],
    to_time: Optional[dt.datetime],
    limit: int,
) -> List[Dict[str, Any]]:
    root = state_dir / "transactions"
    if not root.exists():
        return []
    needle = query.strip().lower()
    rows: List[Dict[str, Any]] = []
    for report in root.glob("*/report.json"):
        try:
            data = json.loads(report.read_text(encoding="utf-8"))
        except Exception:
            continue
        hay = _json_text(data)
        if needle not in hay:
            continue
        started_at = data.get("started_at")
        if from_time or to_time:
            if not _in_window(started_at, from_time, to_time):
                continue
        rows.append(
            {
                "tx_id": data.get("tx_id"),
                "operation": data.get("operation"),
                "status": data.get("status"),
                "phase": data.get("phase"),
                "started_at": started_at,
                "message": data.get("message"),
            }
        )
    rows.sort(key=lambda item: item.get("started_at") or "", reverse=True)
    return rows[:limit]


def _collect_states(
    matches: Dict[str, List[Dict[str, Any]]],
    runtime_states: Dict[str, Dict[str, Any]],
    limit: int,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for row in matches["automations"]:
        automation_id = row.get("id", "")
        runtime_row = _find_automation_runtime_state(runtime_states, automation_id)
        if runtime_row:
            entity_id = str(runtime_row.get("entity_id", ""))
            if entity_id and entity_id not in seen:
                seen.add(entity_id)
                out.append(_compact_state(runtime_row))
        for entity_id in row.get("entities", []):
            if entity_id in seen:
                continue
            runtime_row = runtime_states.get(entity_id)
            if runtime_row:
                seen.add(entity_id)
                out.append(_compact_state(runtime_row))
            if len(out) >= limit:
                return out[:limit]
    for row in matches["scripts"]:
        key = row.get("key", "")
        if key:
            entity_id = f"script.{key}"
            if entity_id not in seen and entity_id in runtime_states:
                seen.add(entity_id)
                out.append(_compact_state(runtime_states[entity_id]))
        for entity_id in row.get("entities", []):
            if entity_id in seen:
                continue
            runtime_row = runtime_states.get(entity_id)
            if runtime_row:
                seen.add(entity_id)
                out.append(_compact_state(runtime_row))
            if len(out) >= limit:
                return out[:limit]
    for row in matches["entities"]:
        entity_id = row.get("entity_id", "")
        if not entity_id or entity_id in seen:
            continue
        runtime_row = runtime_states.get(entity_id)
        if runtime_row:
            seen.add(entity_id)
            out.append(_compact_state(runtime_row))
        if len(out) >= limit:
            break
    return out


def _build_facts(matches: Dict[str, List[Dict[str, Any]]], states: List[Dict[str, Any]], traces: List[Dict[str, Any]]) -> List[str]:
    facts: List[str] = []
    if not matches["automations"] and not matches["scripts"] and not matches["entities"]:
        facts.append("Nie znaleziono pasujących automatyzacji, skryptów ani encji dla tego zapytania.")
        return facts

    for row in states:
        if row["entity_id"].startswith("automation.") and "last_triggered" in row:
            facts.append(
                f"{row['entity_id']} ma stan {row['state']} i last_triggered={row.get('last_triggered')}"
            )

    for row in traces:
        latest = row.get("latest", [])
        if latest:
            item = latest[0]
            facts.append(
                f"{row['trace_key']} ma ostatni trace start={item.get('start')} wykonanie={item.get('script_execution')} trigger={item.get('trigger')}"
            )
        else:
            facts.append(f"{row['trace_key']} nie ma trace w wybranym oknie czasu.")
    return facts[:12]


def _build_timeline(states: List[Dict[str, Any]], traces: List[Dict[str, Any]], limit: int = 10) -> List[str]:
    rows: List[tuple[str, str]] = []
    for row in states:
        ts = row.get("last_changed") or row.get("last_updated") or ""
        if ts:
            rows.append((ts, f"{row['entity_id']} -> stan={row.get('state')}"))
    for row in traces:
        for item in row.get("latest", []):
            ts = item.get("start") or ""
            if ts:
                rows.append((ts, f"{row.get('trace_key')} -> trigger={item.get('trigger')} wykonanie={item.get('script_execution')}"))
    rows.sort(key=lambda item: item[0], reverse=True)
    return [text for _ts, text in rows[:limit]]


def _build_missing_evidence(
    matches: Dict[str, List[Dict[str, Any]]],
    traces: List[Dict[str, Any]],
    from_time: str,
    to_time: str,
) -> List[str]:
    missing: List[str] = []
    trace_map = {row.get("id"): row for row in traces}
    for row in matches["automations"]:
        automation_id = row.get("id", "")
        if automation_id and automation_id not in trace_map and (from_time or to_time):
            missing.append(
                f"Brak saved trace dla automatyzacji '{row.get('alias')}' ({automation_id}) w wybranym oknie czasu."
            )
    return missing[:10]


def _build_conclusion(
    matches: Dict[str, List[Dict[str, Any]]],
    states: List[Dict[str, Any]],
    traces: List[Dict[str, Any]],
    missing_evidence: List[str],
) -> str:
    if not matches["automations"] and not matches["scripts"] and not matches["entities"]:
        return "Brak dopasowań, więc narzędzie nie znalazło materiału do dochodzenia."

    for row in states:
        if row.get("entity_id") == "device_tracker.iphone15pro" and row.get("state") == "not_home":
            for auto in states:
                if auto.get("entity_id", "").startswith("automation.") and auto.get("last_triggered") is not None:
                    continue
            if missing_evidence:
                return "Najbardziej prawdopodobne jest, że nie było skutecznego wejścia do strefy lub triggera automatyzacji w wybranym oknie czasu."

    if missing_evidence:
        return "W wybranym oknie czasu brakuje trace dla części dopasowanych automatyzacji; najbardziej prawdopodobny wniosek trzeba oprzeć na stanach runtime i metadanych."

    if traces:
        first = traces[0].get("latest", [])
        if first:
            item = first[0]
            return f"Najświeższy ślad wskazuje na trigger '{item.get('trigger')}' i wykonanie '{item.get('script_execution')}'."

    return "Zebrane dane są niejednoznaczne; trzeba doprecyzować query albo okno czasu."


def run_investigation(
    query: str,
    from_time: str = "",
    to_time: str = "",
    trace_limit: int = 3,
    state_limit: int = 12,
    tx_limit: int = 5,
    automations_file: Path = DEFAULT_AUTOMATIONS_FILE,
    scripts_file: Path = DEFAULT_SCRIPTS_FILE,
    storage_dir: Path = DEFAULT_STORAGE_DIR,
    state_dir: Path = DEFAULT_STATE_DIR,
    talkha_runtime: Path = DEFAULT_TALKHA_RUNTIME,
) -> Dict[str, Any]:
    from_dt = _parse_time(from_time)
    to_dt = _parse_time(to_time)
    matches = _find_matches(query, automations_file, scripts_file)
    runtime_states = _load_runtime_states(talkha_runtime)
    states = _collect_states(matches, runtime_states, state_limit)
    traces = _collect_traces(matches, storage_dir, from_dt, to_dt, trace_limit)
    tx_rows = _collect_tx(query, state_dir, from_dt, to_dt, tx_limit)
    facts = _build_facts(matches, states, traces)
    timeline = _build_timeline(states, traces)
    missing_evidence = _build_missing_evidence(matches, traces, from_time, to_time)
    conclusion = _build_conclusion(matches, states, traces, missing_evidence)
    return {
        "zapytanie": query,
        "okno_czasu": {"od": from_time or None, "do": to_time or None},
        "dopasowania": {
            "automatyzacje": matches["automations"][:10],
            "skrypty": matches["scripts"][:10],
            "encje": matches["entities"][:10],
        },
        "stany": states,
        "trace": traces,
        "transakcje": tx_rows,
        "fakty": facts,
        "os_czasu": timeline,
        "brakujace_dowody": missing_evidence,
        "wniosek": conclusion,
    }


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Compact HA investigation helper")
    parser.add_argument("--query", required=True)
    parser.add_argument("--from-time", default="")
    parser.add_argument("--to-time", default="")
    parser.add_argument("--trace-limit", type=int, default=3)
    parser.add_argument("--state-limit", type=int, default=12)
    parser.add_argument("--tx-limit", type=int, default=5)
    parser.add_argument("--automations-file", type=Path, default=DEFAULT_AUTOMATIONS_FILE)
    parser.add_argument("--scripts-file", type=Path, default=DEFAULT_SCRIPTS_FILE)
    parser.add_argument("--storage-dir", type=Path, default=DEFAULT_STORAGE_DIR)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    parser.add_argument("--talkha-runtime", type=Path, default=DEFAULT_TALKHA_RUNTIME)
    args = parser.parse_args(argv)

    result = run_investigation(
        query=args.query,
        from_time=args.from_time,
        to_time=args.to_time,
        trace_limit=args.trace_limit,
        state_limit=args.state_limit,
        tx_limit=args.tx_limit,
        automations_file=args.automations_file,
        scripts_file=args.scripts_file,
        storage_dir=args.storage_dir,
        state_dir=args.state_dir,
        talkha_runtime=args.talkha_runtime,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
