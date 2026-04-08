#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import os
import re
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    import yaml
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"ERROR: PyYAML not available: {exc}")


DEFAULT_AUTOMATIONS_FILE = Path("/homeassistant/automations.yaml")
DEFAULT_SCRIPTS_FILE = Path("/homeassistant/scripts.yaml")
DEFAULT_HA_BASE_URL = "http://homeassistant.local:8123/api"
ENTITY_ID_RE = re.compile(r"\b[a-z_]+\.[a-zA-Z0-9_]+\b")


def _read_yaml(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _parse_time(value: str) -> Optional[dt.datetime]:
    raw = (value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    return dt.datetime.fromisoformat(raw)


def _token() -> str:
    token = os.environ.get("HOMEASSISTANT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("HOMEASSISTANT_TOKEN is required")
    return token


def _api_get(path: str, params: Optional[Dict[str, str]] = None, base_url: str = DEFAULT_HA_BASE_URL) -> Any:
    query = f"?{urllib.parse.urlencode(params)}" if params else ""
    req = urllib.request.Request(
        f"{base_url}{path}{query}",
        headers={
            "Authorization": f"Bearer {_token()}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _normalize_entities(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        for part in value.split(","):
            entity_id = part.strip()
            if not entity_id or entity_id in seen:
                continue
            seen.add(entity_id)
            out.append(entity_id)
    return out


def _extract_entity_ids(obj: Any) -> List[str]:
    return sorted(set(ENTITY_ID_RE.findall(json.dumps(obj, ensure_ascii=False))))


def _related_automations(entities: List[str], automations_file: Path) -> List[Dict[str, Any]]:
    if not entities:
        return []
    autos = _read_yaml(automations_file) or []
    rows: List[Dict[str, Any]] = []
    entity_set = set(entities)
    for item in autos:
        found = _extract_entity_ids(item)
        overlap = sorted(entity_set.intersection(found))
        if not overlap:
            continue
        rows.append(
            {
                "id": str(item.get("id", "")),
                "alias": str(item.get("alias", "")),
                "encje_wspolne": overlap[:20],
            }
        )
    return rows[:20]


def _related_scripts(entities: List[str], scripts_file: Path) -> List[Dict[str, Any]]:
    if not entities:
        return []
    scripts = _read_yaml(scripts_file) or {}
    rows: List[Dict[str, Any]] = []
    entity_set = set(entities)
    for key, body in scripts.items():
        found = _extract_entity_ids(body)
        overlap = sorted(entity_set.intersection(found))
        if not overlap:
            continue
        rows.append(
            {
                "key": key,
                "alias": str((body or {}).get("alias", "")),
                "encje_wspolne": overlap[:20],
            }
        )
    return rows[:20]


def _state_attrs(attrs: Dict[str, Any]) -> Dict[str, Any]:
    keep_keys = [
        "friendly_name",
        "source",
        "app_name",
        "media_title",
        "last_triggered",
        "current_position",
        "volume_level",
    ]
    out: Dict[str, Any] = {}
    for key in keep_keys:
        if key in attrs:
            out[key] = attrs.get(key)
    return out


def _history_for_entity(entity_id: str, from_time: str, to_time: str, base_url: str) -> List[Dict[str, Any]]:
    params = {
        "filter_entity_id": entity_id,
        "minimal_response": "0",
        "no_attributes": "0",
    }
    if to_time:
        params["end_time"] = to_time
    data = _api_get(f"/history/period/{from_time}", params=params, base_url=base_url)
    if not isinstance(data, list) or not data:
        return []
    rows = data[0] if isinstance(data[0], list) else []
    if not rows:
        return []
    normalized: List[Dict[str, Any]] = []
    previous_state: Optional[str] = None
    for idx, row in enumerate(rows):
        state = row.get("state")
        attributes = row.get("attributes") or {}
        normalized.append(
            {
                "entity_id": row.get("entity_id") or entity_id,
                "czas": row.get("last_changed") or row.get("last_updated"),
                "stan": state,
                "poprzedni_stan": previous_state if idx > 0 else None,
                "atrybuty": _state_attrs(attributes),
            }
        )
        previous_state = state
    return normalized


def _sort_key(row: Dict[str, Any]) -> dt.datetime:
    stamp = row.get("czas") or ""
    parsed = _parse_time(stamp)
    if parsed is None:
        return dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def _build_facts(events: List[Dict[str, Any]], entities: List[str]) -> List[str]:
    if not events:
        return ["Brak zmian stanów dla wskazanych encji w wybranym oknie czasu."]
    facts: List[str] = []
    per_entity: Dict[str, int] = {}
    for row in events:
        entity_id = str(row.get("entity_id", ""))
        per_entity[entity_id] = per_entity.get(entity_id, 0) + 1
    for entity_id in entities:
        count = per_entity.get(entity_id, 0)
        facts.append(f"{entity_id}: {count} zdarzeń w oknie czasu.")
    last = events[-1]
    facts.append(
        f"Ostatnie zdarzenie: {last.get('entity_id')} -> {last.get('stan')} o {last.get('czas')}."
    )
    return facts[:12]


def _build_conclusion(events: List[Dict[str, Any]], entities: List[str]) -> str:
    if not entities:
        return "Brak encji wejściowych, więc nie zbudowano przebiegu zdarzeń."
    if not events:
        return "Nie znaleziono zmian stanów dla wskazanych encji w wybranym oknie czasu."
    changed = sorted({str(row.get("entity_id", "")) for row in events})
    return (
        "Zebrano przebieg zdarzeń dla wskazanych encji. "
        f"Zmiany wystąpiły dla: {', '.join(changed)}."
    )


def run_event_timeline(
    entities: Iterable[str],
    from_time: str,
    to_time: str = "",
    limit: int = 120,
    automations_file: Path = DEFAULT_AUTOMATIONS_FILE,
    scripts_file: Path = DEFAULT_SCRIPTS_FILE,
    ha_base_url: str = DEFAULT_HA_BASE_URL,
) -> Dict[str, Any]:
    entity_ids = _normalize_entities(entities)
    if not entity_ids:
        return {
            "encje": [],
            "okno_czasu": {"od": from_time or None, "do": to_time or None},
            "zdarzenia": [],
            "powiazane_automatyzacje": [],
            "powiazane_skrypty": [],
            "uwagi": ["Narzędzie bazuje na historii HA. Zmiany samych atrybutów bez zmiany stanu mogą nie być widoczne."],
            "fakty": ["Nie podano żadnych encji."],
            "wniosek": "Brak encji wejściowych.",
        }

    if not from_time.strip():
        raise RuntimeError("--from-time is required for przebieg-zdarzen-ha")

    events: List[Dict[str, Any]] = []
    for entity_id in entity_ids:
        events.extend(_history_for_entity(entity_id, from_time, to_time, ha_base_url))

    events.sort(key=_sort_key)
    compact_events = events[:limit]

    return {
        "encje": entity_ids,
        "okno_czasu": {"od": from_time or None, "do": to_time or None},
        "zdarzenia": compact_events,
        "powiazane_automatyzacje": _related_automations(entity_ids, automations_file),
        "powiazane_skrypty": _related_scripts(entity_ids, scripts_file),
        "uwagi": ["Narzędzie bazuje na historii HA. Zmiany samych atrybutów bez zmiany stanu mogą nie być widoczne."],
        "fakty": _build_facts(compact_events, entity_ids),
        "wniosek": _build_conclusion(compact_events, entity_ids),
    }
