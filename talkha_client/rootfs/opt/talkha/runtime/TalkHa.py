#!/usr/bin/env python3
"""TalkHa - safe operational bridge for Home Assistant.

Design goals:
- one-time auth config (HA_URL + Long-Lived Token)
- read and write operations with transaction log
- backup before every write
- reversible operations where technically possible (undo)
- no Home Assistant restart required for normal work

Notes:
- API/WS calls depend on HA permissions for the token account.
- For generic mutating WS/Service calls, automatic undo may be unavailable.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass
from getpass import getpass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urlparse

import websockets

try:
    import yaml
except Exception:
    yaml = None


DEFAULT_BASE_DIR = Path("/homeassistant")
DEFAULT_CONFIG_PATH = Path("/data/.talkha.env")
DEFAULT_STATE_DIR = Path("/data/.talkha_state")
DEFAULT_BACKUP_ROOT = Path("/homeassistant/TalkHaBackup")
DEFAULT_LOG_FILE = Path("/data/talkha.log")
DEFAULT_SECRETS_PATH = Path("/homeassistant/secrets.yaml")


HELPER_STORAGE_FILES = {
    "input_boolean": ".storage/input_boolean",
    "input_number": ".storage/input_number",
    "input_text": ".storage/input_text",
    "input_select": ".storage/input_select",
}

HELPER_WS_ID_FIELDS = {
    "input_boolean": "input_boolean_id",
    "input_number": "input_number_id",
    "input_text": "input_text_id",
    "input_select": "input_select_id",
}

SYSTEM_LOG_LEVEL_ORDER = {
    "CRITICAL": 0,
    "ERROR": 1,
    "WARNING": 2,
    "INFO": 3,
    "DEBUG": 4,
}

SYSTEM_LOG_LEVEL_FIELDS = ("critical", "error", "warning", "info", "debug")
SYSTEM_LOG_DEFAULT_LEVELS = ("CRITICAL", "ERROR", "WARNING")


class TalkHaError(Exception):
    pass


@dataclass
class Credentials:
    ha_url: str
    ha_token: str


@dataclass
class WsCtx:
    ws: Any
    req_id: int = 1

    async def call(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        msg_id = self.req_id
        self.req_id += 1
        body = dict(payload)
        body["id"] = msg_id
        await self.ws.send(json.dumps(body))

        while True:
            raw = await self.ws.recv()
            data = json.loads(raw)
            if data.get("id") != msg_id:
                continue
            return data


class TxManager:
    def __init__(self, state_dir: Path, backup_root: Path, log_file: Path, base_dir: Path) -> None:
        self.state_dir = state_dir
        self.backup_root = backup_root
        self.log_file = log_file
        self.base_dir = base_dir

    def _now(self) -> str:
        return dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")

    def _append_log(self, entry: Dict[str, Any]) -> None:
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        with self.log_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def start(self, operation: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        tx_id = f"{self._now()}_{uuid.uuid4().hex[:8]}"
        tx_dir = self.state_dir / "transactions" / tx_id
        tx_dir.mkdir(parents=True, exist_ok=True)

        tx = {
            "tx_id": tx_id,
            "operation": operation,
            "started_at": dt.datetime.now().isoformat(),
            "payload": payload,
            "status": "started",
            "backup_files": [],
            "undo_actions": [],
            "notes": [],
        }
        self._write_report(tx_dir, tx)
        self._append_log({"level": "INFO", "event": "tx_start", **tx})
        return {"tx": tx, "tx_dir": tx_dir}

    def note(self, tx: Dict[str, Any], message: str) -> None:
        tx.setdefault("notes", []).append(message)

    def backup_files(self, tx_dir: Path, tx: Dict[str, Any], paths: Iterable[Path]) -> None:
        backup_dir = tx_dir / "backup"
        backup_dir.mkdir(parents=True, exist_ok=True)

        for p in paths:
            p = p.resolve()
            if not p.exists():
                continue
            rel = self._safe_relpath(p)
            target = backup_dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, target)
            tx["backup_files"].append({"source": str(p), "backup": str(target)})

    def add_undo_action(self, tx: Dict[str, Any], action: Dict[str, Any]) -> None:
        tx.setdefault("undo_actions", []).append(action)

    def finish(self, tx_dir: Path, tx: Dict[str, Any], status: str, message: str) -> None:
        tx["status"] = status
        tx["finished_at"] = dt.datetime.now().isoformat()
        tx["message"] = message
        self._write_report(tx_dir, tx)
        level = "INFO" if status == "ok" else "ERROR"
        self._append_log({"level": level, "event": "tx_finish", **tx})

    def _safe_relpath(self, p: Path) -> Path:
        try:
            return p.relative_to(self.base_dir)
        except Exception:
            return Path("external") / p.name

    @staticmethod
    def _write_report(tx_dir: Path, tx: Dict[str, Any]) -> None:
        with (tx_dir / "report.json").open("w", encoding="utf-8") as f:
            json.dump(tx, f, ensure_ascii=False, indent=2)
            f.write("\n")

    def load_tx(self, tx_id: str) -> Tuple[Path, Dict[str, Any]]:
        tx_dir = self.state_dir / "transactions" / tx_id
        report = tx_dir / "report.json"
        if not report.exists():
            raise TalkHaError(f"Transaction not found: {tx_id}")
        tx = json.loads(report.read_text(encoding="utf-8"))
        return tx_dir, tx


def read_key_value_file(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    out: Dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip("\"").strip("'")
    return out


def write_credentials_file(path: Path, ha_url: str, token: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "# TalkHa credentials\n" f"HA_URL={ha_url}\n" f"HA_TOKEN={token}\n"
    path.write_text(content, encoding="utf-8")
    os.chmod(path, 0o600)


def extract_token_from_text(raw: str) -> str:
    # Accept plain token, "Token: <value>", or multi-line content with label.
    text = raw.strip()
    if not text:
        return ""

    for line in raw.splitlines():
        s = line.strip()
        if not s:
            continue
        if s.lower().startswith("token:"):
            s = s.split(":", 1)[1].strip()
            if s:
                return s
            continue
        if s.lower() in {"token", "token:"}:
            continue
        if "." in s and len(s) > 40:
            return s
        return s
    return text


def resolve_credentials(config_file: Path) -> Credentials:
    cfg = read_key_value_file(config_file)
    ha_url = os.environ.get("HA_URL", "").strip() or cfg.get("HA_URL", "").strip()
    token = os.environ.get("HA_TOKEN", "").strip() or cfg.get("HA_TOKEN", "").strip()
    if not ha_url:
        raise TalkHaError(f"Missing HA_URL (env or {config_file})")
    if not token:
        raise TalkHaError(f"Missing HA_TOKEN (env or {config_file})")
    return Credentials(ha_url=ha_url, ha_token=token)


def ws_url_from_ha_url(ha_url: str) -> str:
    parsed = urlparse(ha_url)
    if not parsed.scheme or not parsed.netloc:
        raise TalkHaError(f"Invalid HA_URL: {ha_url}")
    scheme = "wss" if parsed.scheme == "https" else "ws"
    base_path = parsed.path.rstrip("/")
    return f"{scheme}://{parsed.netloc}{base_path}/api/websocket"


async def connect_and_auth(creds: Credentials) -> WsCtx:
    ws = await websockets.connect(ws_url_from_ha_url(creds.ha_url))

    first = json.loads(await ws.recv())
    if first.get("type") != "auth_required":
        await ws.close()
        raise TalkHaError(f"Unexpected first WS frame: {first}")

    await ws.send(json.dumps({"type": "auth", "access_token": creds.ha_token}))
    auth_resp = json.loads(await ws.recv())
    if auth_resp.get("type") != "auth_ok":
        await ws.close()
        raise TalkHaError(f"Authentication failed: {auth_resp}")

    return WsCtx(ws=ws)


async def ws_success(ctx: WsCtx, payload: Dict[str, Any]) -> Any:
    resp = await ctx.call(payload)
    if not resp.get("success", False):
        raise TalkHaError(f"WS call failed for {payload.get('type')}: {resp.get('error')}")
    return resp.get("result")


def parse_json_arg(value: str, arg_name: str) -> Dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise TalkHaError(f"Invalid JSON in {arg_name}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise TalkHaError(f"{arg_name} must be a JSON object")
    return parsed


def parse_csv_list(value: str) -> List[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def normalize_system_log_levels(value: str) -> List[str]:
    raw = parse_csv_list(value)
    levels = [part.upper() for part in raw] if raw else list(SYSTEM_LOG_DEFAULT_LEVELS)
    invalid = [level for level in levels if level not in SYSTEM_LOG_LEVEL_ORDER]
    if invalid:
        raise TalkHaError(f"Unsupported system log level(s): {', '.join(invalid)}")
    return levels


def format_system_log_timestamp(value: Any) -> str:
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc).astimezone().isoformat()
    return str(value or "")


def normalize_system_log_messages(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if value is None:
        return []
    text = str(value).strip()
    return [text] if text else []


def normalize_system_log_source(value: Any) -> str:
    if isinstance(value, list) and len(value) >= 2:
        return f"{value[0]}:{value[1]}"
    if isinstance(value, tuple) and len(value) >= 2:
        return f"{value[0]}:{value[1]}"
    return str(value or "")


def build_repairs_summary(issues: Any) -> List[Dict[str, Any]]:
    if not isinstance(issues, dict):
        return []
    raw_issues = issues.get("issues")
    if not isinstance(raw_issues, list):
        return []

    out: List[Dict[str, Any]] = []
    for raw in raw_issues:
        if not isinstance(raw, dict):
            continue
        if bool(raw.get("ignored", False)):
            continue

        item = {
            "domain": str(raw.get("domain", "")),
            "issue_id": str(raw.get("issue_id", "")),
            "severity": str(raw.get("severity", "")),
            "created": str(raw.get("created", "")),
            "is_fixable": bool(raw.get("is_fixable", False)),
            "learn_more_url": str(raw.get("learn_more_url", "")),
            "translation_key": str(raw.get("translation_key", "")),
        }
        placeholders = raw.get("translation_placeholders")
        if isinstance(placeholders, dict) and placeholders:
            item["translation_placeholders"] = placeholders
        out.append(item)

    severity_order = {"critical": 0, "error": 1, "warning": 2, "info": 3}
    out.sort(
        key=lambda item: (
            severity_order.get(str(item.get("severity", "")).lower(), 999),
            str(item.get("created", "")),
            str(item.get("domain", "")),
            str(item.get("issue_id", "")),
        )
    )
    return out


def build_system_log_summary(
    entries: Any,
    repairs: Any,
    levels: List[str],
    logger_filter: str,
    contains_filter: str,
    source_filter: str,
    limit: int,
    include_exception: bool,
) -> Dict[str, Any]:
    if not isinstance(entries, list):
        raise TalkHaError("Unexpected system_log/list response")

    logger_filter = logger_filter.strip().casefold()
    contains_filter = contains_filter.strip().casefold()
    source_filter = source_filter.strip().casefold()

    total_counts = {field: 0 for field in SYSTEM_LOG_LEVEL_FIELDS}
    groups: List[Dict[str, Any]] = []

    for raw in entries:
        if not isinstance(raw, dict):
            continue

        level = str(raw.get("level", "")).upper()
        if level not in SYSTEM_LOG_LEVEL_ORDER:
            continue

        total_counts[level.lower()] += 1
        if level not in levels:
            continue

        logger_name = str(raw.get("name", ""))
        source_text = normalize_system_log_source(raw.get("source"))
        messages = normalize_system_log_messages(raw.get("message"))

        haystack = "\n".join(messages).casefold()
        if logger_filter and logger_filter not in logger_name.casefold():
            continue
        if contains_filter and contains_filter not in haystack:
            continue
        if source_filter and source_filter not in source_text.casefold():
            continue

        group = {
            "poziom": level,
            "logger": logger_name,
            "count": int(raw.get("count", 0) or 0),
            "pierwsze_wystapienie": format_system_log_timestamp(raw.get("first_occurred")),
            "ostatnie_wystapienie": format_system_log_timestamp(raw.get("timestamp")),
            "source": source_text,
            "wiadomosci": messages,
            "sample": messages[-1] if messages else "",
        }
        if include_exception:
            group["exception"] = str(raw.get("exception", "") or "")
        groups.append(group)

    groups.sort(
        key=lambda item: (
            SYSTEM_LOG_LEVEL_ORDER.get(str(item.get("poziom", "")).upper(), 999),
            -dt.datetime.fromisoformat(item["ostatnie_wystapienie"]).timestamp() if item.get("ostatnie_wystapienie") else 0.0,
            -int(item.get("count", 0)),
            str(item.get("logger", "")),
        )
    )

    if limit < 1:
        raise TalkHaError("--limit must be >= 1")
    groups = groups[:limit]

    result = {
        "ok": True,
        "czas_pobrania": dt.datetime.now().astimezone().isoformat(),
        "zrodlo_danych": "system_log/list",
        "filtry": {
            "level": levels,
            "logger": logger_filter,
            "contains": contains_filter,
            "source_contains": source_filter,
            "limit": limit,
            "include_exception": include_exception,
        },
        "podsumowanie": {
            **total_counts,
            "wszystkie_grupy": sum(total_counts.values()),
            "po_filtrowaniu": len(groups),
        },
        "grupy": groups,
    }
    repairs_summary = build_repairs_summary(repairs)
    if repairs_summary:
        result["problemy_do_naprawy"] = repairs_summary
        result["podsumowanie"]["problemy_do_naprawy"] = len(repairs_summary)
    return result


def parse_local_datetime_input(value: str, arg_name: str, default_now: bool = False) -> dt.datetime:
    text = (value or "").strip()
    if not text:
        if default_now:
            return dt.datetime.now().astimezone()
        raise TalkHaError(f"Missing {arg_name}")

    try:
        parsed = dt.datetime.fromisoformat(text.replace(" ", "T"))
    except ValueError as exc:
        raise TalkHaError(f"Invalid {arg_name}: {value}") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.datetime.now().astimezone().tzinfo)
    return parsed.astimezone()


def parse_simple_yaml_secret(path: Path, key: str) -> str:
    if not path.exists():
        raise TalkHaError(f"Missing secrets file: {path}")
    pattern = re.compile(rf"^\s*{re.escape(key)}\s*:\s*(.+?)\s*$")
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = pattern.match(raw)
        if not match:
            continue
        value = match.group(1).strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        return value
    raise TalkHaError(f"Missing '{key}' in {path}")


def resolve_recorder_db_config(base_dir: Path) -> Dict[str, str]:
    db_url = parse_simple_yaml_secret(base_dir / "secrets.yaml", "mariadb_url")
    parsed = urlparse(db_url)
    if parsed.scheme not in {"mysql", "mariadb"}:
        raise TalkHaError(f"Unsupported recorder db_url scheme: {parsed.scheme}")
    if not parsed.hostname or not parsed.path.strip("/"):
        raise TalkHaError("Invalid mariadb_url in secrets.yaml")
    if parsed.username is None or parsed.password is None:
        raise TalkHaError("mariadb_url must include username and password")
    return {
        "host": parsed.hostname,
        "database": parsed.path.lstrip("/"),
        "user": unquote(parsed.username),
        "password": unquote(parsed.password),
    }


def run_recorder_query_via_ssh(base_dir: Path, sql: str, ha_host: str = "root@192.168.2.70") -> List[str]:
    cfg = resolve_recorder_db_config(base_dir)
    local_mode = os.environ.get("TALKHA_LOCAL_DB", "").lower() in {"1", "true", "yes"}
    if local_mode:
        proc = subprocess.run(
            [
                "mariadb",
                "--skip-ssl",
                "-Nse",
                sql,
                "-h",
                cfg["host"],
                "-u",
                cfg["user"],
                f"-p{cfg['password']}",
                "-D",
                cfg["database"],
            ],
            capture_output=True,
            text=True,
        )
    else:
        remote_cmd = " ".join(
            [
                "mariadb",
                "--skip-ssl",
                "-Nse",
                shlex.quote(sql),
                "-h",
                shlex.quote(cfg["host"]),
                "-u",
                shlex.quote(cfg["user"]),
                f"-p{shlex.quote(cfg['password'])}",
                "-D",
                shlex.quote(cfg["database"]),
            ]
        )
        proc = subprocess.run(["ssh", ha_host, remote_cmd], capture_output=True, text=True)
    if proc.returncode != 0:
        raise TalkHaError(f"MariaDB query failed: {proc.stderr.strip() or proc.stdout.strip()}")
    return [line for line in proc.stdout.splitlines() if line.strip()]


def normalize_message_kind(value: str) -> str:
    kind = (value or "all").strip().lower()
    if kind not in {"all", "tts", "telegram"}:
        raise TalkHaError("--kind must be one of: all, tts, telegram")
    return kind


def build_zigbee_status_report(states: Any) -> Dict[str, Any]:
    if not isinstance(states, list):
        raise TalkHaError("Unexpected get_states response")

    bridge_rows: Dict[str, Dict[str, Any]] = {}
    offline_entities: List[Dict[str, Any]] = []
    online_like_entities: List[Dict[str, Any]] = []

    def _bridge_key(entity_id: str) -> str:
        for prefix in (
            "binary_sensor.zigbee2mqtt_bridge_connection_state",
            "button.zigbee2mqtt_bridge_restart",
            "select.zigbee2mqtt_bridge_log_level",
            "sensor.zigbee2mqtt_bridge_version",
            "switch.zigbee2mqtt_bridge_permit_join",
        ):
            if entity_id.startswith(prefix):
                suffix = entity_id[len(prefix):]
                return suffix or "_1"
        return ""

    for row in states:
        if not isinstance(row, dict):
            continue
        entity_id = str(row.get("entity_id", ""))
        attrs = row.get("attributes") or {}
        friendly_name = str(attrs.get("friendly_name", ""))
        blob = f"{entity_id} {friendly_name}".lower()
        if "zigbee" not in blob:
            continue

        item = {
            "entity_id": entity_id,
            "state": row.get("state"),
            "friendly_name": friendly_name,
        }
        state_text = str(row.get("state"))
        if state_text in {"unavailable", "unknown"}:
            offline_entities.append(item)
        else:
            online_like_entities.append(item)

        bridge_key = _bridge_key(entity_id)
        if not bridge_key:
            continue
        bridge = bridge_rows.setdefault(
            bridge_key,
            {
                "bridge_key": bridge_key,
                "connection_state_entity": "",
                "connection_state": "",
                "version_entity": "",
                "version": "",
                "permit_join_entity": "",
                "permit_join": "",
                "restart_entity": "",
                "log_level_entity": "",
                "offline_entities": [],
            },
        )
        if entity_id.startswith("binary_sensor.zigbee2mqtt_bridge_connection_state"):
            bridge["connection_state_entity"] = entity_id
            bridge["connection_state"] = state_text
        elif entity_id.startswith("sensor.zigbee2mqtt_bridge_version"):
            bridge["version_entity"] = entity_id
            bridge["version"] = state_text
        elif entity_id.startswith("switch.zigbee2mqtt_bridge_permit_join"):
            bridge["permit_join_entity"] = entity_id
            bridge["permit_join"] = state_text
        elif entity_id.startswith("button.zigbee2mqtt_bridge_restart"):
            bridge["restart_entity"] = entity_id
        elif entity_id.startswith("select.zigbee2mqtt_bridge_log_level"):
            bridge["log_level_entity"] = entity_id

        if state_text in {"unavailable", "unknown"}:
            bridge["offline_entities"].append(item)

    bridges = list(bridge_rows.values())
    bridges.sort(key=lambda row: row.get("bridge_key", ""))
    for bridge in bridges:
        state = str(bridge.get("connection_state", ""))
        bridge["online"] = state == "on"
        if state == "off":
            bridge["status"] = "offline"
        elif state in {"unknown", "unavailable", ""}:
            bridge["status"] = "unknown"
        else:
            bridge["status"] = "online"

    return {
        "ok": True,
        "podsumowanie": {
            "mostki_wykryte": len(bridges),
            "mostki_online": sum(1 for row in bridges if row.get("status") == "online"),
            "mostki_offline": sum(1 for row in bridges if row.get("status") == "offline"),
            "encje_zigbee_offline": len(offline_entities),
        },
        "mostki": bridges,
        "encje_offline": offline_entities,
        "encje_online_like": online_like_entities[:80],
    }


def build_message_history(
    rows: List[str],
    from_time: dt.datetime,
    to_time: dt.datetime,
    kind: str,
    source_contains: str,
    contains: str,
    limit: int,
) -> Dict[str, Any]:
    if limit < 1:
        raise TalkHaError("--limit must be >= 1")

    source_contains_cf = source_contains.strip().casefold()
    contains_cf = contains.strip().casefold()

    grouped: Dict[str, Dict[str, Any]] = {}
    outputs: List[Dict[str, Any]] = []

    for row in rows:
        parts = row.split("\t", 3)
        if len(parts) != 4:
            continue
        context_id, ts_raw, event_type, payload_raw = parts
        try:
            ts_val = float(ts_raw)
        except ValueError:
            continue
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue

        ctx = grouped.setdefault(context_id, {})
        if event_type == "automation_triggered":
            ctx["source_name"] = str(payload.get("name", ""))
            ctx["source_entity_id"] = str(payload.get("entity_id", ""))
            ctx["source_trigger"] = str(payload.get("source", ""))
            continue

        if event_type != "call_service":
            continue

        domain = str(payload.get("domain", ""))
        service = str(payload.get("service", ""))
        service_data = payload.get("service_data")
        if not isinstance(service_data, dict):
            service_data = {}

        item: Optional[Dict[str, Any]] = None
        if domain == "tts" and service == "speak":
            media_player = service_data.get("media_player_entity_id")
            if isinstance(media_player, list):
                media_player = media_player[0] if media_player else ""
            item = {
                "czas": format_system_log_timestamp(ts_val),
                "typ": "tts",
                "tekst": str(service_data.get("message", "")).strip(),
                "glosnik": str(media_player or ""),
                "context_id": context_id,
            }
        elif domain == "script" and service == "informator":
            item = {
                "czas": format_system_log_timestamp(ts_val),
                "typ": "telegram",
                "tekst": str(service_data.get("text", "")).strip(),
                "context_id": context_id,
            }

        if item is None:
            continue

        item["zrodlo"] = str(ctx.get("source_name", ""))
        item["zrodlo_encja"] = str(ctx.get("source_entity_id", ""))
        item["trigger"] = str(ctx.get("source_trigger", ""))
        outputs.append(item)

    outputs.sort(key=lambda item: item.get("czas", ""))

    filtered: List[Dict[str, Any]] = []
    for item in outputs:
        if kind != "all" and item.get("typ") != kind:
            continue
        text = str(item.get("tekst", ""))
        source_text = " ".join(
            [
                str(item.get("zrodlo", "")),
                str(item.get("zrodlo_encja", "")),
                str(item.get("trigger", "")),
            ]
        ).strip()
        if contains_cf and contains_cf not in text.casefold():
            continue
        if source_contains_cf and source_contains_cf not in source_text.casefold():
            continue
        filtered.append(item)

    filtered = filtered[:limit]
    tts_count = sum(1 for item in filtered if item.get("typ") == "tts")
    telegram_count = sum(1 for item in filtered if item.get("typ") == "telegram")
    return {
        "ok": True,
        "czas_pobrania": dt.datetime.now().astimezone().isoformat(),
        "zrodlo_danych": "MariaDB Recorder",
        "okno_czasu": {
            "od": from_time.isoformat(),
            "do": to_time.isoformat(),
        },
        "filtry": {
            "kind": kind,
            "source_contains": source_contains,
            "contains": contains,
            "limit": limit,
        },
        "podsumowanie": {
            "wszystkie_komunikaty": len(filtered),
            "tts": tts_count,
            "telegram": telegram_count,
        },
        "komunikaty": filtered,
    }


def _split_marked_blocks(spec_text: str) -> List[Tuple[str, str]]:
    patt = re.compile(r"^(SKRYPT — PODMIEŃ CAŁY|AUTOMATYZACJA — PODMIEŃ CAŁĄ)\s*$", flags=re.M)
    marks = [(m.start(), m.end(), m.group(1)) for m in patt.finditer(spec_text)]
    if not marks:
        return []
    blocks: List[Tuple[str, str]] = []
    for i, (st, en, kind) in enumerate(marks):
        next_st = marks[i + 1][0] if i + 1 < len(marks) else len(spec_text)
        body = spec_text[en:next_st].strip()
        if body:
            blocks.append((kind, body + "\n"))
    return blocks


def load_compare_spec(spec_path: Path) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if yaml is None:
        raise TalkHaError("PyYAML required for compare operation")

    text = spec_path.read_text(encoding="utf-8")
    marked = _split_marked_blocks(text)
    if marked:
        scripts: List[Dict[str, Any]] = []
        automations: List[Dict[str, Any]] = []
        for kind, body in marked:
            item = yaml.safe_load(body)
            if not isinstance(item, dict):
                raise TalkHaError("Marked block spec must decode to YAML mapping")
            if kind.startswith("SKRYPT"):
                scripts.append(item)
            else:
                automations.append(item)
        return scripts, automations

    obj = yaml.safe_load(text)
    if not isinstance(obj, dict):
        raise TalkHaError("Spec file must be YAML mapping or marked blocks format")

    scripts_obj = obj.get("scripts", [])
    automations_obj = obj.get("automations", [])
    if not isinstance(scripts_obj, list) or not isinstance(automations_obj, list):
        raise TalkHaError("Spec keys 'scripts' and 'automations' must be lists")

    scripts = [x for x in scripts_obj if isinstance(x, dict)]
    automations = [x for x in automations_obj if isinstance(x, dict)]
    return scripts, automations


def build_script_indexes(scripts_data: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[Tuple[str, Dict[str, Any]]]]]:
    by_key: Dict[str, Dict[str, Any]] = {}
    by_alias: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
    for key, body in scripts_data.items():
        if not isinstance(body, dict):
            continue
        by_key[str(key)] = body
        alias = str(body.get("alias", ""))
        if alias:
            by_alias.setdefault(alias, []).append((str(key), body))
    return by_key, by_alias


def build_automation_indexes(autos_data: List[Any]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    by_alias: Dict[str, List[Dict[str, Any]]] = {}
    for item in autos_data:
        if not isinstance(item, dict):
            continue
        aid = str(item.get("id", ""))
        if aid:
            by_id[aid] = item
        alias = str(item.get("alias", ""))
        if alias:
            by_alias.setdefault(alias, []).append(item)
    return by_id, by_alias


COMPARE_KEY_ALIASES: Dict[str, str] = {
    "trigger": "triggers",
    "triggers": "trigger",
    "condition": "conditions",
    "conditions": "condition",
    "action": "actions",
    "actions": "action",
}


def _normalize_text(value: str, key: str, strict_text: bool) -> str:
    text = value.replace("\r\n", "\n").replace("\r", "\n")
    if strict_text:
        return text
    if key == "description":
        # GUI vs YAML block styles can differ while meaning stays the same.
        return " ".join(text.split())
    lines = [ln.rstrip() for ln in text.split("\n")]
    return "\n".join(lines).rstrip("\n")


def _normalize_for_compare(value: Any, key: str, strict_text: bool) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k in sorted(value.keys(), key=lambda x: str(x)):
            out[str(k)] = _normalize_for_compare(value[k], str(k), strict_text)
        return out
    if isinstance(value, list):
        return [_normalize_for_compare(v, key, strict_text) for v in value]
    if isinstance(value, str):
        return _normalize_text(value, key, strict_text)
    return value


def _lookup_with_alias(obj: Dict[str, Any], key: str) -> Tuple[bool, Any]:
    if key in obj:
        return True, obj[key]
    alias = COMPARE_KEY_ALIASES.get(key)
    if alias and alias in obj:
        return True, obj[alias]
    return False, None


def compare_subset(spec: Dict[str, Any], current: Dict[str, Any], strict_text: bool = False) -> List[str]:
    diffs: List[str] = []
    for k, v in spec.items():
        found, cur_val = _lookup_with_alias(current, k)
        if not found:
            diffs.append(f"missing_key:{k}")
            continue
        spec_n = _normalize_for_compare(v, k, strict_text)
        cur_n = _normalize_for_compare(cur_val, k, strict_text)
        if cur_n != spec_n:
            diffs.append(f"diff_key:{k}")
    return diffs


def match_script_spec(
    spec: Dict[str, Any],
    by_key: Dict[str, Dict[str, Any]],
    by_alias: Dict[str, List[Tuple[str, Dict[str, Any]]]],
) -> Tuple[str, Optional[Dict[str, Any]], Optional[str]]:
    key = str(spec.get("key", "")).strip()
    alias = str(spec.get("alias", "")).strip()

    if key:
        body = by_key.get(key)
        if body is None:
            return "MISSING", None, None
        return "FOUND", body, key

    if alias:
        hits = by_alias.get(alias, [])
        if not hits:
            return "MISSING", None, None
        if len(hits) > 1:
            return "AMBIGUOUS", None, None
        return "FOUND", hits[0][1], hits[0][0]

    return "INVALID_SPEC", None, None


def match_automation_spec(
    spec: Dict[str, Any],
    by_id: Dict[str, Dict[str, Any]],
    by_alias: Dict[str, List[Dict[str, Any]]],
) -> Tuple[str, Optional[Dict[str, Any]], Optional[str]]:
    aid = str(spec.get("id", "")).strip()
    alias = str(spec.get("alias", "")).strip()

    if aid:
        item = by_id.get(aid)
        if item is None:
            return "MISSING", None, None
        return "FOUND", item, aid

    if alias:
        hits = by_alias.get(alias, [])
        if not hits:
            return "MISSING", None, None
        if len(hits) > 1:
            return "AMBIGUOUS", None, None
        one = hits[0]
        return "FOUND", one, str(one.get("id", ""))

    return "INVALID_SPEC", None, None


def load_json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise TalkHaError(f"Invalid JSON file {path}: {exc}") from exc


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def require_explicit(confirm: str, op: str) -> None:
    if confirm != "REQUIRED":
        raise TalkHaError(f"{op} requires --explicit-confirm REQUIRED")


def forbid_local_write(op: str) -> None:
    raise TalkHaError(
        f"{op} is disabled in API-only mode. "
        "Use WebSocket/API operations (ws-call/service-call/config_* registry calls)."
    )


def normalize_categories_result(result: Any, scope: str) -> List[Dict[str, Any]]:
    if isinstance(result, dict):
        if "categories" in result and isinstance(result["categories"], dict):
            scoped = result["categories"].get(scope, [])
            return [c for c in scoped if isinstance(c, dict)]
        if scope in result and isinstance(result[scope], list):
            return [c for c in result[scope] if isinstance(c, dict)]
    if isinstance(result, list):
        out = []
        for item in result:
            if not isinstance(item, dict):
                continue
            item_scope = str(item.get("scope") or item.get("category_type") or "").strip().lower()
            if item_scope and item_scope != scope:
                continue
            out.append(item)
        return out
    return []


async def ws_list_categories(ctx: WsCtx, scope: str) -> List[Dict[str, Any]]:
    attempts = [
        {"type": "config/category_registry/list"},
        {"type": "config/category_registry/list", "scope": scope},
        {"type": "config/category_registry/list", "category_type": scope},
    ]
    last_err: Optional[Exception] = None
    for payload in attempts:
        try:
            result = await ws_success(ctx, payload)
            return normalize_categories_result(result, scope)
        except Exception as exc:
            last_err = exc
    raise TalkHaError(f"Cannot fetch categories: {last_err}")


async def ws_entity_registry_list(ctx: WsCtx) -> List[Dict[str, Any]]:
    result = await ws_success(ctx, {"type": "config/entity_registry/list"})
    if not isinstance(result, list):
        raise TalkHaError("Unexpected entity registry response")
    return [x for x in result if isinstance(x, dict)]


async def ws_update_entity_category(ctx: WsCtx, entity_id: str, scope: str, category_id: Optional[str]) -> None:
    if category_id:
        attempts = [
            {"type": "config/entity_registry/update", "entity_id": entity_id, "categories": {scope: category_id}},
            {
                "type": "config/entity_registry/update",
                "entity_id": entity_id,
                "category_id": category_id,
                "scope": scope,
            },
            {
                "type": "config/entity_registry/update",
                "entity_id": entity_id,
                "category_id": category_id,
                "category_type": scope,
            },
        ]
    else:
        attempts = [
            {"type": "config/entity_registry/update", "entity_id": entity_id, "categories": {}},
            {
                "type": "config/entity_registry/update",
                "entity_id": entity_id,
                "category_id": None,
                "scope": scope,
            },
            {
                "type": "config/entity_registry/update",
                "entity_id": entity_id,
                "category_id": None,
                "category_type": scope,
            },
        ]

    last_err: Optional[Exception] = None
    for payload in attempts:
        try:
            await ws_success(ctx, payload)
            return
        except Exception as exc:
            last_err = exc
    raise TalkHaError(f"Unable to update category for {entity_id}: {last_err}")


def pick_automation_entity(entities: List[Dict[str, Any]], automation_id: str, entity_id: str) -> Dict[str, Any]:
    matches = []
    for entry in entities:
        if entry.get("platform") != "automation":
            continue
        if entity_id and entry.get("entity_id") == entity_id:
            matches.append(entry)
            continue
        if automation_id:
            caps = entry.get("capabilities") if isinstance(entry.get("capabilities"), dict) else {}
            cap_id = str(caps.get("id", ""))
            uid = str(entry.get("unique_id", ""))
            if uid == automation_id or cap_id == automation_id:
                matches.append(entry)

    if not matches:
        raise TalkHaError("Automation not found in entity registry")
    if len(matches) > 1:
        ids = ", ".join(m.get("entity_id", "?") for m in matches)
        raise TalkHaError(f"Ambiguous automation match: {ids}")
    return matches[0]


def helper_file_path(base_dir: Path, kind: str) -> Path:
    if kind not in HELPER_STORAGE_FILES:
        raise TalkHaError(f"Unsupported helper kind: {kind}")
    return base_dir / HELPER_STORAGE_FILES[kind]


def helper_ws_id_field(kind: str) -> str:
    try:
        return HELPER_WS_ID_FIELDS[kind]
    except KeyError as exc:
        raise TalkHaError(f"Unsupported helper kind: {kind}") from exc


def normalize_helper_id(kind: str, helper_id_or_entity: str) -> str:
    raw = helper_id_or_entity.strip()
    if not raw:
        raise TalkHaError("Helper id/entity cannot be empty")
    prefix = f"{kind}."
    if raw.startswith(prefix):
        return raw[len(prefix) :]
    if "." in raw:
        domain, obj = raw.split(".", 1)
        if domain != kind:
            raise TalkHaError(f"Entity domain mismatch. Expected {kind}, got {domain}")
        return obj
    return raw


def load_helper_storage(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise TalkHaError(f"Missing helper storage file: {path}")
    data = load_json_file(path)
    if not isinstance(data, dict):
        raise TalkHaError("Invalid helper storage structure")
    items = data.get("data", {}).get("items")
    if not isinstance(items, list):
        raise TalkHaError("Invalid helper storage data.items")
    return data


def validate_helper_item(kind: str, item: Dict[str, Any]) -> None:
    if "id" not in item or not str(item["id"]).strip():
        raise TalkHaError("Helper item requires non-empty 'id'")

    if kind in ("input_boolean",):
        if "name" not in item:
            raise TalkHaError("input_boolean requires 'name'")
    elif kind == "input_number":
        for req in ("name", "min", "max", "step"):
            if req not in item:
                raise TalkHaError(f"input_number requires '{req}'")
    elif kind == "input_text":
        for req in ("name", "min", "max", "mode"):
            if req not in item:
                raise TalkHaError(f"input_text requires '{req}'")
    elif kind == "input_select":
        if "name" not in item or "options" not in item:
            raise TalkHaError("input_select requires 'name' and 'options'")
        if not isinstance(item["options"], list) or not item["options"]:
            raise TalkHaError("input_select options must be non-empty list")


def helper_entity_id(kind: str, helper_id: str) -> str:
    return f"{kind}.{helper_id}"


def helper_payload_without_id(item: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in item.items() if k != "id"}


def helper_backend_id_from_entry(kind: str, entry: Dict[str, Any]) -> str:
    unique_id = str(entry.get("unique_id", "")).strip()
    if unique_id:
        return unique_id
    entity_id = str(entry.get("entity_id", "")).strip()
    prefix = f"{kind}."
    if entity_id.startswith(prefix):
        return entity_id[len(prefix) :]
    raise TalkHaError(f"Cannot determine backend helper id for {entity_id or kind}")


async def ws_list_helpers(ctx: WsCtx, kind: str) -> List[Dict[str, Any]]:
    entities = await ws_entity_registry_list(ctx)
    states = await ws_success(ctx, {"type": "get_states"})
    if not isinstance(states, list):
        raise TalkHaError("Unexpected get_states response")
    state_by_entity = {}
    for row in states:
        if not isinstance(row, dict):
            continue
        entity_id = str(row.get("entity_id", "")).strip()
        if entity_id.startswith(f"{kind}."):
            state_by_entity[entity_id] = row

    out_by_entity: Dict[str, Dict[str, Any]] = {}
    for entry in entities:
        if entry.get("platform") != kind:
            continue
        entity_id = str(entry.get("entity_id", "")).strip()
        if not entity_id.startswith(f"{kind}."):
            continue
        st = state_by_entity.get(entity_id, {})
        attrs = st.get("attributes", {}) if isinstance(st.get("attributes"), dict) else {}
        out_by_entity[entity_id] = {
            "entity_id": entity_id,
            "helper_id": helper_backend_id_from_entry(kind, entry),
            "name": attrs.get("friendly_name") or entry.get("original_name") or entry.get("name") or entity_id,
            "icon": attrs.get("icon") or entry.get("original_icon"),
            "state": st.get("state"),
            "editable": not bool(entry.get("config_entry_id")),
        }

    for entity_id, st in state_by_entity.items():
        if entity_id in out_by_entity:
            continue
        attrs = st.get("attributes", {}) if isinstance(st.get("attributes"), dict) else {}
        out_by_entity[entity_id] = {
            "entity_id": entity_id,
            "helper_id": entity_id.split(".", 1)[1],
            "name": attrs.get("friendly_name") or entity_id,
            "icon": attrs.get("icon"),
            "state": st.get("state"),
            "editable": True,
        }

    out = sorted(out_by_entity.values(), key=lambda row: str(row.get("entity_id", "")))
    return out


async def ws_find_helper_entry(ctx: WsCtx, kind: str, helper_id_or_entity: str) -> Optional[Dict[str, Any]]:
    helper_id = normalize_helper_id(kind, helper_id_or_entity)
    wanted_entity_id = helper_entity_id(kind, helper_id)
    entities = await ws_entity_registry_list(ctx)

    exact_entity: Optional[Dict[str, Any]] = None
    exact_backend: Optional[Dict[str, Any]] = None
    for entry in entities:
        if entry.get("platform") != kind:
            continue
        entity_id = str(entry.get("entity_id", "")).strip()
        unique_id = str(entry.get("unique_id", "")).strip()
        if entity_id == wanted_entity_id:
            exact_entity = entry
            break
        if unique_id == helper_id:
            exact_backend = entry
    if exact_entity or exact_backend:
        return exact_entity or exact_backend

    states = await ws_success(ctx, {"type": "get_states"})
    if not isinstance(states, list):
        raise TalkHaError("Unexpected get_states response")
    for row in states:
        if isinstance(row, dict) and str(row.get("entity_id", "")).strip() == wanted_entity_id:
            return {
                "entity_id": wanted_entity_id,
                "unique_id": helper_id,
                "platform": kind,
            }
    return None


def write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    atomic_write_text(path, text)


def find_automation_block(lines: List[str], target_id: str) -> Tuple[int, int, Dict[str, Any]]:
    if yaml is None:
        raise TalkHaError("PyYAML required for YAML operations")
    starts = [i for i, ln in enumerate(lines) if ln.startswith("- ")]
    matches: List[Tuple[int, int, Dict[str, Any]]] = []

    for idx, st in enumerate(starts):
        en = starts[idx + 1] if idx + 1 < len(starts) else len(lines)
        chunk = "".join(lines[st:en])
        parsed = yaml.safe_load(chunk)
        if isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
            data = parsed[0]
        elif isinstance(parsed, dict):
            data = parsed
        else:
            continue
        if str(data.get("id", "")) == str(target_id):
            matches.append((st, en, data))

    if not matches:
        raise TalkHaError(f"Automation id not found: {target_id}")
    if len(matches) > 1:
        raise TalkHaError(f"Automation id ambiguous: {target_id}")
    return matches[0]


SCRIPT_KEY_RE = re.compile(r"^([A-Za-z0-9_]+):\s*$")
CATEGORY_RE = re.compile(r"^\s*#\s*(.*?)\s*$")


def find_script_block(lines: List[str], target_key: str) -> Tuple[int, int]:
    starts: List[Tuple[str, int]] = []
    for i, line in enumerate(lines):
        m = SCRIPT_KEY_RE.match(line)
        if m:
            starts.append((m.group(1), i))

    matches = [(k, i) for (k, i) in starts if k == target_key]
    if not matches:
        raise TalkHaError(f"Script key not found: {target_key}")
    if len(matches) > 1:
        raise TalkHaError(f"Script key ambiguous: {target_key}")

    start_i = matches[0][1]
    next_positions = [i for (_k, i) in starts if i > start_i]
    end_i = min(next_positions) if next_positions else len(lines)
    return start_i, end_i


def parse_new_automation_block(path: Path, required_id: str) -> List[str]:
    if yaml is None:
        raise TalkHaError("PyYAML required for YAML operations")
    raw = path.read_text(encoding="utf-8")
    parsed = yaml.safe_load(raw)
    if isinstance(parsed, dict):
        obj = parsed
    elif isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
        obj = parsed[0]
    else:
        raise TalkHaError("New automation block must be mapping or one-item list")

    if str(obj.get("id", "")) != str(required_id):
        raise TalkHaError("New automation block id must match --target-id")

    if "alias" not in obj:
        raise TalkHaError("New automation block requires alias")

    out = yaml.safe_dump([obj], allow_unicode=True, sort_keys=False)
    return out.splitlines(keepends=True)


def parse_new_automation_block_for_add(path: Path) -> Tuple[List[str], Dict[str, Any]]:
    if yaml is None:
        raise TalkHaError("PyYAML required for YAML operations")
    raw = path.read_text(encoding="utf-8")
    parsed = yaml.safe_load(raw)
    if isinstance(parsed, dict):
        obj = parsed
    elif isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
        obj = parsed[0]
    else:
        raise TalkHaError("New automation block must be mapping or one-item list")
    if "id" not in obj or not str(obj.get("id", "")).strip():
        raise TalkHaError("New automation block requires non-empty id")
    if "alias" not in obj or not str(obj.get("alias", "")).strip():
        raise TalkHaError("New automation block requires non-empty alias")
    out = yaml.safe_dump([obj], allow_unicode=True, sort_keys=False)
    return out.splitlines(keepends=True), obj


def normalize_category_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.strip().lower())


def extract_category_name(line: str) -> Optional[str]:
    m = CATEGORY_RE.match(line)
    if not m:
        return None
    body = m.group(1).strip()
    if not body:
        return None
    body = re.sub(r"^=+", "", body).strip()
    body = re.sub(r"=+$", "", body).strip()
    body = re.sub(r"^-+", "", body).strip()
    body = re.sub(r"-+$", "", body).strip()
    return body or None


def find_category_sections(lines: List[str]) -> List[Tuple[str, str, int, int]]:
    marks: List[Tuple[int, str, str]] = []
    for i, line in enumerate(lines):
        name = extract_category_name(line)
        if not name:
            continue
        marks.append((i, name, normalize_category_name(name)))
    out: List[Tuple[str, str, int, int]] = []
    for idx, (st, raw, norm) in enumerate(marks):
        en = marks[idx + 1][0] if idx + 1 < len(marks) else len(lines)
        out.append((raw, norm, st, en))
    return out


def insert_automation_under_category(
    old_lines: List[str],
    category: str,
    block_lines: List[str],
    insert_mode: str,
) -> List[str]:
    norm = normalize_category_name(category)
    sections = find_category_sections(old_lines)
    matches = [s for s in sections if s[1] == norm]
    new_lines = list(old_lines)

    if matches:
        if len(matches) > 1:
            raise TalkHaError(f"Category ambiguous: {category}")
        _raw, _norm, _st, en = matches[0]
        insert_at = en
        payload = []
        if insert_at > 0 and (insert_at == len(new_lines) or new_lines[insert_at - 1].strip() != ""):
            payload.append("\n")
        payload.extend(block_lines)
        new_lines[insert_at:insert_at] = payload
        return new_lines

    if insert_mode != "create_category_then_insert":
        raise TalkHaError(f"Category not found: {category}")

    if new_lines and new_lines[-1].strip() != "":
        new_lines.append("\n")
    new_lines.append(f"# ===== {category.upper()} =====\n")
    new_lines.extend(block_lines)
    return new_lines


def parse_new_script_block(path: Path, required_key: str) -> List[str]:
    if yaml is None:
        raise TalkHaError("PyYAML required for YAML operations")
    raw = path.read_text(encoding="utf-8")
    parsed = yaml.safe_load(raw)
    if not isinstance(parsed, dict) or len(parsed) != 1:
        raise TalkHaError("New script block must be one top-level mapping")
    key = next(iter(parsed.keys()))
    if key != required_key:
        raise TalkHaError("New script top-level key must match --target-key")
    body = parsed[key]
    if not isinstance(body, dict):
        raise TalkHaError("Script body must be object")
    if "alias" not in body or "sequence" not in body:
        raise TalkHaError("Script body requires alias and sequence")
    out = yaml.safe_dump(parsed, allow_unicode=True, sort_keys=False)
    return out.splitlines(keepends=True)


def default_backup_paths(base_dir: Path) -> List[Path]:
    candidates = [
        base_dir / "automations.yaml",
        base_dir / "scripts.yaml",
        base_dir / ".storage" / "core.entity_registry",
        base_dir / ".storage" / "core.category_registry",
        base_dir / ".storage" / "input_boolean",
        base_dir / ".storage" / "input_number",
        base_dir / ".storage" / "input_text",
        base_dir / ".storage" / "input_select",
        base_dir / ".storage" / "lovelace_dashboards",
    ]
    return [p for p in candidates if p.exists()]


def create_snapshot(backup_root: Path, base_dir: Path, name: str, extra_paths: List[str]) -> Path:
    stamp = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    snap_dir = backup_root / f"{stamp}_{name}"
    snap_dir.mkdir(parents=True, exist_ok=True)

    paths: List[Path] = default_backup_paths(base_dir)
    for raw in extra_paths:
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = (base_dir / p).resolve()
        if p.exists():
            paths.append(p)

    unique = []
    seen = set()
    for p in paths:
        sp = str(p)
        if sp in seen:
            continue
        seen.add(sp)
        unique.append(p)

    for p in unique:
        try:
            rel = p.relative_to(base_dir)
        except Exception:
            rel = Path("external") / p.name
        target = snap_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, target)

    return snap_dir


async def run_async(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).expanduser().resolve()
    config_file = Path(args.config_file).expanduser().resolve()
    state_dir = Path(args.state_dir).expanduser().resolve()
    backup_root = Path(args.backup_root).expanduser().resolve()
    log_file = Path(args.log_file).expanduser().resolve()

    txm = TxManager(state_dir=state_dir, backup_root=backup_root, log_file=log_file, base_dir=base_dir)

    if args.cmd == "init-auth":
        ha_url = (args.ha_url or "").strip()
        if not ha_url:
            ha_url = input("HA_URL: ").strip()
        _ = ws_url_from_ha_url(ha_url)

        token = (args.token or "").strip()
        if not token and args.token_file:
            raw = Path(args.token_file).expanduser().read_text(encoding="utf-8")
            token = extract_token_from_text(raw)
        if not token:
            token = getpass("HA_TOKEN (hidden): ").strip()

        if not token:
            raise TalkHaError("HA_TOKEN is required")
        write_credentials_file(config_file, ha_url, token)
        print(f"OK: credentials saved to {config_file}")
        return 0

    if args.cmd == "snapshot":
        snap_dir = create_snapshot(backup_root, base_dir, args.name, args.include or [])
        print(f"OK: snapshot {snap_dir}")
        return 0

    if args.cmd == "tx-report":
        _tx_dir, tx = txm.load_tx(args.tx_id)
        print(json.dumps(tx, ensure_ascii=False, indent=2))
        return 0

    # Commands below may need credentials.
    creds: Optional[Credentials] = None
    ws_ctx: Optional[WsCtx] = None

    async def ensure_ws() -> WsCtx:
        nonlocal creds, ws_ctx
        if ws_ctx is not None:
            return ws_ctx
        if creds is None:
            creds = resolve_credentials(config_file)
        ws_ctx = await connect_and_auth(creds)
        return ws_ctx

    try:
        if args.cmd == "test-auth":
            ctx = await ensure_ws()
            _ = ctx
            print("OK: auth")
            return 0

        if args.cmd == "automation-alias-audit":
            ctx = await ensure_ws()
            states = await ws_success(ctx, {"type": "get_states"})
            if not isinstance(states, list):
                raise TalkHaError("Unexpected get_states response")

            needle = args.contains.strip().lower()
            out = []
            for st in states:
                if not isinstance(st, dict):
                    continue
                ent = str(st.get("entity_id", ""))
                if not ent.startswith("automation."):
                    continue
                attrs = st.get("attributes", {}) if isinstance(st.get("attributes"), dict) else {}
                friendly = str(attrs.get("friendly_name", ""))
                if needle and needle not in friendly.lower():
                    continue
                out.append(
                    {
                        "entity_id": ent,
                        "state": st.get("state"),
                        "id": attrs.get("id"),
                        "friendly_name": friendly,
                        "last_triggered": attrs.get("last_triggered"),
                        "mode": attrs.get("mode"),
                    }
                )

            out.sort(key=lambda x: x.get("friendly_name", ""))
            if args.as_json:
                print(json.dumps(out, ensure_ascii=False, indent=2))
            else:
                for row in out:
                    print(
                        f"{row['entity_id']} | {row['state']} | {row['friendly_name']} | "
                        f"id={row['id']} | last={row['last_triggered']}"
                    )
            return 0

        if args.cmd == "ws-call":
            payload = parse_json_arg(args.payload_json, "--payload-json")
            if "type" in payload:
                raise TalkHaError("Do not include 'type' in --payload-json; use --type")
            payload["type"] = args.type

            mutating = args.mutating
            tx_info = None
            if mutating:
                require_explicit(args.explicit_confirm, "mutating ws-call")
                tx_info = txm.start("ws-call", {"payload": payload, "mutating": True})
                txm.backup_files(tx_info["tx_dir"], tx_info["tx"], default_backup_paths(base_dir))
                txm.note(tx_info["tx"], "Generic mutating WS call may not be auto-undoable")

            ctx = await ensure_ws()
            result = await ws_success(ctx, payload)
            print(json.dumps(result, ensure_ascii=False, indent=2))

            if tx_info:
                txm.finish(tx_info["tx_dir"], tx_info["tx"], "ok", "WS mutating call executed")
            return 0

        if args.cmd == "service-call":
            require_explicit(args.explicit_confirm, "service-call")
            target = parse_json_arg(args.target_json, "--target-json")
            data = parse_json_arg(args.data_json, "--data-json")

            tx_info = txm.start(
                "service-call",
                {"domain": args.domain, "service": args.service, "target": target, "data": data},
            )
            txm.backup_files(tx_info["tx_dir"], tx_info["tx"], default_backup_paths(base_dir))
            txm.note(tx_info["tx"], "Service calls may not be auto-undoable")

            ctx = await ensure_ws()
            result = await ws_success(
                ctx,
                {
                    "type": "call_service",
                    "domain": args.domain,
                    "service": args.service,
                    "target": target,
                    "service_data": data,
                },
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            txm.finish(tx_info["tx_dir"], tx_info["tx"], "ok", "Service call executed")
            return 0

        if args.cmd == "list-categories":
            ctx = await ensure_ws()
            cats = await ws_list_categories(ctx, args.scope)
            for c in cats:
                print(f"{c.get('name','')}\t{c.get('category_id','')}")
            return 0

        if args.cmd == "set-automation-category":
            tx_info = txm.start(
                "set-automation-category",
                {
                    "automation_id": args.automation_id,
                    "entity_id": args.entity_id,
                    "category": args.category,
                    "scope": args.scope,
                },
            )
            txm.backup_files(tx_info["tx_dir"], tx_info["tx"], default_backup_paths(base_dir))

            ctx = await ensure_ws()
            categories = await ws_list_categories(ctx, args.scope)
            cat = next((x for x in categories if str(x.get("name", "")).strip().casefold() == args.category.strip().casefold()), None)
            if not cat:
                raise TalkHaError(f"Category not found: {args.category}")
            category_id = str(cat.get("category_id", ""))
            if not category_id:
                raise TalkHaError("Invalid category id")

            entities = await ws_entity_registry_list(ctx)
            target = pick_automation_entity(entities, args.automation_id or "", args.entity_id or "")
            entity_id = str(target.get("entity_id", ""))
            if not entity_id:
                raise TalkHaError("Target automation missing entity_id")

            prev_categories = target.get("categories") if isinstance(target.get("categories"), dict) else {}
            prev_cat_id = prev_categories.get(args.scope)

            txm.add_undo_action(
                tx_info["tx"],
                {
                    "type": "ws_restore_automation_category",
                    "entity_id": entity_id,
                    "scope": args.scope,
                    "category_id": prev_cat_id,
                },
            )

            await ws_update_entity_category(ctx, entity_id, args.scope, category_id)
            txm.finish(tx_info["tx_dir"], tx_info["tx"], "ok", f"{entity_id} -> {args.category}")
            print(f"OK: {entity_id} -> {args.category} ({category_id})")
            return 0

        if args.cmd == "helper-list":
            ctx = await ensure_ws()
            items = await ws_list_helpers(ctx, args.kind)
            if args.as_json:
                print(json.dumps(items, ensure_ascii=False, indent=2))
            else:
                for item in items:
                    print(
                        f"{item['entity_id']}\t{item.get('name','')}\t"
                        f"helper_id={item.get('helper_id','')}"
                    )
            return 0

        if args.cmd == "get-entity":
            ctx = await ensure_ws()
            states = await ws_success(ctx, {"type": "get_states"})
            if not isinstance(states, list):
                raise TalkHaError("Unexpected get_states response")
            target = next((row for row in states if isinstance(row, dict) and str(row.get("entity_id", "")) == args.entity_id), None)
            if target is None:
                raise TalkHaError(f"Entity not found: {args.entity_id}")
            print(json.dumps(target, ensure_ascii=False, indent=2))
            return 0

        if args.cmd == "zigbee-status-report":
            ctx = await ensure_ws()
            states = await ws_success(ctx, {"type": "get_states"})
            report = build_zigbee_status_report(states)
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 0

        if args.cmd == "helper-upsert":
            require_explicit(args.explicit_confirm, "helper-upsert")
            item = parse_json_arg(args.item_json, "--item-json")
            helper_id = normalize_helper_id(args.kind, args.helper)
            item["id"] = helper_id
            validate_helper_item(args.kind, item)

            tx_info = txm.start(
                "helper-upsert",
                {"kind": args.kind, "helper": helper_id, "item": item},
            )
            txm.backup_files(tx_info["tx_dir"], tx_info["tx"], default_backup_paths(base_dir))
            txm.note(tx_info["tx"], "GUI helper mutation over WebSocket/API")

            ctx = await ensure_ws()
            existing = await ws_find_helper_entry(ctx, args.kind, helper_id)
            payload = helper_payload_without_id(item)
            if existing is not None:
                backend_id = helper_backend_id_from_entry(args.kind, existing)
                payload[helper_ws_id_field(args.kind)] = backend_id
                result = await ws_success(ctx, {"type": f"{args.kind}/update", **payload})
                entity_id = str(existing.get("entity_id", helper_entity_id(args.kind, helper_id)))
                action = "updated"
            else:
                result = await ws_success(ctx, {"type": f"{args.kind}/create", **payload})
                created_backend_id = str(result.get("id", "")).strip()
                if not created_backend_id:
                    raise TalkHaError(f"{args.kind}/create did not return helper id")
                created_entity_id = helper_entity_id(args.kind, created_backend_id)
                entity_id = created_entity_id
                if created_backend_id != helper_id:
                    await ws_success(
                        ctx,
                        {
                            "type": "config/entity_registry/update",
                            "entity_id": created_entity_id,
                            "new_entity_id": helper_entity_id(args.kind, helper_id),
                        },
                    )
                    entity_id = helper_entity_id(args.kind, helper_id)
                    txm.note(
                        tx_info["tx"],
                        f"Created backend id {created_backend_id} and renamed entity to {entity_id}",
                    )
                action = "created"

            confirmed = await ws_find_helper_entry(ctx, args.kind, helper_id)
            if confirmed is None:
                raise TalkHaError(f"Helper not visible after {action}: {helper_entity_id(args.kind, helper_id)}")

            print(
                json.dumps(
                    {
                        "kind": args.kind,
                        "helper": helper_id,
                        "entity_id": str(confirmed.get("entity_id", entity_id)),
                        "backend_id": helper_backend_id_from_entry(args.kind, confirmed),
                        "action": action,
                        "result": result,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            txm.finish(tx_info["tx_dir"], tx_info["tx"], "ok", f"Helper {action} via GUI/API")
            return 0

        if args.cmd == "helper-delete":
            require_explicit(args.explicit_confirm, "helper-delete")
            helper_id = normalize_helper_id(args.kind, args.helper)
            tx_info = txm.start(
                "helper-delete",
                {"kind": args.kind, "helper": helper_id},
            )
            txm.backup_files(tx_info["tx_dir"], tx_info["tx"], default_backup_paths(base_dir))
            txm.note(tx_info["tx"], "GUI helper deletion over WebSocket/API")

            ctx = await ensure_ws()
            existing = await ws_find_helper_entry(ctx, args.kind, helper_id)
            if existing is None:
                raise TalkHaError(f"Helper not found: {helper_entity_id(args.kind, helper_id)}")
            backend_id = helper_backend_id_from_entry(args.kind, existing)
            entity_id = str(existing.get("entity_id", helper_entity_id(args.kind, helper_id)))

            result = await ws_success(
                ctx,
                {"type": f"{args.kind}/delete", helper_ws_id_field(args.kind): backend_id},
            )

            confirmed = await ws_find_helper_entry(ctx, args.kind, helper_id)
            if confirmed is not None:
                raise TalkHaError(f"Helper still present after delete: {entity_id}")

            print(
                json.dumps(
                    {
                        "kind": args.kind,
                        "helper": helper_id,
                        "entity_id": entity_id,
                        "backend_id": backend_id,
                        "action": "deleted",
                        "result": result,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            txm.finish(tx_info["tx_dir"], tx_info["tx"], "ok", "Helper deleted via GUI/API")
            return 0

        if args.cmd == "replace-automation-block":
            forbid_local_write("replace-automation-block")

        if args.cmd == "add-automation-block":
            forbid_local_write("add-automation-block")

        if args.cmd == "replace-script-block":
            forbid_local_write("replace-script-block")

        if args.cmd == "panel-export":
            panel_file = base_dir / ".storage" / f"lovelace.{args.dashboard_id}"
            if not panel_file.exists():
                panel_file = base_dir / ".storage" / f"lovelace.dashboard_{args.dashboard_id}"
            if not panel_file.exists():
                raise TalkHaError(f"Dashboard storage file not found for id: {args.dashboard_id}")

            out = Path(args.output).expanduser().resolve() if args.output else (base_dir / "tools" / f"panel_{args.dashboard_id}.json")
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(panel_file, out)
            print(f"OK: exported to {out}")
            return 0

        if args.cmd == "panel-replace":
            forbid_local_write("panel-replace")

        if args.cmd == "diagnostics":
            out = {
                "time": dt.datetime.now().isoformat(),
                "base_dir": str(base_dir),
                "files": {},
            }
            for p in default_backup_paths(base_dir):
                out["files"][str(p)] = {"exists": p.exists(), "size": p.stat().st_size if p.exists() else None}

            try:
                ctx = await ensure_ws()
                states = await ws_success(ctx, {"type": "get_states"})
                out["ha_states_count"] = len(states) if isinstance(states, list) else None
            except Exception as exc:
                out["ha_states_error"] = str(exc)

            print(json.dumps(out, ensure_ascii=False, indent=2))
            return 0

        if args.cmd == "podsumowanie-logow-systemowych":
            ctx = await ensure_ws()
            entries = await ws_success(ctx, {"type": "system_log/list"})
            repairs = await ws_success(ctx, {"type": "repairs/list_issues"})
            result = build_system_log_summary(
                entries=entries,
                repairs=repairs,
                levels=normalize_system_log_levels(args.level),
                logger_filter=args.logger,
                contains_filter=args.contains,
                source_filter=args.source_contains,
                limit=args.limit,
                include_exception=args.include_exception,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        if args.cmd == "historia-komunikatow":
            from_time = parse_local_datetime_input(args.from_time, "--from-time")
            to_time = parse_local_datetime_input(args.to_time, "--to-time", default_now=True)
            if to_time < from_time:
                raise TalkHaError("--to-time must be >= --from-time")

            sql = (
                "SELECT HEX(e.context_id_bin), e.time_fired_ts, et.event_type, COALESCE(ed.shared_data, '') "
                "FROM events e "
                "JOIN event_types et ON e.event_type_id = et.event_type_id "
                "LEFT JOIN event_data ed ON e.data_id = ed.data_id "
                f"WHERE e.time_fired_ts BETWEEN {from_time.timestamp():.6f} AND {to_time.timestamp():.6f} "
                "AND ("
                "et.event_type = 'automation_triggered' "
                "OR (et.event_type = 'call_service' AND ("
                "(ed.shared_data LIKE '%\"domain\":\"tts\"%' AND ed.shared_data LIKE '%\"service\":\"speak\"%') "
                "OR ed.shared_data LIKE '%\"service\":\"informator\"%' "
                "OR ed.shared_data LIKE '%\"service\":\"powiedz_tekst_gosia_duplikuj\"%'"
                "))"
                ") "
                "ORDER BY e.time_fired_ts ASC, e.event_id ASC"
            )
            rows = run_recorder_query_via_ssh(base_dir, sql)
            result = build_message_history(
                rows=rows,
                from_time=from_time,
                to_time=to_time,
                kind=normalize_message_kind(args.kind),
                source_contains=args.source_contains,
                contains=args.contains,
                limit=args.limit,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        if args.cmd == "compare-spec":
            spec_path = Path(args.spec_file).expanduser().resolve()
            scripts_spec, automations_spec = load_compare_spec(spec_path)

            scripts_file = Path(args.scripts_file).expanduser().resolve() if args.scripts_file else base_dir / "scripts.yaml"
            automations_file = Path(args.automations_file).expanduser().resolve() if args.automations_file else base_dir / "automations.yaml"
            if not scripts_file.exists():
                raise TalkHaError(f"Missing scripts file: {scripts_file}")
            if not automations_file.exists():
                raise TalkHaError(f"Missing automations file: {automations_file}")

            scripts_data = yaml.safe_load(scripts_file.read_text(encoding="utf-8")) if yaml else None
            autos_data = yaml.safe_load(automations_file.read_text(encoding="utf-8")) if yaml else None
            if not isinstance(scripts_data, dict):
                raise TalkHaError("scripts.yaml must decode to mapping")
            if not isinstance(autos_data, list):
                raise TalkHaError("automations.yaml must decode to list")

            s_by_key, s_by_alias = build_script_indexes(scripts_data)
            a_by_id, a_by_alias = build_automation_indexes(autos_data)

            report: Dict[str, Any] = {
                "spec_file": str(spec_path),
                "scripts_file": str(scripts_file),
                "automations_file": str(automations_file),
                "scripts": [],
                "automations": [],
                "summary": {"scripts": {"match": 0, "diff": 0, "missing": 0, "ambiguous": 0, "invalid_spec": 0},
                            "automations": {"match": 0, "diff": 0, "missing": 0, "ambiguous": 0, "invalid_spec": 0}},
            }

            for spec in scripts_spec:
                state, curr, matched_key = match_script_spec(spec, s_by_key, s_by_alias)
                alias = str(spec.get("alias", ""))
                row: Dict[str, Any] = {"alias": alias, "matched_key": matched_key, "state": state}
                if state == "FOUND" and curr is not None:
                    diffs = compare_subset(spec, curr, strict_text=args.strict_text)
                    row["state"] = "MATCH" if not diffs else "DIFF"
                    row["diffs"] = diffs
                report["scripts"].append(row)

            for spec in automations_spec:
                state, curr, matched_id = match_automation_spec(spec, a_by_id, a_by_alias)
                alias = str(spec.get("alias", ""))
                row = {"alias": alias, "matched_id": matched_id, "state": state}
                if state == "FOUND" and curr is not None:
                    diffs = compare_subset(spec, curr, strict_text=args.strict_text)
                    row["state"] = "MATCH" if not diffs else "DIFF"
                    row["diffs"] = diffs
                report["automations"].append(row)

            for kind in ("scripts", "automations"):
                for row in report[kind]:
                    state = str(row["state"]).lower()
                    if state == "match":
                        report["summary"][kind]["match"] += 1
                    elif state == "diff":
                        report["summary"][kind]["diff"] += 1
                    elif state == "missing":
                        report["summary"][kind]["missing"] += 1
                    elif state == "ambiguous":
                        report["summary"][kind]["ambiguous"] += 1
                    elif state == "invalid_spec":
                        report["summary"][kind]["invalid_spec"] += 1

            if args.report_out:
                out = Path(args.report_out).expanduser().resolve()
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            if args.as_json:
                print(json.dumps(report, ensure_ascii=False, indent=2))
            else:
                print("SCRIPTS:")
                for row in report["scripts"]:
                    print(f"- {row.get('alias','')} -> {row['state']}")
                    if row.get("diffs"):
                        print(f"  diffs: {', '.join(row['diffs'])}")
                print("AUTOMATIONS:")
                for row in report["automations"]:
                    print(f"- {row.get('alias','')} -> {row['state']}")
                    if row.get("diffs"):
                        print(f"  diffs: {', '.join(row['diffs'])}")
                print("SUMMARY:")
                print(json.dumps(report["summary"], ensure_ascii=False))
            return 0

        if args.cmd == "undo":
            tx_dir, tx = txm.load_tx(args.tx_id)
            if tx.get("status") != "ok":
                raise TalkHaError(f"Cannot undo transaction with status {tx.get('status')}")

            undo_tx = txm.start("undo", {"source_tx": args.tx_id})
            txm.backup_files(undo_tx["tx_dir"], undo_tx["tx"], default_backup_paths(base_dir))

            for action in reversed(tx.get("undo_actions", [])):
                a_type = action.get("type")
                if a_type == "restore_file_from_backup":
                    idx = int(action.get("backup_index", 0))
                    backup_files = tx.get("backup_files", [])
                    if idx >= len(backup_files):
                        raise TalkHaError("Invalid undo backup index")
                    backup = Path(backup_files[idx]["backup"])
                    target = Path(action["target"])
                    shutil.copy2(backup, target)

                elif a_type == "helper-delete":
                    kind = action["kind"]
                    hid = action["id"]
                    storage = helper_file_path(base_dir, kind)
                    data = load_helper_storage(storage)
                    items = data["data"]["items"]
                    items[:] = [it for it in items if str(it.get("id", "")) != hid]
                    write_json_atomic(storage, data)

                elif a_type == "helper-restore":
                    kind = action["kind"]
                    hid = action["id"]
                    item = action["item"]
                    storage = helper_file_path(base_dir, kind)
                    data = load_helper_storage(storage)
                    items = data["data"]["items"]
                    done = False
                    for it in items:
                        if str(it.get("id", "")) == hid:
                            it.clear()
                            it.update(item)
                            done = True
                            break
                    if not done:
                        items.append(item)
                    write_json_atomic(storage, data)

                elif a_type == "ws_restore_automation_category":
                    ctx = await ensure_ws()
                    await ws_update_entity_category(
                        ctx,
                        entity_id=str(action["entity_id"]),
                        scope=str(action.get("scope", "automation")),
                        category_id=action.get("category_id"),
                    )

                else:
                    raise TalkHaError(f"Unknown undo action type: {a_type}")

            txm.finish(undo_tx["tx_dir"], undo_tx["tx"], "ok", f"Undo executed for {args.tx_id}")
            print(f"OK: undo {args.tx_id}")
            return 0

        raise TalkHaError(f"Unsupported command: {args.cmd}")

    except Exception as exc:
        if isinstance(exc, TalkHaError):
            raise
        raise TalkHaError(f"Unexpected failure: {exc}") from exc
    finally:
        if ws_ctx is not None:
            await ws_ctx.ws.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="TalkHa - safe HA operations bridge")
    p.add_argument("--base-dir", default=str(DEFAULT_BASE_DIR), help="HA base directory")
    p.add_argument("--config-file", default=str(DEFAULT_CONFIG_PATH), help="Auth config file")
    p.add_argument("--state-dir", default=str(DEFAULT_STATE_DIR), help="State directory (tx reports)")
    p.add_argument("--backup-root", default=str(DEFAULT_BACKUP_ROOT), help="Backup root")
    p.add_argument("--log-file", default=str(DEFAULT_LOG_FILE), help="TalkHa log file")

    sub = p.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init-auth", help="Save HA_URL + HA_TOKEN once")
    p_init.add_argument("--ha-url", default="", help="e.g. http://192.168.2.70:8123")
    p_init.add_argument("--token", default="", help="Long-Lived token")
    p_init.add_argument("--token-file", default="", help="Path to file containing only token")

    sub.add_parser("test-auth", help="Test authentication")

    p_audit = sub.add_parser("automation-alias-audit", help="List automations by friendly_name fragment")
    p_audit.add_argument("--contains", default="", help="Case-insensitive text in automation friendly_name")
    p_audit.add_argument("--as-json", action="store_true")

    p_snap = sub.add_parser("snapshot", help="Create snapshot backup")
    p_snap.add_argument("--name", default="manual", help="Snapshot label")
    p_snap.add_argument("--include", action="append", help="Additional path (repeatable)")

    p_report = sub.add_parser("tx-report", help="Print transaction report")
    p_report.add_argument("--tx-id", required=True)

    p_undo = sub.add_parser("undo", help="Undo transaction")
    p_undo.add_argument("--tx-id", required=True)

    p_diag = sub.add_parser("diagnostics", help="Basic diagnostics report")
    p_diag.add_argument("--as-json", action="store_true", help="(reserved)")

    p_syslog = sub.add_parser("podsumowanie-logow-systemowych", help="Summarize current Home Assistant system_log entries")
    p_syslog.add_argument("--level", default="CRITICAL,ERROR,WARNING", help="Comma-separated levels")
    p_syslog.add_argument("--logger", default="", help="Case-insensitive logger filter")
    p_syslog.add_argument("--contains", default="", help="Case-insensitive message filter")
    p_syslog.add_argument("--source-contains", default="", help="Case-insensitive source path filter")
    p_syslog.add_argument("--limit", type=int, default=100, help="Maximum number of groups to return")
    p_syslog.add_argument("--include-exception", action="store_true", help="Include exception field in output")
    p_syslog.add_argument("--as-json", action="store_true", help="(reserved)")

    p_msgs = sub.add_parser("historia-komunikatow", help="Return TTS/Telegram message history from Recorder (MariaDB)")
    p_msgs.add_argument("--from-time", required=True, help="Local time, e.g. '2026-03-20 00:00:00'")
    p_msgs.add_argument("--to-time", default="", help="Local time, default: now")
    p_msgs.add_argument("--kind", default="all", choices=["all", "tts", "telegram"], help="Filter message type")
    p_msgs.add_argument("--source-contains", default="", help="Case-insensitive source alias/entity/trigger filter")
    p_msgs.add_argument("--contains", default="", help="Case-insensitive text filter")
    p_msgs.add_argument("--limit", type=int, default=200, help="Maximum number of messages to return")
    p_msgs.add_argument("--as-json", action="store_true", help="(reserved)")

    p_cmp = sub.add_parser("compare-spec", help="Compare scripts/automations against YAML spec file")
    p_cmp.add_argument("--spec-file", required=True, help="YAML spec path")
    p_cmp.add_argument("--scripts-file", default="", help="Override scripts.yaml path")
    p_cmp.add_argument("--automations-file", default="", help="Override automations.yaml path")
    p_cmp.add_argument("--strict-text", action="store_true", help="Disable semantic text normalization in compare")
    p_cmp.add_argument("--as-json", action="store_true", help="Print JSON report")
    p_cmp.add_argument("--report-out", default="", help="Save full report JSON")

    p_wsc = sub.add_parser("ws-call", help="Generic WS call")
    p_wsc.add_argument("--type", required=True, help="WS message type")
    p_wsc.add_argument("--payload-json", default="{}", help="Additional JSON object payload")
    p_wsc.add_argument("--mutating", action="store_true", help="Mark as mutating (requires explicit confirm)")
    p_wsc.add_argument("--explicit-confirm", default="")

    p_svc = sub.add_parser("service-call", help="Call HA service over WS")
    p_svc.add_argument("--domain", required=True)
    p_svc.add_argument("--service", required=True)
    p_svc.add_argument("--target-json", default="{}")
    p_svc.add_argument("--data-json", default="{}")
    p_svc.add_argument("--explicit-confirm", required=True)

    p_cat = sub.add_parser("list-categories", help="List category registry")
    p_cat.add_argument("--scope", default="automation")

    p_setcat = sub.add_parser("set-automation-category", help="Assign category to automation")
    p_setcat.add_argument("--automation-id", default="")
    p_setcat.add_argument("--entity-id", default="")
    p_setcat.add_argument("--category", required=True)
    p_setcat.add_argument("--scope", default="automation")

    p_hl = sub.add_parser("helper-list", help="List GUI helpers from runtime/entity registry")
    p_hl.add_argument("--kind", required=True, choices=sorted(HELPER_STORAGE_FILES.keys()))
    p_hl.add_argument("--as-json", action="store_true")

    p_ge = sub.add_parser("get-entity", help="Return single entity state by entity_id")
    p_ge.add_argument("--entity-id", required=True)

    sub.add_parser("zigbee-status-report", help="Summarize Zigbee bridge online/offline status from runtime states")

    p_hu = sub.add_parser("helper-upsert", help="Create/update GUI helper over WebSocket/API")
    p_hu.add_argument("--kind", required=True, choices=sorted(HELPER_STORAGE_FILES.keys()))
    p_hu.add_argument("--helper", required=True, help="id or entity_id")
    p_hu.add_argument("--item-json", default="{}", help="JSON patch merged into helper item")
    p_hu.add_argument("--explicit-confirm", required=True)

    p_hd = sub.add_parser("helper-delete", help="Delete GUI helper over WebSocket/API")
    p_hd.add_argument("--kind", required=True, choices=sorted(HELPER_STORAGE_FILES.keys()))
    p_hd.add_argument("--helper", required=True, help="id or entity_id")
    p_hd.add_argument("--explicit-confirm", required=True)

    p_ra = sub.add_parser("replace-automation-block", help="Replace automation YAML block by id")
    p_ra.add_argument("--target-id", required=True)
    p_ra.add_argument("--new-block-path", required=True)
    p_ra.add_argument("--automations-file", default="")

    p_aa = sub.add_parser("add-automation-block", help="Add automation YAML block under category")
    p_aa.add_argument("--new-block-path", required=True)
    p_aa.add_argument("--category", required=True)
    p_aa.add_argument("--insert-mode", default="create_category_then_insert", choices=["under_category", "create_category_then_insert"])
    p_aa.add_argument("--automations-file", default="")

    p_rs = sub.add_parser("replace-script-block", help="Replace script YAML block by top-level key")
    p_rs.add_argument("--target-key", required=True)
    p_rs.add_argument("--new-block-path", required=True)
    p_rs.add_argument("--scripts-file", default="")

    p_pe = sub.add_parser("panel-export", help="Export storage dashboard JSON")
    p_pe.add_argument("--dashboard-id", required=True, help="e.g. dashboard_test or test")
    p_pe.add_argument("--output", default="")

    p_pr = sub.add_parser("panel-replace", help="Replace storage dashboard JSON")
    p_pr.add_argument("--dashboard-id", required=True)
    p_pr.add_argument("--new-json", required=True)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return asyncio.run(run_async(args))
    except TalkHaError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
