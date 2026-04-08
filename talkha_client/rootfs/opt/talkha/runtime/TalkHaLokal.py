#!/usr/bin/env python3
"""TalkHaLokal - local Home Assistant config operator with safe transactions.

Focus:
- local file operations (no HA API required)
- deterministic find/replace for automations/scripts/helpers
- compact snapshots for LLM token reduction
"""

from __future__ import annotations

import argparse
import base64
from collections import Counter
from copy import deepcopy
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import yaml
except Exception as exc:  # pragma: no cover
    print(f"ERROR: PyYAML not available: {exc}", file=sys.stderr)
    sys.exit(2)

from talkha_investigate import run_investigation
from talkha_przebieg_zdarzen import run_event_timeline

DEFAULT_BASE_DIR = Path("/homeassistant")
DEFAULT_STATE_DIR = Path("/data/.talkhalokal_state")
DEFAULT_LOG_FILE = Path("/data/talkhalokal.log")
DEFAULT_BACKUP_ROOT = Path("/homeassistant/TalkHaBackup")
DEFAULT_AUTOMATIONS_FILE = Path("/homeassistant/automations.yaml")
DEFAULT_SCRIPTS_FILE = Path("/homeassistant/scripts.yaml")
DEFAULT_STORAGE_DIR = Path("/homeassistant/.storage")
DEFAULT_LOVELACE_FILE = Path("/homeassistant/.storage/lovelace")
DEFAULT_HA_HOST = "local-addon"
DEFAULT_TALKHA_RUNTIME = Path("/opt/talkha/runtime/TalkHa.py")

HELPER_FILES = {
    "input_boolean": "input_boolean",
    "input_number": "input_number",
    "input_text": "input_text",
    "input_select": "input_select",
}

ENTITY_ID_RE = re.compile(r"\b[a-z_]+\.[a-zA-Z0-9_]+\b")


class TalkHaLokalError(Exception):
    pass


@dataclass
class TxCtx:
    tx_id: str
    tx_dir: Path
    report_path: Path


class TxManager:
    def __init__(self, state_dir: Path, log_file: Path, backup_root: Path) -> None:
        self.state_dir = state_dir
        self.log_file = log_file
        self.backup_root = backup_root

    def _now(self) -> str:
        return dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")

    def _append_log(self, row: Dict[str, Any]) -> None:
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        with self.log_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def start(self, op: str, payload: Dict[str, Any]) -> TxCtx:
        tx_id = f"{self._now()}_{uuid.uuid4().hex[:8]}"
        tx_dir = self.state_dir / "transactions" / tx_id
        tx_dir.mkdir(parents=True, exist_ok=True)
        report = {
            "tx_id": tx_id,
            "operation": op,
            "started_at": dt.datetime.now().isoformat(),
            "status": "started",
            "payload": payload,
            "backups": [],
            "notes": [],
            "checks": {},
            "runtime_actions": [],
            "rollback": None,
        }
        report_path = tx_dir / "report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        self._append_log({"level": "INFO", "event": "tx_start", **report})
        return TxCtx(tx_id=tx_id, tx_dir=tx_dir, report_path=report_path)

    def _load(self, tx: TxCtx) -> Dict[str, Any]:
        return json.loads(tx.report_path.read_text(encoding="utf-8"))

    def _save(self, tx: TxCtx, data: Dict[str, Any]) -> None:
        tx.report_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def backup_files(self, tx: TxCtx, files: Iterable[Path], backup_dir: Path) -> None:
        data = self._load(tx)
        bdir = backup_dir / tx.tx_id
        bdir.mkdir(parents=True, exist_ok=True)
        data["backup_dir"] = str(bdir)
        for idx, src in enumerate(files, start=1):
            if not src.exists():
                continue
            dst = bdir / f"{idx:02d}_{src.name}"
            shutil.copy2(src, dst)
            data["backups"].append({"source": str(src), "backup": str(dst), "sha256": sha256_file(src)})
        self._save(tx, data)

    def rollback_tx_ctx(self, tx: TxCtx) -> int:
        data = self._load(tx)
        backups = data.get("backups", [])
        if not backups:
            return 0
        restored = 0
        for row in backups:
            src = Path(row["source"])
            bak = Path(row["backup"])
            if not bak.exists():
                raise TalkHaLokalError(f"Missing backup file: {bak}")
            shutil.copy2(bak, src)
            restored += 1
        return restored

    def note(self, tx: TxCtx, note: str) -> None:
        data = self._load(tx)
        data.setdefault("notes", []).append(note)
        self._save(tx, data)

    def set_phase(self, tx: TxCtx, phase: str) -> None:
        data = self._load(tx)
        data["phase"] = phase
        self._save(tx, data)

    def set_check(self, tx: TxCtx, name: str, payload: Dict[str, Any]) -> None:
        data = self._load(tx)
        data.setdefault("checks", {})[name] = payload
        self._save(tx, data)

    def add_runtime_action(self, tx: TxCtx, payload: Dict[str, Any]) -> None:
        data = self._load(tx)
        data.setdefault("runtime_actions", []).append(payload)
        self._save(tx, data)

    def set_rollback(self, tx: TxCtx, payload: Dict[str, Any]) -> None:
        data = self._load(tx)
        data["rollback"] = payload
        self._save(tx, data)

    def finish(self, tx: TxCtx, status: str, message: str) -> None:
        data = self._load(tx)
        data["status"] = status
        data["finished_at"] = dt.datetime.now().isoformat()
        data["message"] = message
        self._save(tx, data)
        self._append_log({"level": "INFO" if status == "ok" else "ERROR", "event": "tx_finish", **data})

    def load_tx_by_id(self, tx_id: str) -> Dict[str, Any]:
        p = self.state_dir / "transactions" / tx_id / "report.json"
        if not p.exists():
            raise TalkHaLokalError(f"Transaction not found: {tx_id}")
        return json.loads(p.read_text(encoding="utf-8"))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def load_yaml(path: Path) -> Any:
    if not path.exists():
        raise TalkHaLokalError(f"File not found: {path}")
    raw = path.read_text(encoding="utf-8")
    return yaml.safe_load(raw)


def dump_yaml(data: Any) -> str:
    return yaml.safe_dump(data, allow_unicode=True, sort_keys=False)


def dump_yaml_block(data: Any) -> str:
    return yaml.safe_dump(data, allow_unicode=True, sort_keys=False).rstrip() + "\n"


def dump_json_pretty(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2) + "\n"


def _line_start_offsets(text: str) -> List[int]:
    offsets = [0]
    for idx, ch in enumerate(text):
        if ch == "\n":
            offsets.append(idx + 1)
    return offsets


def _root_node(path: Path) -> Tuple[str, Any]:
    raw = path.read_text(encoding="utf-8")
    node = yaml.compose(raw)
    return raw, node


def _automation_text_ranges(path: Path) -> List[Tuple[int, int]]:
    raw, node = _root_node(path)
    if node is None:
        return []
    if not isinstance(node, yaml.nodes.SequenceNode):
        raise TalkHaLokalError("automations.yaml must be YAML list")
    line_offsets = _line_start_offsets(raw)
    starts = [line_offsets[item.start_mark.line] for item in node.value]
    ends = starts[1:] + [len(raw)]
    return list(zip(starts, ends))


def _script_text_ranges(path: Path) -> Dict[str, Tuple[int, int]]:
    raw, node = _root_node(path)
    if node is None:
        return {}
    if not isinstance(node, yaml.nodes.MappingNode):
        raise TalkHaLokalError("scripts.yaml must be YAML mapping")
    starts: List[Tuple[str, int]] = []
    for key_node, _value_node in node.value:
        starts.append((str(key_node.value), key_node.start_mark.index))
    out: Dict[str, Tuple[int, int]] = {}
    for idx, (key, start) in enumerate(starts):
        end = starts[idx + 1][1] if idx + 1 < len(starts) else len(raw)
        out[key] = (start, end)
    return out


def render_automation_block(block: Dict[str, Any]) -> str:
    return dump_yaml_block([block])


def render_script_block(key: str, block: Dict[str, Any]) -> str:
    return dump_yaml_block({key: block})


def replace_automation_block_text(path: Path, index: int, block: Dict[str, Any]) -> None:
    raw = path.read_text(encoding="utf-8")
    ranges = _automation_text_ranges(path)
    if index < 0 or index >= len(ranges):
        raise TalkHaLokalError(f"Automation index out of range: {index}")
    start, end = ranges[index]
    atomic_write_text(path, raw[:start] + render_automation_block(block) + raw[end:])


def append_automation_block_text(path: Path, block: Dict[str, Any]) -> None:
    raw = path.read_text(encoding="utf-8") if path.exists() else ""
    rendered = render_automation_block(block)
    if raw and not raw.endswith("\n"):
        raw += "\n"
    atomic_write_text(path, raw + rendered)


def delete_automation_block_text(path: Path, index: int) -> None:
    raw = path.read_text(encoding="utf-8")
    ranges = _automation_text_ranges(path)
    if index < 0 or index >= len(ranges):
        raise TalkHaLokalError(f"Automation index out of range: {index}")
    start, end = ranges[index]
    atomic_write_text(path, raw[:start] + raw[end:])


def replace_script_block_text(path: Path, key: str, block: Dict[str, Any]) -> None:
    raw = path.read_text(encoding="utf-8")
    ranges = _script_text_ranges(path)
    if key not in ranges:
        raise TalkHaLokalError(f"Script key not found: {key}")
    start, end = ranges[key]
    atomic_write_text(path, raw[:start] + render_script_block(key, block) + raw[end:])


def append_script_block_text(path: Path, key: str, block: Dict[str, Any]) -> None:
    raw = path.read_text(encoding="utf-8") if path.exists() else ""
    rendered = render_script_block(key, block)
    if raw and not raw.endswith("\n"):
        raw += "\n"
    atomic_write_text(path, raw + rendered)


def delete_script_block_text(path: Path, key: str) -> None:
    raw = path.read_text(encoding="utf-8")
    ranges = _script_text_ranges(path)
    if key not in ranges:
        raise TalkHaLokalError(f"Script key not found: {key}")
    start, end = ranges[key]
    atomic_write_text(path, raw[:start] + raw[end:])


def run_external(cmd: List[str]) -> Dict[str, Any]:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return {
        "cmd": cmd,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "ok": proc.returncode == 0,
    }


def run_ha_core_check(ha_host: str) -> Dict[str, Any]:
    if os.environ.get("TALKHA_ADDON_MODE", "").lower() in {"1", "true", "yes"}:
        return {
            "ok": True,
            "returncode": 0,
            "stdout": "ha core check skipped in add-on mode",
            "stderr": "",
            "cmd": ["ha", "core", "check"],
        }
    return run_external(["ssh", ha_host, "ha core check"])


def run_reload_service(talkha_runtime: Path, domain: str, service: str) -> Dict[str, Any]:
    return run_external(
        [
            "python3",
            str(talkha_runtime),
            "service-call",
            "--domain",
            domain,
            "--service",
            service,
            "--explicit-confirm",
            "REQUIRED",
        ]
    )


def reload_plan_for_mutation(cmd: str, helper_kind: str = "") -> List[Tuple[str, str]]:
    if cmd == "upsert-automation":
        return [("automation", "reload")]
    if cmd == "upsert-script":
        return [("script", "reload")]
    if cmd == "helper-upsert":
        if helper_kind not in HELPER_FILES:
            raise TalkHaLokalError(f"Unsupported helper kind for reload: {helper_kind}")
        return [(helper_kind, "reload")]
    raise TalkHaLokalError(f"Unsupported mutation command for reload plan: {cmd}")


def run_reload_plan(tx: TxCtx, txm: TxManager, talkha_runtime: Path, reload_plan: List[Tuple[str, str]]) -> Dict[str, Any]:
    results = []
    for domain, service in reload_plan:
        result = run_reload_service(talkha_runtime, domain, service)
        action = {"domain": domain, "service": service, **result}
        txm.add_runtime_action(tx, action)
        results.append(action)
        if not result["ok"]:
            return {"ok": False, "results": results}
    return {"ok": True, "results": results}


def rollback_with_restore(
    tx: TxCtx,
    txm: TxManager,
    ha_host: str,
    talkha_runtime: Path,
    reload_plan: List[Tuple[str, str]],
    reason: str,
) -> int:
    restored = txm.rollback_tx_ctx(tx)
    rollback_payload: Dict[str, Any] = {"reason": reason, "restored_files": restored}
    check = run_ha_core_check(ha_host)
    rollback_payload["ha_core_check"] = check
    if check["ok"]:
        rollback_payload["reload"] = run_reload_plan(tx, txm, talkha_runtime, reload_plan)
    txm.set_rollback(tx, rollback_payload)
    return restored


@contextmanager
def file_mutex(state_dir: Path, name: str, timeout_sec: float = 300.0, poll_sec: float = 0.2):
    lock_dir = state_dir / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{name}.lock"
    fh = lock_path.open("a+", encoding="utf-8")
    start = time.monotonic()
    locked = False
    try:
        while True:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
                fh.seek(0)
                fh.truncate()
                fh.write(json.dumps({"pid": os.getpid(), "locked_at": dt.datetime.now().isoformat()}) + "\n")
                fh.flush()
                break
            except BlockingIOError:
                if time.monotonic() - start >= timeout_sec:
                    raise TalkHaLokalError(f"Mutation lock timeout for {name} after {timeout_sec:.0f}s")
                time.sleep(poll_sec)
        yield
    finally:
        if locked:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            finally:
                fh.close()
        else:
            fh.close()


def finalize_interrupted_mutation(
    tx: TxCtx,
    txm: TxManager,
    ha_host: str,
    talkha_runtime: Path,
    reload_plan: List[Tuple[str, str]],
    exc: BaseException,
) -> None:
    data = txm._load(tx)
    if data.get("status") != "started":
        return
    phase = data.get("phase", "started")
    wrote = phase in {"written", "integrity_ok", "ha_core_check_ok", "reload_ok"}
    reload_ok = data.get("checks", {}).get("reload", {}).get("ok") is True
    exc_name = exc.__class__.__name__

    if wrote and not reload_ok:
        try:
            restored = rollback_with_restore(tx, txm, ha_host, talkha_runtime, reload_plan, "interrupted")
            txm.finish(
                tx,
                "error",
                f"Mutation interrupted at phase '{phase}'; rollback restored {restored} file(s): {exc_name}",
            )
        except Exception as rollback_exc:
            txm.set_rollback(
                tx,
                {
                    "reason": "interrupted_rollback_failed",
                    "error": str(rollback_exc),
                    "phase": phase,
                },
            )
            txm.finish(
                tx,
                "error",
                f"Mutation interrupted at phase '{phase}' and rollback failed: {rollback_exc}",
            )
        return

    txm.finish(tx, "error", f"Mutation interrupted at phase '{phase}': {exc_name}")


def success_payload(txm: TxManager, tx: TxCtx, extra: Dict[str, Any]) -> Dict[str, Any]:
    data = txm._load(tx)
    payload = {
        "tx_id": tx.tx_id,
        "status": data.get("status"),
        "message": data.get("message"),
        "backup_dir": data.get("backup_dir"),
        "checks": data.get("checks", {}),
        "rollback": data.get("rollback"),
    }
    payload.update(extra)
    return payload


def load_automations(path: Path) -> List[Dict[str, Any]]:
    data = load_yaml(path)
    if data is None:
        return []
    if not isinstance(data, list):
        raise TalkHaLokalError("automations.yaml must be YAML list")
    out: List[Dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            out.append(item)
    return out


def load_scripts(path: Path) -> Dict[str, Dict[str, Any]]:
    data = load_yaml(path)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise TalkHaLokalError("scripts.yaml must be YAML mapping")
    out: Dict[str, Dict[str, Any]] = {}
    for k, v in data.items():
        if isinstance(v, dict):
            out[str(k)] = v
    return out


def get_helper_path(storage_dir: Path, kind: str) -> Path:
    if kind not in HELPER_FILES:
        raise TalkHaLokalError(f"Unsupported helper kind: {kind}")
    return storage_dir / HELPER_FILES[kind]


def load_helper_items(storage_dir: Path, kind: str) -> List[Dict[str, Any]]:
    path = get_helper_path(storage_dir, kind)
    if not path.exists():
        raise TalkHaLokalError(f"Helper storage file missing: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    items = raw.get("data", {}).get("items")
    if not isinstance(items, list):
        raise TalkHaLokalError(f"Invalid helper storage structure: {path}")
    return items


def save_helper_items(storage_dir: Path, kind: str, items: List[Dict[str, Any]]) -> None:
    path = get_helper_path(storage_dir, kind)
    raw = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"version": 1, "minor_version": 1, "key": kind, "data": {"items": []}}
    if "data" not in raw or not isinstance(raw["data"], dict):
        raw["data"] = {}
    raw["data"]["items"] = items
    atomic_write_text(path, json.dumps(raw, ensure_ascii=False, indent=2) + "\n")


def validate_helper_item(kind: str, item: Dict[str, Any]) -> None:
    if "id" not in item or not str(item["id"]).strip():
        raise TalkHaLokalError("Helper item requires non-empty 'id'")

    if kind == "input_boolean":
        if "name" not in item:
            raise TalkHaLokalError("input_boolean requires 'name'")
    elif kind == "input_number":
        for req in ("name", "min", "max", "step"):
            if req not in item:
                raise TalkHaLokalError(f"input_number requires '{req}'")
    elif kind == "input_text":
        for req in ("name", "min", "max", "mode"):
            if req not in item:
                raise TalkHaLokalError(f"input_text requires '{req}'")
    elif kind == "input_select":
        if "name" not in item or "options" not in item:
            raise TalkHaLokalError("input_select requires 'name' and 'options'")
        if not isinstance(item["options"], list) or not item["options"]:
            raise TalkHaLokalError("input_select options must be non-empty list")


def helper_payload_without_id(item: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in item.items() if k != "id"}


def extract_strings(obj: Any) -> Iterable[str]:
    if obj is None:
        return
    if isinstance(obj, str):
        yield obj
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from extract_strings(k)
            yield from extract_strings(v)
        return
    if isinstance(obj, list):
        for it in obj:
            yield from extract_strings(it)
        return


def extract_entity_ids(obj: Any) -> List[str]:
    found = set()
    for s in extract_strings(obj):
        for m in ENTITY_ID_RE.findall(s):
            found.add(m)
    return sorted(found)


def automation_enabled(a: Dict[str, Any]) -> str:
    init = str(a.get("initial_state", "")).strip().lower()
    if init in {"false", "off", "0"}:
        return "disabled"
    if init in {"true", "on", "1"}:
        return "enabled"
    return "enabled_or_default"


def summarize_automations(autos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for a in autos:
        out.append(
            {
                "id": str(a.get("id", "")),
                "alias": str(a.get("alias", "")),
                "enabled": automation_enabled(a),
                "entities": extract_entity_ids(a)[:40],
            }
        )
    return out


def summarize_scripts(scripts: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for key, body in scripts.items():
        out.append(
            {
                "key": key,
                "alias": str(body.get("alias", "")),
                "mode": str(body.get("mode", "")),
                "entities": extract_entity_ids(body)[:40],
            }
        )
    return out


def summarize_helpers(storage_dir: Path) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for kind in HELPER_FILES:
        try:
            items = load_helper_items(storage_dir, kind)
        except Exception:
            out[kind] = {"count": 0, "error": "unavailable"}
            continue
        out[kind] = {
            "count": len(items),
            "sample": [f"{kind}.{str(x.get('id',''))}" for x in items[:15]],
        }
    return out


def cmd_scan(args: argparse.Namespace) -> int:
    autos = load_automations(args.automations_file)
    scripts = load_scripts(args.scripts_file)
    payload = {
        "automations": summarize_automations(autos),
        "scripts": summarize_scripts(scripts),
        "helpers": summarize_helpers(args.storage_dir),
    }
    if args.compact:
        payload = {
            "counts": {
                "automations": len(payload["automations"]),
                "scripts": len(payload["scripts"]),
                "helpers": {k: v.get("count", 0) for k, v in payload["helpers"].items()},
            },
            "automations_top": payload["automations"][: args.limit],
            "scripts_top": payload["scripts"][: args.limit],
        }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_find(args: argparse.Namespace) -> int:
    q = args.query.strip().lower()
    if not q:
        raise TalkHaLokalError("--query cannot be empty")

    autos = load_automations(args.automations_file)
    scripts = load_scripts(args.scripts_file)

    out = {"automations": [], "scripts": [], "entities": []}

    for a in autos:
        txt = json.dumps(a, ensure_ascii=False).lower()
        if q in txt:
            out["automations"].append(
                {
                    "id": str(a.get("id", "")),
                    "alias": str(a.get("alias", "")),
                    "enabled": automation_enabled(a),
                    "entities": extract_entity_ids(a)[:30],
                }
            )

    for key, body in scripts.items():
        txt = json.dumps(body, ensure_ascii=False).lower()
        if q in txt or q in key.lower():
            out["scripts"].append(
                {
                    "key": key,
                    "alias": str(body.get("alias", "")),
                    "mode": str(body.get("mode", "")),
                    "entities": extract_entity_ids(body)[:30],
                }
            )

    ent_set = set()
    for row in out["automations"]:
        ent_set.update(row["entities"])
    for row in out["scripts"]:
        ent_set.update(row["entities"])
    out["entities"] = sorted(ent_set)[:200]

    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def parse_block_yaml(path: Path) -> Dict[str, Any]:
    obj = load_yaml(path)
    if isinstance(obj, list):
        if len(obj) != 1 or not isinstance(obj[0], dict):
            raise TalkHaLokalError("Block file must contain one YAML mapping")
        return obj[0]
    if isinstance(obj, dict):
        return obj
    raise TalkHaLokalError("Block file must be YAML mapping or one-item list")


def parse_block_text(raw: str) -> Dict[str, Any]:
    try:
        obj = yaml.safe_load(raw)
    except Exception as exc:
        raise TalkHaLokalError(f"Invalid block YAML text: {exc}") from exc
    if isinstance(obj, list):
        if len(obj) != 1 or not isinstance(obj[0], dict):
            raise TalkHaLokalError("Inline block must contain one YAML mapping")
        return obj[0]
    if isinstance(obj, dict):
        return obj
    raise TalkHaLokalError("Inline block must be YAML mapping or one-item list")


def resolve_block_arg(block_file: Optional[Path], block_base64: str) -> Dict[str, Any]:
    if block_file:
        return parse_block_yaml(block_file)
    if block_base64:
        try:
            raw = base64.b64decode(block_base64.encode("utf-8"), validate=True).decode("utf-8")
        except Exception as exc:
            raise TalkHaLokalError(f"Invalid --block-base64 payload: {exc}") from exc
        return parse_block_text(raw)
    raise TalkHaLokalError("Provide --block-file or --block-base64")


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def load_json_file(path: Path) -> Any:
    if not path.exists():
        raise TalkHaLokalError(f"File not found: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise TalkHaLokalError(f"Invalid JSON in {path}: {exc}") from exc


def path_to_string(path: Tuple[Any, ...]) -> str:
    if not path:
        return "$"
    parts = ["$"]
    for item in path:
        if isinstance(item, int):
            parts.append(f"[{item}]")
        else:
            parts.append(f".{item}")
    return "".join(parts)


def get_at_path(obj: Any, path: Tuple[Any, ...]) -> Any:
    cur = obj
    for item in path:
        cur = cur[item]
    return cur


def set_at_path(obj: Any, path: Tuple[Any, ...], value: Any) -> None:
    if not path:
        raise TalkHaLokalError("Cannot replace root object directly")
    parent = get_at_path(obj, path[:-1])
    parent[path[-1]] = value


def find_lovelace_cards_by_title(obj: Any, title: str) -> List[Tuple[Tuple[Any, ...], Dict[str, Any]]]:
    matches: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = []
    want = title.strip()

    def walk(node: Any, path: Tuple[Any, ...]) -> None:
        if isinstance(node, dict):
            if str(node.get("title", "")).strip() == want:
                matches.append((path, node))
            for key, value in node.items():
                walk(value, path + (key,))
        elif isinstance(node, list):
            for idx, value in enumerate(node):
                walk(value, path + (idx,))

    walk(obj, tuple())
    return matches


def replace_strings_in_obj(obj: Any, mapping: Dict[str, str]) -> Tuple[Any, int]:
    count = 0

    def walk(node: Any) -> Any:
        nonlocal count
        if isinstance(node, str):
            new_node = node
            for old, new in mapping.items():
                hits = new_node.count(old)
                if hits:
                    new_node = new_node.replace(old, new)
                    count += hits
            return new_node
        if isinstance(node, list):
            return [walk(x) for x in node]
        if isinstance(node, dict):
            return {k: walk(v) for k, v in node.items()}
        return node

    return walk(obj), count


def verify_lovelace_integrity(before: Any, after: Any, target_path: Tuple[Any, ...]) -> None:
    restored = deepcopy(after)
    set_at_path(restored, target_path, deepcopy(get_at_path(before, target_path)))
    if canonical_json(before) != canonical_json(restored):
        raise TalkHaLokalError("Integrity check failed: content outside target card changed unexpectedly")


def cmd_lovelace_find_card(args: argparse.Namespace) -> int:
    data = load_json_file(args.dashboard_file)
    matches = find_lovelace_cards_by_title(data, args.title)
    payload = {
        "dashboard_file": str(args.dashboard_file),
        "title": args.title,
        "matches": [
            {
                "index": idx + 1,
                "path": path_to_string(path),
                "type": str(card.get("type", "")),
                "keys": sorted(card.keys()),
                "preview": card,
            }
            for idx, (path, card) in enumerate(matches[: args.limit])
        ],
        "count": len(matches),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_lovelace_replace_entities_in_card(args: argparse.Namespace, txm: TxManager) -> int:
    mapping = json.loads(args.mapping_json)
    if not isinstance(mapping, dict) or not mapping:
        raise TalkHaLokalError("--mapping-json must be non-empty JSON object")
    mapping = {str(k): str(v) for k, v in mapping.items()}

    before = load_json_file(args.dashboard_file)
    matches = find_lovelace_cards_by_title(before, args.title)
    if not matches:
        raise TalkHaLokalError(f"Lovelace card title not found: {args.title}")
    if len(matches) > 1:
        raise TalkHaLokalError(f"Ambiguous Lovelace title '{args.title}': found {len(matches)} cards")

    target_path, target_card = matches[0]

    tx = txm.start(
        "lovelace-replace-entities-in-card",
        {
            "file": str(args.dashboard_file),
            "title": args.title,
            "path": path_to_string(target_path),
            "backup_dir": str(args.backup_dir),
            "mapping": mapping,
        },
    )
    txm.set_phase(tx, "started")
    try:
        with file_mutex(txm.state_dir, f"mutation_{args.dashboard_file.name}"):
            txm.set_phase(tx, "locked")
            txm.backup_files(tx, [args.dashboard_file], args.backup_dir)
            txm.set_phase(tx, "backed_up")

            new_card, replace_count = replace_strings_in_obj(target_card, mapping)
            if replace_count == 0:
                txm.finish(tx, "error", "No entity references matched mapping in target card")
                raise TalkHaLokalError("No entity references matched mapping in target card")

            after = deepcopy(before)
            set_at_path(after, target_path, new_card)

            atomic_write_text(args.dashboard_file, dump_json_pretty(after))
            txm.set_phase(tx, "written")

            written = load_json_file(args.dashboard_file)
            try:
                verify_lovelace_integrity(before, written, target_path)
            except Exception as exc:
                restored = txm.rollback_tx_ctx(tx)
                msg = f"Integrity check failed after Lovelace write; rollback restored {restored} file(s): {exc}"
                txm.set_check(tx, "integrity", {"ok": False, "error": str(exc)})
                txm.set_rollback(tx, {"reason": "integrity_failed", "restored_files": restored})
                txm.finish(tx, "error", msg)
                raise TalkHaLokalError(msg)

            txm.set_check(
                tx,
                "integrity",
                {
                    "ok": True,
                    "changed_only_target": True,
                    "target_path": path_to_string(target_path),
                    "replacements": replace_count,
                },
            )
            txm.set_phase(tx, "integrity_ok")
            txm.note(tx, "Lovelace storage change may require frontend refresh or HA restart")
            txm.finish(tx, "ok", "lovelace card entity replacement applied")
            print(
                json.dumps(
                    success_payload(
                        txm,
                        tx,
                        {
                            "dashboard_file": str(args.dashboard_file),
                            "title": args.title,
                            "path": path_to_string(target_path),
                            "replacements": replace_count,
                            "restart_or_refresh_may_be_required": True,
                        },
                    ),
                    ensure_ascii=False,
                )
            )
            return 0
    except BaseException as exc:
        finalize_interrupted_mutation(tx, txm, args.ha_host, args.talkha_runtime, [], exc)
        raise


def verify_automation_integrity(
    before: List[Dict[str, Any]],
    after: List[Dict[str, Any]],
    replaced: bool,
    target_index: Optional[int],
    expected_new_block: Optional[Dict[str, Any]],
) -> None:
    if replaced:
        if target_index is None:
            raise TalkHaLokalError("Internal integrity error: missing target index")
        if len(before) != len(after):
            raise TalkHaLokalError("Integrity check failed: automation count changed unexpectedly")
        for idx in range(len(before)):
            if idx == target_index:
                continue
            if canonical_json(before[idx]) != canonical_json(after[idx]):
                raise TalkHaLokalError(f"Integrity check failed: automation block #{idx + 1} changed unexpectedly")
        return

    if len(after) != len(before) + 1:
        raise TalkHaLokalError("Integrity check failed: expected exactly one new automation block")
    before_cnt = Counter(canonical_json(x) for x in before)
    after_cnt = Counter(canonical_json(x) for x in after)
    for key, val in before_cnt.items():
        if after_cnt.get(key, 0) < val:
            raise TalkHaLokalError("Integrity check failed: existing automation block was removed/changed")
    diff = after_cnt - before_cnt
    if sum(diff.values()) != 1:
        raise TalkHaLokalError("Integrity check failed: more than one automation block was added/changed")
    if expected_new_block is not None:
        new_fp = canonical_json(expected_new_block)
        if diff.get(new_fp, 0) != 1:
            raise TalkHaLokalError("Integrity check failed: added automation block does not match requested block")


def verify_script_integrity(
    before: Dict[str, Dict[str, Any]],
    after: Dict[str, Dict[str, Any]],
    target_key: str,
    existed: bool,
) -> None:
    before_keys = set(before.keys())
    after_keys = set(after.keys())
    if existed:
        if before_keys != after_keys:
            raise TalkHaLokalError("Integrity check failed: script key set changed unexpectedly")
        for key in before_keys:
            if key == target_key:
                continue
            if canonical_json(before[key]) != canonical_json(after[key]):
                raise TalkHaLokalError(f"Integrity check failed: script '{key}' changed unexpectedly")
        return

    if after_keys != (before_keys | {target_key}):
        raise TalkHaLokalError("Integrity check failed: expected exactly one new script key")
    for key in before_keys:
        if canonical_json(before[key]) != canonical_json(after[key]):
            raise TalkHaLokalError(f"Integrity check failed: script '{key}' changed unexpectedly")


def verify_automation_delete_integrity(
    before: List[Dict[str, Any]],
    after: List[Dict[str, Any]],
    target_index: int,
) -> None:
    if len(after) != len(before) - 1:
        raise TalkHaLokalError("Integrity check failed: expected exactly one removed automation block")
    expected = before[:target_index] + before[target_index + 1 :]
    if len(expected) != len(after):
        raise TalkHaLokalError("Integrity check failed: unexpected automation count after delete")
    for idx in range(len(after)):
        if canonical_json(expected[idx]) != canonical_json(after[idx]):
            raise TalkHaLokalError(f"Integrity check failed: automation block #{idx + 1} changed unexpectedly")


def verify_script_delete_integrity(
    before: Dict[str, Dict[str, Any]],
    after: Dict[str, Dict[str, Any]],
    target_key: str,
) -> None:
    before_keys = set(before.keys())
    after_keys = set(after.keys())
    if after_keys != (before_keys - {target_key}):
        raise TalkHaLokalError("Integrity check failed: expected exactly one removed script key")
    for key in after_keys:
        if canonical_json(before[key]) != canonical_json(after[key]):
            raise TalkHaLokalError(f"Integrity check failed: script '{key}' changed unexpectedly")


def helper_map_by_id(items: List[Dict[str, Any]], kind: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for item in items:
        hid = str(item.get("id", "")).strip()
        if not hid:
            raise TalkHaLokalError(f"Integrity check failed: {kind} item without id")
        if hid in out:
            raise TalkHaLokalError(f"Integrity check failed: duplicated helper id '{kind}.{hid}'")
        out[hid] = item
    return out


def verify_helper_integrity(
    kind: str,
    before: List[Dict[str, Any]],
    after: List[Dict[str, Any]],
    target_id: str,
    replaced: bool,
) -> None:
    bmap = helper_map_by_id(before, kind)
    amap = helper_map_by_id(after, kind)
    bkeys = set(bmap.keys())
    akeys = set(amap.keys())

    if replaced:
        if bkeys != akeys:
            raise TalkHaLokalError("Integrity check failed: helper id set changed unexpectedly")
        for hid in bkeys:
            if hid == target_id:
                continue
            if canonical_json(bmap[hid]) != canonical_json(amap[hid]):
                raise TalkHaLokalError(f"Integrity check failed: helper '{kind}.{hid}' changed unexpectedly")
        return

    if akeys != (bkeys | {target_id}):
        raise TalkHaLokalError("Integrity check failed: expected exactly one new helper id")
    for hid in bkeys:
        if canonical_json(bmap[hid]) != canonical_json(amap[hid]):
            raise TalkHaLokalError(f"Integrity check failed: helper '{kind}.{hid}' changed unexpectedly")


def _norm(s: str) -> str:
    return s.strip().casefold()


def find_automation_matches(autos: List[Dict[str, Any]], target: str, match_by: str) -> List[int]:
    t = _norm(target)
    if not t:
        return []
    out: List[int] = []
    for i, item in enumerate(autos):
        item_id = _norm(str(item.get("id", "")))
        item_alias = _norm(str(item.get("alias", "")))
        if match_by == "id" and item_id == t:
            out.append(i)
        elif match_by == "alias" and item_alias == t:
            out.append(i)
        elif match_by == "id-or-alias" and (item_id == t or item_alias == t):
            out.append(i)
    return out


def find_script_keys(scripts: Dict[str, Dict[str, Any]], target: str, match_by: str) -> List[str]:
    t = _norm(target)
    if not t:
        return []
    out: List[str] = []
    for key, body in scripts.items():
        k = _norm(key)
        alias = _norm(str(body.get("alias", "")))
        if match_by == "key" and k == t:
            out.append(key)
        elif match_by == "alias" and alias == t:
            out.append(key)
        elif match_by == "key-or-alias" and (k == t or alias == t):
            out.append(key)
    return out


def cmd_upsert_automation(args: argparse.Namespace, txm: TxManager) -> int:
    autos_before = load_automations(args.automations_file)
    autos = [dict(x) for x in autos_before]
    block = resolve_block_arg(args.block_file, args.block_base64)
    block_id = str(block.get("id", "")).strip()
    block_alias = str(block.get("alias", "")).strip()
    target = (args.target or "").strip() or block_id or block_alias

    tx = txm.start(
        "upsert-automation",
        {
            "file": str(args.automations_file),
            "target": target,
            "match_by": args.match_by,
            "backup_dir": str(args.backup_dir),
        },
    )
    txm.set_phase(tx, "started")
    try:
        with file_mutex(txm.state_dir, f"mutation_{args.automations_file.name}"):
            txm.set_phase(tx, "locked")
            txm.backup_files(tx, [args.automations_file], args.backup_dir)
            txm.set_phase(tx, "backed_up")

            replaced = False
            target_index: Optional[int] = None
            if target:
                matches = find_automation_matches(autos, target, args.match_by)
                if len(matches) > 1:
                    txm.finish(tx, "error", f"Ambiguous automation target: {target}")
                    raise TalkHaLokalError(f"Ambiguous automation target: {target}")
                if len(matches) == 1:
                    i = matches[0]
                    target_index = i
                    existing = autos[i]
                    existing_id = str(existing.get("id", "")).strip()
                    if not str(block.get("id", "")).strip():
                        block["id"] = existing_id
                    elif str(block.get("id", "")).strip() != existing_id and not args.allow_id_change:
                        txm.note(tx, f"id mismatch: preserving existing id {existing_id}")
                        block["id"] = existing_id
                    autos[i] = block
                    replaced = True

            if not replaced:
                if not str(block.get("id", "")).strip():
                    txm.finish(tx, "error", "New automation requires non-empty id")
                    raise TalkHaLokalError("New automation block requires non-empty id")
                if not args.allow_add:
                    txm.finish(tx, "error", "Automation target not found and --allow-add not set")
                    raise TalkHaLokalError("Automation target not found. Use --allow-add to append.")
                autos.append(block)

            if replaced:
                replace_automation_block_text(args.automations_file, target_index, block)
            else:
                append_automation_block_text(args.automations_file, block)
            txm.set_phase(tx, "written")

            autos_after = load_automations(args.automations_file)
            try:
                verify_automation_integrity(
                    before=autos_before,
                    after=autos_after,
                    replaced=replaced,
                    target_index=target_index,
                    expected_new_block=None if replaced else block,
                )
            except Exception as exc:
                restored = txm.rollback_tx_ctx(tx)
                msg = f"Integrity check failed after automation write; rollback restored {restored} file(s): {exc}"
                txm.set_check(tx, "integrity", {"ok": False, "error": str(exc)})
                txm.set_rollback(tx, {"reason": "integrity_failed", "restored_files": restored})
                txm.finish(tx, "error", msg)
                raise TalkHaLokalError(msg)
            txm.set_check(tx, "integrity", {"ok": True, "changed_only_target": True})
            txm.set_phase(tx, "integrity_ok")

            core_check = run_ha_core_check(args.ha_host)
            txm.set_check(tx, "ha_core_check", core_check)
            if not core_check["ok"]:
                restored = rollback_with_restore(
                    tx,
                    txm,
                    args.ha_host,
                    args.talkha_runtime,
                    reload_plan_for_mutation("upsert-automation"),
                    "ha_core_check_failed",
                )
                msg = f"ha core check failed after automation write; rollback restored {restored} file(s)"
                txm.finish(tx, "error", msg)
                raise TalkHaLokalError(msg)
            txm.set_phase(tx, "ha_core_check_ok")

            reload_result = run_reload_plan(tx, txm, args.talkha_runtime, reload_plan_for_mutation("upsert-automation"))
            txm.set_check(tx, "reload", reload_result)
            if not reload_result["ok"]:
                restored = rollback_with_restore(
                    tx,
                    txm,
                    args.ha_host,
                    args.talkha_runtime,
                    reload_plan_for_mutation("upsert-automation"),
                    "reload_failed",
                )
                msg = f"automation reload failed after write; rollback restored {restored} file(s)"
                txm.finish(tx, "error", msg)
                raise TalkHaLokalError(msg)
            txm.set_phase(tx, "reload_ok")
            txm.note(tx, "automation replaced" if replaced else "automation appended")
            txm.finish(tx, "ok", "automation upsert applied")
            print(json.dumps(success_payload(txm, tx, {"replaced": replaced}), ensure_ascii=False))
            return 0
    except BaseException as exc:
        finalize_interrupted_mutation(
            tx,
            txm,
            args.ha_host,
            args.talkha_runtime,
            reload_plan_for_mutation("upsert-automation"),
            exc,
        )
        raise


def cmd_delete_automation(args: argparse.Namespace, txm: TxManager) -> int:
    autos_before = load_automations(args.automations_file)
    target = (args.target or "").strip()
    if not target:
        raise TalkHaLokalError("Provide --target for delete-automation")

    tx = txm.start(
        "delete-automation",
        {
            "file": str(args.automations_file),
            "target": target,
            "match_by": args.match_by,
            "backup_dir": str(args.backup_dir),
        },
    )
    txm.set_phase(tx, "started")
    try:
        with file_mutex(txm.state_dir, f"mutation_{args.automations_file.name}"):
            txm.set_phase(tx, "locked")
            txm.backup_files(tx, [args.automations_file], args.backup_dir)
            txm.set_phase(tx, "backed_up")

            matches = find_automation_matches(autos_before, target, args.match_by)
            if len(matches) > 1:
                txm.finish(tx, "error", f"Ambiguous automation target: {target}")
                raise TalkHaLokalError(f"Ambiguous automation target: {target}")
            if not matches:
                txm.finish(tx, "error", f"Automation target not found: {target}")
                raise TalkHaLokalError(f"Automation target not found: {target}")

            target_index = matches[0]
            removed = autos_before[target_index]
            removed_alias = str(removed.get("alias", "")).strip()
            removed_id = str(removed.get("id", "")).strip()

            delete_automation_block_text(args.automations_file, target_index)
            txm.set_phase(tx, "written")

            autos_after = load_automations(args.automations_file)
            try:
                verify_automation_delete_integrity(autos_before, autos_after, target_index)
            except Exception as exc:
                restored = txm.rollback_tx_ctx(tx)
                msg = f"Integrity check failed after automation delete; rollback restored {restored} file(s): {exc}"
                txm.set_check(tx, "integrity", {"ok": False, "error": str(exc)})
                txm.set_rollback(tx, {"reason": "integrity_failed", "restored_files": restored})
                txm.finish(tx, "error", msg)
                raise TalkHaLokalError(msg)
            txm.set_check(tx, "integrity", {"ok": True, "changed_only_target": True, "removed": True})
            txm.set_phase(tx, "integrity_ok")

            core_check = run_ha_core_check(args.ha_host)
            txm.set_check(tx, "ha_core_check", core_check)
            if not core_check["ok"]:
                restored = rollback_with_restore(
                    tx,
                    txm,
                    args.ha_host,
                    args.talkha_runtime,
                    reload_plan_for_mutation("upsert-automation"),
                    "ha_core_check_failed",
                )
                msg = f"ha core check failed after automation delete; rollback restored {restored} file(s)"
                txm.finish(tx, "error", msg)
                raise TalkHaLokalError(msg)
            txm.set_phase(tx, "ha_core_check_ok")

            reload_result = run_reload_plan(tx, txm, args.talkha_runtime, reload_plan_for_mutation("upsert-automation"))
            txm.set_check(tx, "reload", reload_result)
            if not reload_result["ok"]:
                restored = rollback_with_restore(
                    tx,
                    txm,
                    args.ha_host,
                    args.talkha_runtime,
                    reload_plan_for_mutation("upsert-automation"),
                    "reload_failed",
                )
                msg = f"automation reload failed after delete; rollback restored {restored} file(s)"
                txm.finish(tx, "error", msg)
                raise TalkHaLokalError(msg)
            txm.set_phase(tx, "reload_ok")

            txm.note(tx, f"automation deleted alias={removed_alias} id={removed_id}")
            txm.finish(tx, "ok", "automation delete applied")
            print(json.dumps(success_payload(txm, tx, {"removed_alias": removed_alias, "removed_id": removed_id}), ensure_ascii=False))
            return 0
    except BaseException as exc:
        finalize_interrupted_mutation(
            tx,
            txm,
            args.ha_host,
            args.talkha_runtime,
            reload_plan_for_mutation("upsert-automation"),
            exc,
        )
        raise


def cmd_upsert_script(args: argparse.Namespace, txm: TxManager) -> int:
    scripts_before = load_scripts(args.scripts_file)
    scripts = dict(scripts_before)
    block = resolve_block_arg(args.block_file, args.block_base64)
    key = (args.key or "").strip()
    target = (args.target or "").strip() or str(block.get("alias", "")).strip()
    if not key and not target:
        raise TalkHaLokalError("Provide --key for add, or --target/alias for replace")

    tx = txm.start(
        "upsert-script",
        {
            "file": str(args.scripts_file),
            "key": key,
            "target": target,
            "match_by": args.match_by,
            "backup_dir": str(args.backup_dir),
        },
    )
    txm.set_phase(tx, "started")
    try:
        with file_mutex(txm.state_dir, f"mutation_{args.scripts_file.name}"):
            txm.set_phase(tx, "locked")
            txm.backup_files(tx, [args.scripts_file], args.backup_dir)
            txm.set_phase(tx, "backed_up")

            target_key = key
            if not target_key and target:
                matches = find_script_keys(scripts, target, args.match_by)
                if len(matches) > 1:
                    txm.finish(tx, "error", f"Ambiguous script target: {target}")
                    raise TalkHaLokalError(f"Ambiguous script target: {target}")
                if len(matches) == 1:
                    target_key = matches[0]
                else:
                    txm.finish(tx, "error", "Script target not found and --key not provided")
                    raise TalkHaLokalError("Script target not found. Use --key to add a new script.")

            existed = target_key in scripts
            scripts[target_key] = block
            if existed:
                replace_script_block_text(args.scripts_file, target_key, block)
            else:
                append_script_block_text(args.scripts_file, target_key, block)
            txm.set_phase(tx, "written")

            scripts_after = load_scripts(args.scripts_file)
            try:
                verify_script_integrity(before=scripts_before, after=scripts_after, target_key=target_key, existed=existed)
            except Exception as exc:
                restored = txm.rollback_tx_ctx(tx)
                msg = f"Integrity check failed after script write; rollback restored {restored} file(s): {exc}"
                txm.set_check(tx, "integrity", {"ok": False, "error": str(exc)})
                txm.set_rollback(tx, {"reason": "integrity_failed", "restored_files": restored})
                txm.finish(tx, "error", msg)
                raise TalkHaLokalError(msg)
            txm.set_check(tx, "integrity", {"ok": True, "changed_only_target": True})
            txm.set_phase(tx, "integrity_ok")

            core_check = run_ha_core_check(args.ha_host)
            txm.set_check(tx, "ha_core_check", core_check)
            if not core_check["ok"]:
                restored = rollback_with_restore(
                    tx,
                    txm,
                    args.ha_host,
                    args.talkha_runtime,
                    reload_plan_for_mutation("upsert-script"),
                    "ha_core_check_failed",
                )
                msg = f"ha core check failed after script write; rollback restored {restored} file(s)"
                txm.finish(tx, "error", msg)
                raise TalkHaLokalError(msg)
            txm.set_phase(tx, "ha_core_check_ok")

            reload_result = run_reload_plan(tx, txm, args.talkha_runtime, reload_plan_for_mutation("upsert-script"))
            txm.set_check(tx, "reload", reload_result)
            if not reload_result["ok"]:
                restored = rollback_with_restore(
                    tx,
                    txm,
                    args.ha_host,
                    args.talkha_runtime,
                    reload_plan_for_mutation("upsert-script"),
                    "reload_failed",
                )
                msg = f"script reload failed after write; rollback restored {restored} file(s)"
                txm.finish(tx, "error", msg)
                raise TalkHaLokalError(msg)
            txm.set_phase(tx, "reload_ok")
            txm.note(tx, f"script key={target_key} " + ("replaced" if existed else "added"))
            txm.finish(tx, "ok", "script upsert applied")
            print(json.dumps(success_payload(txm, tx, {"replaced": existed, "key": target_key}), ensure_ascii=False))
            return 0
    except BaseException as exc:
        finalize_interrupted_mutation(
            tx,
            txm,
            args.ha_host,
            args.talkha_runtime,
            reload_plan_for_mutation("upsert-script"),
            exc,
        )
        raise


def cmd_delete_script(args: argparse.Namespace, txm: TxManager) -> int:
    scripts_before = load_scripts(args.scripts_file)
    target = (args.target or "").strip()
    if not target:
        raise TalkHaLokalError("Provide --target for delete-script")

    tx = txm.start(
        "delete-script",
        {
            "file": str(args.scripts_file),
            "target": target,
            "match_by": args.match_by,
            "backup_dir": str(args.backup_dir),
        },
    )
    txm.set_phase(tx, "started")
    try:
        with file_mutex(txm.state_dir, f"mutation_{args.scripts_file.name}"):
            txm.set_phase(tx, "locked")
            txm.backup_files(tx, [args.scripts_file], args.backup_dir)
            txm.set_phase(tx, "backed_up")

            matches = find_script_keys(scripts_before, target, args.match_by)
            if len(matches) > 1:
                txm.finish(tx, "error", f"Ambiguous script target: {target}")
                raise TalkHaLokalError(f"Ambiguous script target: {target}")
            if not matches:
                txm.finish(tx, "error", f"Script target not found: {target}")
                raise TalkHaLokalError(f"Script target not found: {target}")

            target_key = matches[0]
            removed = scripts_before[target_key]
            removed_alias = str(removed.get("alias", "")).strip()

            delete_script_block_text(args.scripts_file, target_key)
            txm.set_phase(tx, "written")

            scripts_after = load_scripts(args.scripts_file)
            try:
                verify_script_delete_integrity(scripts_before, scripts_after, target_key)
            except Exception as exc:
                restored = txm.rollback_tx_ctx(tx)
                msg = f"Integrity check failed after script delete; rollback restored {restored} file(s): {exc}"
                txm.set_check(tx, "integrity", {"ok": False, "error": str(exc)})
                txm.set_rollback(tx, {"reason": "integrity_failed", "restored_files": restored})
                txm.finish(tx, "error", msg)
                raise TalkHaLokalError(msg)
            txm.set_check(tx, "integrity", {"ok": True, "changed_only_target": True, "removed": True})
            txm.set_phase(tx, "integrity_ok")

            core_check = run_ha_core_check(args.ha_host)
            txm.set_check(tx, "ha_core_check", core_check)
            if not core_check["ok"]:
                restored = rollback_with_restore(
                    tx,
                    txm,
                    args.ha_host,
                    args.talkha_runtime,
                    reload_plan_for_mutation("upsert-script"),
                    "ha_core_check_failed",
                )
                msg = f"ha core check failed after script delete; rollback restored {restored} file(s)"
                txm.finish(tx, "error", msg)
                raise TalkHaLokalError(msg)
            txm.set_phase(tx, "ha_core_check_ok")

            reload_result = run_reload_plan(tx, txm, args.talkha_runtime, reload_plan_for_mutation("upsert-script"))
            txm.set_check(tx, "reload", reload_result)
            if not reload_result["ok"]:
                restored = rollback_with_restore(
                    tx,
                    txm,
                    args.ha_host,
                    args.talkha_runtime,
                    reload_plan_for_mutation("upsert-script"),
                    "reload_failed",
                )
                msg = f"script reload failed after delete; rollback restored {restored} file(s)"
                txm.finish(tx, "error", msg)
                raise TalkHaLokalError(msg)
            txm.set_phase(tx, "reload_ok")

            txm.note(tx, f"script deleted key={target_key} alias={removed_alias}")
            txm.finish(tx, "ok", "script delete applied")
            print(json.dumps(success_payload(txm, tx, {"removed_key": target_key, "removed_alias": removed_alias}), ensure_ascii=False))
            return 0
    except BaseException as exc:
        finalize_interrupted_mutation(
            tx,
            txm,
            args.ha_host,
            args.talkha_runtime,
            reload_plan_for_mutation("upsert-script"),
            exc,
        )
        raise


def normalize_helper_id(kind: str, value: str) -> str:
    raw = value.strip()
    prefix = f"{kind}."
    if raw.startswith(prefix):
        return raw[len(prefix) :]
    if "." in raw:
        domain, obj = raw.split(".", 1)
        if domain != kind:
            raise TalkHaLokalError(f"Helper domain mismatch: expected {kind}, got {domain}")
        return obj
    return raw


def cmd_helper_upsert(args: argparse.Namespace, txm: TxManager) -> int:
    helper_id = normalize_helper_id(args.kind, args.helper)

    item = json.loads(args.item_json)
    if not isinstance(item, dict):
        raise TalkHaLokalError("--item-json must be JSON object")
    item["id"] = helper_id
    validate_helper_item(args.kind, item)

    tx = txm.start(
        "helper-upsert",
        {"kind": args.kind, "helper": helper_id, "backup_dir": str(args.backup_dir)},
    )
    txm.set_phase(tx, "started")
    try:
        with file_mutex(txm.state_dir, f"mutation_helper_{args.kind}"):
            txm.set_phase(tx, "locked")
            txm.backup_files(
                tx,
                [
                    get_helper_path(args.storage_dir, args.kind),
                    args.storage_dir / "core.entity_registry",
                ],
                args.backup_dir,
            )
            txm.set_phase(tx, "backed_up")
            result = run_external(
                [
                    "python3",
                    str(args.talkha_runtime),
                    "helper-upsert",
                    "--kind",
                    args.kind,
                    "--helper",
                    f"{args.kind}.{helper_id}",
                    "--item-json",
                    json.dumps(helper_payload_without_id(item), ensure_ascii=False),
                    "--explicit-confirm",
                    "REQUIRED",
                ]
            )
            txm.add_runtime_action(tx, {"type": "talkha_helper_upsert", **result})
            txm.set_check(tx, "talkha_helper_upsert", result)
            if not result["ok"]:
                txm.finish(tx, "error", "TalkHa.py helper-upsert failed")
                raise TalkHaLokalError(result["stderr"] or result["stdout"] or "TalkHa.py helper-upsert failed")
            txm.set_phase(tx, "runtime_ok")

            list_result = run_external(
                [
                    "python3",
                    str(args.talkha_runtime),
                    "helper-list",
                    "--kind",
                    args.kind,
                    "--as-json",
                ]
            )
            txm.set_check(tx, "helper_list", list_result)
            if not list_result["ok"]:
                txm.finish(tx, "error", "TalkHa.py helper-list failed after upsert")
                raise TalkHaLokalError(list_result["stderr"] or list_result["stdout"] or "TalkHa.py helper-list failed")
            try:
                listed = json.loads(list_result["stdout"])
            except Exception as exc:
                txm.finish(tx, "error", f"Invalid JSON from helper-list: {exc}")
                raise TalkHaLokalError(f"Invalid JSON from helper-list: {exc}") from exc
            entity_id = f"{args.kind}.{helper_id}"
            found = any(isinstance(row, dict) and str(row.get("entity_id", "")) == entity_id for row in listed)
            integrity = {"ok": found, "changed_only_target": True, "entity_id": entity_id}
            txm.set_check(tx, "integrity", integrity)
            if not found:
                txm.finish(tx, "error", f"Helper not visible in runtime after upsert: {entity_id}")
                raise TalkHaLokalError(f"Helper not visible in runtime after upsert: {entity_id}")
            txm.set_phase(tx, "integrity_ok")

            txm.finish(tx, "ok", "helper upsert applied via TalkHa.py GUI/API path")
            print(json.dumps(success_payload(txm, tx, {"replaced": None, "kind": args.kind, "helper": helper_id, "entity_id": entity_id}), ensure_ascii=False))
            return 0
    except BaseException as exc:
        finalize_interrupted_mutation(
            tx,
            txm,
            args.ha_host,
            args.talkha_runtime,
            [],
            exc,
        )
        raise


def cmd_tx_report(args: argparse.Namespace, txm: TxManager) -> int:
    data = txm.load_tx_by_id(args.tx_id)
    print(json.dumps(data, ensure_ascii=False, indent=2))
    return 0


def cmd_rollback(args: argparse.Namespace, txm: TxManager) -> int:
    data = txm.load_tx_by_id(args.tx_id)
    backups = data.get("backups", [])
    if not backups:
        raise TalkHaLokalError("Transaction has no backups")
    for row in backups:
        src = Path(row["source"])
        bak = Path(row["backup"])
        if not bak.exists():
            raise TalkHaLokalError(f"Missing backup file: {bak}")
        shutil.copy2(bak, src)
    print(json.dumps({"tx_id": args.tx_id, "status": "rolled_back", "files": len(backups)}, ensure_ascii=False))
    return 0


def cmd_snapshot(args: argparse.Namespace) -> int:
    topic = args.topic.strip().lower()
    autos = load_automations(args.automations_file)
    scripts = load_scripts(args.scripts_file)

    selected_autos = []
    selected_scripts = []

    for a in autos:
        if topic and topic not in json.dumps(a, ensure_ascii=False).lower():
            continue
        selected_autos.append(
            {
                "id": str(a.get("id", "")),
                "alias": str(a.get("alias", "")),
                "enabled": automation_enabled(a),
                "entities": extract_entity_ids(a)[:20],
                "trigger_keys": sorted(list((a.get("trigger") or a.get("triggers") or [{}])[0].keys())) if (a.get("trigger") or a.get("triggers")) else [],
            }
        )
        if len(selected_autos) >= args.limit:
            break

    for key, body in scripts.items():
        txt = json.dumps(body, ensure_ascii=False).lower()
        if topic and topic not in txt and topic not in key.lower():
            continue
        selected_scripts.append(
            {
                "key": key,
                "alias": str(body.get("alias", "")),
                "mode": str(body.get("mode", "")),
                "entities": extract_entity_ids(body)[:20],
            }
        )
        if len(selected_scripts) >= args.limit:
            break

    entity_set = set()
    for row in selected_autos:
        entity_set.update(row["entities"])
    for row in selected_scripts:
        entity_set.update(row["entities"])

    payload = {
        "topic": args.topic,
        "automations": selected_autos,
        "scripts": selected_scripts,
        "entities": sorted(entity_set)[: args.entity_limit],
        "summary": {
            "automations_selected": len(selected_autos),
            "scripts_selected": len(selected_scripts),
            "entities_selected": min(len(entity_set), args.entity_limit),
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_get_script(args: argparse.Namespace) -> int:
    scripts = load_scripts(args.scripts_file)
    target = (args.target or "").strip()
    if not target:
        raise TalkHaLokalError("Provide --target for get-script")

    matches = find_script_keys(scripts, target, args.match_by)
    if len(matches) > 1:
        raise TalkHaLokalError(f"Ambiguous script target: {target}")
    if not matches:
        raise TalkHaLokalError(f"Script target not found: {target}")

    key = matches[0]
    body = scripts[key]
    payload = {
        "key": key,
        "alias": str(body.get("alias", "")),
        "mode": str(body.get("mode", "")),
        "yaml": render_script_block(key, body),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_get_automation(args: argparse.Namespace) -> int:
    autos = load_automations(args.automations_file)
    target = (args.target or "").strip()
    if not target:
        raise TalkHaLokalError("Provide --target for get-automation")

    matches = find_automation_matches(autos, target, args.match_by)
    if len(matches) > 1:
        raise TalkHaLokalError(f"Ambiguous automation target: {target}")
    if not matches:
        raise TalkHaLokalError(f"Automation target not found: {target}")

    index = matches[0]
    block = autos[index]
    payload = {
        "id": str(block.get("id", "")),
        "alias": str(block.get("alias", "")),
        "enabled": automation_enabled(block),
        "yaml": render_automation_block(block),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_investigate(args: argparse.Namespace) -> int:
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


def cmd_event_timeline(args: argparse.Namespace) -> int:
    result = run_event_timeline(
        entities=args.entities,
        from_time=args.from_time,
        to_time=args.to_time,
        limit=args.limit,
        automations_file=args.automations_file,
        scripts_file=args.scripts_file,
        ha_base_url=args.ha_base_url,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="TalkHaLokal local HA operator")
    p.add_argument("--base-dir", type=Path, default=DEFAULT_BASE_DIR)
    p.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    p.add_argument("--log-file", type=Path, default=DEFAULT_LOG_FILE)
    p.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
    p.add_argument("--automations-file", type=Path, default=DEFAULT_AUTOMATIONS_FILE)
    p.add_argument("--scripts-file", type=Path, default=DEFAULT_SCRIPTS_FILE)
    p.add_argument("--storage-dir", type=Path, default=DEFAULT_STORAGE_DIR)
    p.add_argument("--lovelace-file", type=Path, default=DEFAULT_LOVELACE_FILE)
    p.add_argument("--ha-host", default=DEFAULT_HA_HOST)
    p.add_argument("--ha-base-url", default="http://192.168.2.70:8123/api")
    p.add_argument("--talkha-runtime", type=Path, default=DEFAULT_TALKHA_RUNTIME)

    sub = p.add_subparsers(dest="cmd", required=True)

    s_scan = sub.add_parser("scan", help="Scan automations/scripts/helpers")
    s_scan.add_argument("--compact", action="store_true")
    s_scan.add_argument("--limit", type=int, default=30)

    s_find = sub.add_parser("find", help="Find by query")
    s_find.add_argument("--query", required=True)

    s_snap = sub.add_parser("snapshot", help="Compact topic snapshot for LLM")
    s_snap.add_argument("--topic", default="")
    s_snap.add_argument("--limit", type=int, default=25)
    s_snap.add_argument("--entity-limit", type=int, default=120)

    s_gs = sub.add_parser("get-script", help="Get full script block by key or alias")
    s_gs.add_argument("--target", required=True)
    s_gs.add_argument("--match-by", choices=["key", "alias", "key-or-alias"], default="key-or-alias")

    s_ga = sub.add_parser("get-automation", help="Get full automation block by id or alias")
    s_ga.add_argument("--target", required=True)
    s_ga.add_argument("--match-by", choices=["id", "alias", "id-or-alias"], default="id-or-alias")

    s_inv = sub.add_parser("investigate", help="Compact read-only investigation for automations/scripts/entities")
    s_inv.add_argument("--query", required=True)
    s_inv.add_argument("--from-time", default="")
    s_inv.add_argument("--to-time", default="")
    s_inv.add_argument("--trace-limit", type=int, default=3)
    s_inv.add_argument("--state-limit", type=int, default=12)
    s_inv.add_argument("--tx-limit", type=int, default=5)

    s_evt = sub.add_parser("przebieg-zdarzen-ha", help="Compact HA event timeline for selected entities")
    s_evt.add_argument("--entities", nargs="+", required=True, help="Entity ids or comma-separated entity ids")
    s_evt.add_argument("--from-time", required=True)
    s_evt.add_argument("--to-time", default="")
    s_evt.add_argument("--limit", type=int, default=120)

    s_lf = sub.add_parser("lovelace-find-card", help="Find Lovelace cards by exact title in storage dashboard")
    s_lf.add_argument("--dashboard-file", type=Path, default=DEFAULT_LOVELACE_FILE)
    s_lf.add_argument("--title", required=True)
    s_lf.add_argument("--limit", type=int, default=10)

    s_lr = sub.add_parser("lovelace-replace-entities-in-card", help="Replace entity ids only inside one Lovelace card matched by exact title")
    s_lr.add_argument("--dashboard-file", type=Path, default=DEFAULT_LOVELACE_FILE)
    s_lr.add_argument("--title", required=True)
    s_lr.add_argument("--mapping-json", required=True, help='JSON object: {"old.entity":"new.entity"}')
    s_lr.add_argument("--backup-dir", type=Path, required=True)

    s_upa = sub.add_parser("upsert-automation", help="Replace/add automation by alias/id")
    s_upa.add_argument("--block-file", type=Path)
    s_upa.add_argument("--block-base64", default="", help="Base64-encoded YAML block")
    s_upa.add_argument("--target", help="Match target (alias or id)")
    s_upa.add_argument("--match-by", choices=["id", "alias", "id-or-alias"], default="id-or-alias")
    s_upa.add_argument("--allow-id-change", action="store_true")
    s_upa.add_argument("--allow-add", action="store_true")
    s_upa.add_argument("--backup-dir", type=Path, required=True)

    s_da = sub.add_parser("delete-automation", help="Delete one automation by alias/id")
    s_da.add_argument("--target", required=True, help="Match target (alias or id)")
    s_da.add_argument("--match-by", choices=["id", "alias", "id-or-alias"], default="alias")
    s_da.add_argument("--backup-dir", type=Path, required=True)

    s_ups = sub.add_parser("upsert-script", help="Replace/add script by alias/key")
    s_ups.add_argument("--key", help="Script key (required only when adding new)")
    s_ups.add_argument("--target", help="Match target (alias or key)")
    s_ups.add_argument("--match-by", choices=["key", "alias", "key-or-alias"], default="key-or-alias")
    s_ups.add_argument("--block-file", type=Path)
    s_ups.add_argument("--block-base64", default="", help="Base64-encoded YAML block")
    s_ups.add_argument("--backup-dir", type=Path, required=True)

    s_ds = sub.add_parser("delete-script", help="Delete one script by alias/key")
    s_ds.add_argument("--target", required=True, help="Match target (alias or key)")
    s_ds.add_argument("--match-by", choices=["key", "alias", "key-or-alias"], default="alias")
    s_ds.add_argument("--backup-dir", type=Path, required=True)

    s_hu = sub.add_parser("helper-upsert", help="Create/update GUI helper via TalkHa.py WebSocket/API path")
    s_hu.add_argument("--kind", required=True, choices=sorted(HELPER_FILES.keys()))
    s_hu.add_argument("--helper", required=True)
    s_hu.add_argument("--item-json", required=True)
    s_hu.add_argument("--backup-dir", type=Path, required=True)

    s_tr = sub.add_parser("tx-report", help="Show transaction report")
    s_tr.add_argument("--tx-id", required=True)

    s_rb = sub.add_parser("rollback", help="Restore files from transaction backup")
    s_rb.add_argument("--tx-id", required=True)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    txm = TxManager(state_dir=args.state_dir, log_file=args.log_file, backup_root=args.backup_root)

    try:
        if args.cmd == "scan":
            return cmd_scan(args)
        if args.cmd == "find":
            return cmd_find(args)
        if args.cmd == "snapshot":
            return cmd_snapshot(args)
        if args.cmd == "get-script":
            return cmd_get_script(args)
        if args.cmd == "get-automation":
            return cmd_get_automation(args)
        if args.cmd == "investigate":
            return cmd_investigate(args)
        if args.cmd == "przebieg-zdarzen-ha":
            return cmd_event_timeline(args)
        if args.cmd == "lovelace-find-card":
            return cmd_lovelace_find_card(args)
        if args.cmd == "lovelace-replace-entities-in-card":
            return cmd_lovelace_replace_entities_in_card(args, txm)
        if args.cmd == "upsert-automation":
            return cmd_upsert_automation(args, txm)
        if args.cmd == "delete-automation":
            return cmd_delete_automation(args, txm)
        if args.cmd == "upsert-script":
            return cmd_upsert_script(args, txm)
        if args.cmd == "delete-script":
            return cmd_delete_script(args, txm)
        if args.cmd == "helper-upsert":
            return cmd_helper_upsert(args, txm)
        if args.cmd == "tx-report":
            return cmd_tx_report(args, txm)
        if args.cmd == "rollback":
            return cmd_rollback(args, txm)
        raise TalkHaLokalError(f"Unsupported command: {args.cmd}")
    except TalkHaLokalError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"ERROR: Unexpected failure: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
