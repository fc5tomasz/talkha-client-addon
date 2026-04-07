from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
from typing import Any

import aiohttp


TALKHA = os.environ.get("TALKHA_RUNTIME_PATH", "/opt/talkha/runtime/TalkHa.py")
TALKHALOKAL = os.environ.get("TALKHALOKAL_RUNTIME_PATH", "/opt/talkha/runtime/TalkHaLokal.py")
OPERATOR_URL = os.environ["TALKHA_OPERATOR_URL"].rstrip("/")
CLIENT_ID = os.environ["TALKHA_CLIENT_ID"]
REGISTRATION_TOKEN = os.environ["TALKHA_REGISTRATION_TOKEN"]
MODE = os.environ.get("TALKHA_MODE", "full")
ALLOW_MUTATIONS = os.environ.get("TALKHA_ALLOW_MUTATIONS", "true").lower() == "true"
POLL_INTERVAL = int(os.environ.get("TALKHA_POLL_INTERVAL", "10"))


def _env() -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["TALKHA_LOCAL_DB"] = "1"
    return env


def _blocked(args: list[str]) -> bool:
    if ALLOW_MUTATIONS:
        return False
    blocked = {
        "helper-upsert",
        "helper-delete",
        "service-call",
        "replace-automation-block",
        "add-automation-block",
        "replace-script-block",
        "panel-replace",
        "upsert-automation",
        "delete-automation",
        "upsert-script",
        "delete-script",
        "lovelace-replace-entities-in-card",
        "rollback",
    }
    return any(arg in blocked for arg in args)


def _run(cmd: list[str]) -> dict[str, Any]:
    proc = subprocess.run(cmd, capture_output=True, text=True, env=_env())
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "cmd": cmd,
    }


def _runtime_defaults() -> list[str]:
    return [
        "--base-dir",
        "/config",
        "--state-dir",
        "/data/.talkhalokal_state",
        "--log-file",
        "/data/talkhalokal.log",
        "--backup-root",
        "/config/TalkHaBackup",
        "--automations-file",
        "/config/automations.yaml",
        "--scripts-file",
        "/config/scripts.yaml",
        "--storage-dir",
        "/config/.storage",
        "--lovelace-file",
        "/config/.storage/lovelace",
        "--talkha-runtime",
        TALKHA,
    ]


def run_job(job: dict[str, Any]) -> dict[str, Any]:
    job_type = job.get("type")
    args = job.get("args", [])
    if not isinstance(args, list) or not all(isinstance(item, str) for item in args):
        return {"ok": False, "error": "args must be string list"}
    if _blocked(args):
        return {"ok": False, "error": "mutations disabled"}

    if job_type == "talkha":
        return _run(["python3", TALKHA, *args])
    if job_type == "talkhalokal":
        return _run(["python3", TALKHALOKAL, *_runtime_defaults(), *args])
    return {"ok": False, "error": f"unsupported job type: {job_type}"}


async def register(session: aiohttp.ClientSession) -> dict[str, Any]:
    payload = {
        "client_id": CLIENT_ID,
        "registration_token": REGISTRATION_TOKEN,
        "mode": MODE,
        "allow_mutations": ALLOW_MUTATIONS,
        "hostname": socket.gethostname(),
        "capabilities": ["talkha", "talkhalokal"],
    }
    async with session.post(f"{OPERATOR_URL}/api/v1/register", json=payload) as resp:
        data = await resp.json(content_type=None)
        if resp.status != 200 or not data.get("ok"):
            raise RuntimeError(f"registration failed: {data}")
        return data


async def poll(session: aiohttp.ClientSession, session_token: str) -> dict[str, Any]:
    payload = {"client_id": CLIENT_ID, "session_token": session_token}
    async with session.post(f"{OPERATOR_URL}/api/v1/poll", json=payload) as resp:
        data = await resp.json(content_type=None)
        if resp.status != 200:
            raise RuntimeError(f"poll failed: {data}")
        return data


async def submit_result(
    session: aiohttp.ClientSession,
    session_token: str,
    job_id: str,
    result: dict[str, Any],
) -> None:
    payload = {
        "client_id": CLIENT_ID,
        "session_token": session_token,
        "job_id": job_id,
        "result": result,
    }
    async with session.post(f"{OPERATOR_URL}/api/v1/result", json=payload) as resp:
        data = await resp.json(content_type=None)
        if resp.status != 200 or not data.get("ok"):
            raise RuntimeError(f"submit result failed: {data}")


async def agent_loop() -> None:
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                registration = await register(session)
                session_token = registration["session_token"]
                poll_interval = int(registration.get("poll_interval", POLL_INTERVAL))
                print(
                    json.dumps(
                        {"ok": True, "client_id": CLIENT_ID, "registered": True, "operator_url": OPERATOR_URL},
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                while True:
                    payload = await poll(session, session_token)
                    job = payload.get("job")
                    if job:
                        result = run_job(job)
                        await submit_result(session, session_token, job["job_id"], result)
                    await asyncio.sleep(max(poll_interval, 2))
            except Exception as exc:
                print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), flush=True)
                await asyncio.sleep(max(POLL_INTERVAL, 5))


def main() -> None:
    asyncio.run(agent_loop())


if __name__ == "__main__":
    main()
