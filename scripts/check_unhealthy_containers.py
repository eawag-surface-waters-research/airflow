#!/usr/bin/env python3
"""Email an alert when docker containers transition to/from unhealthy.

State is persisted between runs so repeated runs over the same incident do not
spam. Emails fire only on changes to the set of unhealthy containers (including
full recovery).
"""
from __future__ import annotations

import smtplib
import socket
import subprocess
import sys
from email.message import EmailMessage
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ENV_FILE = REPO / ".env"
STATE_FILE = Path("/var/tmp/alplakes_unhealthy_state")
DOCKER = "/usr/bin/docker"


def load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip("'\"")
    return env


def unhealthy_containers() -> list[str]:
    out = subprocess.check_output(
        [DOCKER, "ps", "--filter", "health=unhealthy", "--format", "{{.Names}}"],
        text=True,
    )
    return sorted(n for n in out.splitlines() if n)


def container_details() -> str:
    return subprocess.check_output(
        [DOCKER, "ps", "-a", "--filter", "health=unhealthy",
         "--format", "table {{.Names}}\t{{.Status}}\t{{.Image}}"],
        text=True,
    )


def send_email(env: dict[str, str], subject: str, body: str) -> None:
    to_addr = env.get("HEALTHCHECK_EMAIL_TO") or env["GMAIL_ADDRESS"]
    msg = EmailMessage()
    msg["From"] = f"Alplakes Healthcheck <{env['GMAIL_ADDRESS']}>"
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)
    with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as s:
        s.starttls()
        s.login(env["GMAIL_ADDRESS"], env["GMAIL_PASSWORD"])
        s.send_message(msg)


def main() -> int:
    env = load_env(ENV_FILE)
    current = unhealthy_containers()
    previous = sorted(STATE_FILE.read_text().split()) if STATE_FILE.exists() else []

    if current == previous:
        return 0

    if current:
        subject = f"[Alplakes] {len(current)} container(s) unhealthy: {', '.join(current)}"
        body = f"Unhealthy containers on {socket.gethostname()}:\n\n{container_details()}"
        send_email(env, subject, body)
    elif previous:
        subject = f"[Alplakes] All containers healthy (recovered: {', '.join(previous)})"
        body = "Previously unhealthy containers have recovered."
        send_email(env, subject, body)

    STATE_FILE.write_text("\n".join(current))
    return 0


if __name__ == "__main__":
    sys.exit(main())
