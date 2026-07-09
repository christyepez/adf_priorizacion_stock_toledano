from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class NotificationResult:
    status: str
    status_code: int | None = None
    error_message: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_emails(emails: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if emails is None:
        return []
    if isinstance(emails, (list, tuple)):
        raw_values = emails
    else:
        raw_values = str(emails).replace(";", ",").split(",")
    return [email.strip() for email in raw_values if str(email).strip()]


def notification_timestamp() -> str:
    return datetime.now(UTC).isoformat()


def build_notification_payload(
    *,
    emails: str | list[str] | tuple[str, ...] | None,
    message: str,
    name: str,
    process: str,
    run_id: str,
    status: str,
    subject: str,
    timestamp: str | None = None,
) -> dict[str, Any]:
    return {
        "emails": parse_emails(emails),
        "message": message,
        "name": name,
        "process": process,
        "runId": run_id,
        "status": (status or "").strip().upper(),
        "subject": subject,
        "timestamp": timestamp or notification_timestamp(),
    }


def read_notification_endpoint(dbutils: Any, secret_scope: str, notification_endpoint_secret: str) -> str:
    if not secret_scope:
        raise ValueError("secret_scope es obligatorio")
    if not notification_endpoint_secret:
        raise ValueError("notification_endpoint_secret es obligatorio")
    return dbutils.secrets.get(secret_scope, notification_endpoint_secret)


def post_json(endpoint: str, payload: dict[str, Any], timeout_seconds: int = 30) -> NotificationResult:
    if not endpoint:
        raise ValueError("endpoint de notificacion es obligatorio")

    body = json.dumps(payload).encode("utf-8")
    request = Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            return NotificationResult(status="success", status_code=getattr(response, "status", None))
    except HTTPError as exc:
        return NotificationResult(status="error", status_code=exc.code, error_message=str(exc))
    except URLError as exc:
        return NotificationResult(status="error", error_message=str(exc))


def send_notification(endpoint: str, payload: dict[str, Any]) -> NotificationResult:
    return post_json(endpoint, payload)
