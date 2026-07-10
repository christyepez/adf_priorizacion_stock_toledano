from priorizacion_stock_toledano.audit.audit_logger import (
    audit_table_identifier,
    build_audit_event,
    validate_audit_status,
)
from priorizacion_stock_toledano.audit.notifier import (
    build_notification_payload,
    parse_emails,
    read_notification_endpoint,
)


class FakeSecrets:
    def __init__(self):
        self.calls = []

    def get(self, scope, key):
        self.calls.append((scope, key))
        return "https://logic-app.example/secure"


class FakeDbutils:
    def __init__(self):
        self.secrets = FakeSecrets()


def test_parse_emails_accepts_comma_and_semicolon():
    assert parse_emails("a@example.com; b@example.com, c@example.com") == [
        "a@example.com",
        "b@example.com",
        "c@example.com",
    ]


def test_build_notification_payload_matches_adf_webactivity_contract():
    payload = build_notification_payload(
        emails="ops@example.com, data@example.com",
        message="Proceso finalizado",
        name="Priorizacion Stock Toledano",
        process="Modelo_Priorizacion_Stock",
        run_id="12345",
        status="SUCCEEDED",
        subject="OK Priorizacion Stock",
        timestamp="2026-07-09T20:00:00+00:00",
    )

    assert payload == {
        "emails": ["ops@example.com", "data@example.com"],
        "message": "Proceso finalizado",
        "name": "Priorizacion Stock Toledano",
        "process": "Modelo_Priorizacion_Stock",
        "runId": "12345",
        "status": "SUCCEEDED",
        "subject": "OK Priorizacion Stock",
        "timestamp": "2026-07-09T20:00:00+00:00",
    }


def test_read_notification_endpoint_uses_secret_scope():
    dbutils = FakeDbutils()

    endpoint = read_notification_endpoint(dbutils, "scope", "notification-endpoint")

    assert endpoint == "https://logic-app.example/secure"
    assert dbutils.secrets.calls == [("scope", "notification-endpoint")]


def test_validate_audit_status_accepts_required_statuses():
    assert validate_audit_status("started") == "STARTED"
    assert validate_audit_status("SUCCEEDED") == "SUCCEEDED"
    assert validate_audit_status("failed") == "FAILED"
    assert validate_audit_status("warning") == "WARNING"


def test_build_audit_event_normalizes_status_and_keeps_run_context():
    event = build_audit_event(
        ambiente="dev",
        process="Modelo_Priorizacion_Stock",
        run_id="run-1",
        status="started",
        name="Priorizacion Stock Toledano",
        message="Inicio",
        task="audit_start",
        timestamp="2026-07-09T20:00:00+00:00",
    )

    assert event.status == "STARTED"
    assert event.run_id == "run-1"
    assert event.task == "audit_start"
    assert event.as_dict()["message"] == "Inicio"


def test_audit_table_identifier_defaults_to_gold_atlas_table():
    assert audit_table_identifier("toledano_gold_dev", "atlas") == "toledano_gold_dev.atlas.audit_pipeline_events"
