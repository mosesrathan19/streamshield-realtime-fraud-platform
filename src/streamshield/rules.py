"""Explainable, illustrative risk signals, not real financial fraud decisions."""
from __future__ import annotations
from datetime import datetime

REQUIRED = ("event_id", "transaction_id", "account_id", "merchant_id", "amount_cents", "currency", "country", "event_time", "event_type", "schema_version")


def validate(event: dict) -> None:
    if not isinstance(event, dict) or any(field not in event for field in REQUIRED):
        raise ValueError("missing required event fields")
    if any(not isinstance(event[k], str) or not event[k] for k in ("event_id", "transaction_id", "account_id", "merchant_id")):
        raise ValueError("invalid identifiers")
    if type(event["amount_cents"]) is not int or event["amount_cents"] <= 0:
        raise ValueError("amount_cents must be a positive integer")
    if event["currency"] != "USD" or event["event_type"] != "payment_authorized":
        raise ValueError("unsupported payment event")
    if event["schema_version"] != 1:
        raise ValueError("unsupported schema version")
    if not isinstance(event["country"], str) or len(event["country"]) != 2:
        raise ValueError("country must have 2 letters")
    try:
        stamp = datetime.fromisoformat(event["event_time"])
    except (TypeError, ValueError) as err:
        raise ValueError("invalid event_time") from err
    if stamp.tzinfo is None:
        raise ValueError("event_time must be timezone-aware")


def evaluate(event: dict) -> list[str]:
    """Stable per-event signals. Stateful velocity comes in a later streaming phase."""
    validate(event)
    reasons = []
    if event["amount_cents"] >= 250_000:
        reasons.append("HIGH_AMOUNT")
    if event["country"] == "ZZ":
        reasons.append("SYNTHETIC_COUNTRY_ANOMALY")
    return reasons
