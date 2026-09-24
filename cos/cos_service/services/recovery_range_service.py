"""Choix du point de restauration et du recovery range qui le couvre.

Règle IBM (doc bvm-restore) : ``restore_point_in_time`` "must be between the
start and end times published for the RecoveryRange being restored from".
Le point est donc une entrée du client, et le range est choisi parce qu'il
contient ce point, pas parce qu'il est le plus récent.

Logique pure, sans appel réseau : les ranges sont ceux renvoyés par
``restore_service.list_recovery_ranges``.
"""
from datetime import datetime, timezone

# Format des dates dans l'API Backup Vault : "2024-06-04T12:00:00.000Z".
IBM_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class RecoveryRangeError(ValueError):
    """Point de restauration ou range invalide : à convertir en refus de la demande."""


def parse_time(value: str, field: str = "restore_point_in_time") -> datetime:
    """ISO 8601 -> datetime UTC aware. Une valeur sans fuseau est lue comme UTC."""
    if not value or not isinstance(value, str):
        raise RecoveryRangeError(f"{field} is required (ISO 8601, e.g. 2026-09-15T10:30:00Z)")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise RecoveryRangeError(f"{field} '{value}' is not a valid ISO 8601 date-time") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_time(moment: datetime) -> str:
    """datetime aware -> format attendu par l'API IBM, en UTC, millisecondes."""
    utc = moment.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def range_window(recovery_range: dict) -> tuple[datetime, datetime]:
    return (
        parse_time(recovery_range["range_start_time"], "range_start_time"),
        parse_time(recovery_range["range_end_time"], "range_end_time"),
    )


def contains(recovery_range: dict, moment: datetime) -> bool:
    start, end = range_window(recovery_range)
    return start <= moment <= end


def describe(recovery_range: dict) -> str:
    return (
        f"{recovery_range['recovery_range_id']} "
        f"[{recovery_range['range_start_time']} -> {recovery_range['range_end_time']}]"
    )


def select_recovery_range(ranges: list[dict], restore_point: datetime, recovery_range_id: str | None = None) -> dict:
    """Range qui couvre ``restore_point``.

    - ``recovery_range_id`` donné : ce range doit exister et contenir le point.
    - sinon : le range dont [start, end] contient le point ; s'il y en a
      plusieurs (chevauchement), le plus récemment créé.
    Lève ``RecoveryRangeError`` avec la liste des fenêtres disponibles sinon.
    """
    if not ranges:
        raise RecoveryRangeError("no recovery range exists for this bucket in this backup vault")

    available = ", ".join(describe(r) for r in ranges)

    if recovery_range_id is not None:
        wanted = next((r for r in ranges if r["recovery_range_id"] == recovery_range_id), None)
        if wanted is None:
            raise RecoveryRangeError(f"recovery_range_id {recovery_range_id} not found (available: {available})")
        if not contains(wanted, restore_point):
            raise RecoveryRangeError(
                f"restore_point_in_time {format_time(restore_point)} is outside recovery range {describe(wanted)}"
            )
        return wanted

    candidates = [r for r in ranges if contains(r, restore_point)]
    if not candidates:
        raise RecoveryRangeError(
            f"restore_point_in_time {format_time(restore_point)} is not covered by any recovery range "
            f"(available: {available})"
        )
    return max(candidates, key=lambda r: parse_time(r.get("range_create_time") or r["range_start_time"], "range_create_time"))
