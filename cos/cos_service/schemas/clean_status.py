"""États du nettoyage d'un bucket (colonne ``bucket.clean_status``).

Cycle avec période de grâce (docs/adr/0003-periode-de-grace-du-clean.md) :

    (aucun / success / failed / cancelled)
        --clean-->          SCHEDULED      bucket en quarantaine, exécution à ``clean_execute_at``
        SCHEDULED --date--> IN_PROGRESS    règle d'expiration posée, vidage en cours
        SCHEDULED --cancel-> CANCELLED     quarantaine levée, rien n'a été supprimé
        IN_PROGRESS -------> SUCCESS | FAILED
        FAILED    --cancel-> CANCELLED     levée de la quarantaine après un échec

Les valeurs ``in_progress`` / ``success`` / ``failed`` sont celles que la
colonne portait déjà (``Status``) : aucune migration de données.
"""
from enum import Enum


class CleanStatus(str, Enum):
    SCHEDULED = "scheduled"
    INPROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @classmethod
    def busy(cls) -> tuple["CleanStatus", ...]:
        """États pendant lesquels une nouvelle demande de clean est refusée."""
        return (cls.SCHEDULED, cls.INPROGRESS)
