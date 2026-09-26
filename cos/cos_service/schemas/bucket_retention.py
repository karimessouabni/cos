"""Schéma de rétention d'un bucket COS.

Deux formats de payload sont acceptés :

* **Format courant** : chaque borne est saisie dans une unité explicite,
  ``default_days`` / ``default_years``, ``minimum_days`` / ``minimum_years``,
  ``maximum_days`` / ``maximum_years``. Une seule unité par demande.
* **Format historique (déprécié)** : ``default``, ``minimum``, ``maximum``,
  implicitement en jours. Il est accepté tel quel pour ne pas casser les
  clients existants, recopié vers ``*_days`` avant validation et signalé par
  un warning. Le mélange des deux formats est refusé.

Dans les deux formats, des bornes fournies sans ``retention_enabled`` activent
la rétention (``retention_enabled`` passe à True) ; un ``False`` explicite est
conservé.

Quel que soit le format d'entrée, la base et le Terraform interne ne
connaissent que des jours (``in_days``). L'unité saisie est exposée par la
propriété ``unit`` et les bornes dans cette unité par ``default``, ``minimum``
et ``maximum``.
"""

import logging
from typing import Any

from pydantic import BaseModel, PrivateAttr, model_validator

DAYS = "days"
YEARS = "years"
UNITS = (DAYS, YEARS)

MAX_RETENTION_YEARS = 5
DAYS_PER_YEAR = 365

_RETENTION_KEYS = ("default", "minimum", "maximum")

logger = logging.getLogger(__name__)


def max_retention(unit: str) -> int:
    """Plafond de rétention exprimé dans l'unité demandée."""
    if unit == YEARS:
        return MAX_RETENTION_YEARS
    return MAX_RETENTION_YEARS * DAYS_PER_YEAR


def to_days(value: int | None, unit: str) -> int | None:
    if value is None:
        return None
    return value * DAYS_PER_YEAR if unit == YEARS else value


class BucketRetention(BaseModel):
    retention_enabled: bool | None = None

    default_days: int | None = None
    minimum_days: int | None = None
    maximum_days: int | None = None

    default_years: int | None = None
    minimum_years: int | None = None
    maximum_years: int | None = None

    _legacy_format: bool = PrivateAttr(default=False)

    # --- compatibilité : format historique -----------------------------------
    @model_validator(mode="wrap")
    @classmethod
    def _accept_legacy_format(cls, data: Any, handler):
        """``default``/``minimum``/``maximum`` (jours implicites) -> ``*_days``.

        Ces clés ne sont pas des champs : elles sont absorbées ici, avant la
        validation des champs. À retirer à la date annoncée dans
        docs/adr/0001-retention-unites-jours-annees.md.
        """
        legacy = {}
        if isinstance(data, dict):
            legacy = {key: data[key] for key in _RETENTION_KEYS if data.get(key) is not None}
            if legacy:
                explicit = [
                    f"{key}_{unit}"
                    for key in _RETENTION_KEYS
                    for unit in UNITS
                    if data.get(f"{key}_{unit}") is not None
                ]
                if explicit:
                    raise ValueError(
                        "Retention must use either the legacy fields (default, minimum, maximum) "
                        f"or the unit-suffixed fields ({', '.join(explicit)}), not both."
                    )
                logger.warning(
                    "Legacy retention payload (implicit days) received: %s. "
                    "Use default_days / minimum_days / maximum_days (or *_years) instead.",
                    legacy,
                )
                data = {key: value for key, value in data.items() if key not in _RETENTION_KEYS}
                data.update({f"{key}_{DAYS}": value for key, value in legacy.items()})

        instance = handler(data)
        instance._legacy_format = bool(legacy)
        return instance

    # --- règles ----------------------------------------------------------------
    @model_validator(mode="after")
    def _validate(self) -> "BucketRetention":
        """Des bornes sans drapeau valent une demande de rétention.

        Un client qui envoie ``default_days`` sans ``retention_enabled`` veut
        évidemment une rétention : le drapeau passe à True. Un ``False``
        explicite est respecté (la demande sera déclinée en aval si un choix
        ``retention_*`` l'exige). Le mélange d'unités n'est pas refusé ici :
        un choix ``retention_daily`` / ``retention_yearly`` explicite tranche
        dans ``apply_choice_unit`` ; sans choix, ``unit`` vaut None et le
        service décline la demande.
        """
        if self.retention_enabled is None and not self.is_empty():
            logger.info("retention bounds given without retention_enabled: enabling retention")
            self.retention_enabled = True
        return self

    @property
    def legacy_format(self) -> bool:
        """True si le client a envoyé l'ancien format (jours implicites)."""
        return self._legacy_format

    # --- lecture ---------------------------------------------------------------
    def _units_present(self) -> set[str]:
        return {
            unit
            for key in _RETENTION_KEYS
            for unit in UNITS
            if getattr(self, f"{key}_{unit}") is not None
        }

    @property
    def unit(self) -> str | None:
        """Unité saisie : "days" ou "years". None si rien n'est saisi ou si les
        deux unités sont mélangées (les validations aval refusent alors la demande)."""
        units = self._units_present()
        if len(units) != 1:
            return None
        return next(iter(units))

    def value(self, key: str) -> int | None:
        """Borne ``key`` dans l'unité saisie (None si absente ou unité ambiguë)."""
        unit = self.unit
        if unit is None:
            return None
        return getattr(self, f"{key}_{unit}")

    @property
    def default(self) -> int | None:
        return self.value("default")

    @property
    def minimum(self) -> int | None:
        return self.value("minimum")

    @property
    def maximum(self) -> int | None:
        return self.value("maximum")

    def in_days(self, key: str) -> int | None:
        """Borne ``key`` convertie en jours, unité canonique de la base et du Terraform."""
        unit = self.unit
        if unit is None:
            return None
        return to_days(getattr(self, f"{key}_{unit}"), unit)

    def is_empty(self) -> bool:
        """Aucune borne saisie (le drapeau ``retention_enabled`` seul ne compte pas)."""
        return not self._units_present()

    def as_sent(self) -> dict:
        """Bornes telles que le client les a envoyées, pour écho dans le state.

        Format historique : les valeurs sont déjà sous ``default/minimum/maximum``
        en jours dans le state, rien à ajouter. Format courant : ``{key}_{unit}``.
        """
        if self._legacy_format:
            return {}
        return {
            f"{key}_{unit}": getattr(self, f"{key}_{unit}")
            for key in _RETENTION_KEYS
            for unit in UNITS
            if getattr(self, f"{key}_{unit}") is not None
        }
