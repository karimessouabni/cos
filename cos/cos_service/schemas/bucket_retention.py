"""Schéma de rétention d'un bucket COS.

Chaque borne (``default``, ``minimum``, ``maximum``) est saisie dans une unité
explicite : ``*_days`` ou ``*_years``, une seule unité par demande. Le plafond
est de cinq ans, comparé dans l'unité saisie ; en jours il vaut l'équivalent
exact de cinq ans à la date de la demande (années bissextiles comprises).

Compatibilité : le format historique ``default`` / ``minimum`` / ``maximum``
sans suffixe (jours implicites) reste accepté. Ces trois champs sont déclarés
dans le modèle, donc visibles dans le contrat (schéma JSON, documentation de
l'orchestrateur) avec la mention "deprecated". Une fois validés, ils sont
recopiés vers ``*_days``, journalisés en warning, et jamais exportés. Ils
seront retirés à la date annoncée dans
docs/adr/0001-retention-unites-jours-annees.md. Le mélange des deux formats
est refusé.

Des bornes fournies sans ``retention_enabled`` activent la rétention ; un
``false`` explicite est conservé.
"""
import logging
from datetime import date

from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)

MAX_RETENTION_YEARS = 5
LEGACY_REMOVAL_DATE = "2027-03-31"  # voir docs/adr/0001-retention-unites-jours-annees.md

DAYS = "days"
YEARS = "years"
_UNITS = (DAYS, YEARS)
_RETENTION_KEYS = ("default", "minimum", "maximum")


def years_to_days(years: int, start: date | None = None) -> int:
    """Équivalent exact en jours de `years` années à partir de `start` (bissextiles comprises)."""
    start = start or date.today()
    return (start + relativedelta(years=years) - start).days


def max_retention_days(start: date | None = None) -> int:
    """Nombre exact de jours dans 5 ans à partir de `start` (bissextiles comprises).

    1826 ou 1827 selon le nombre de 29 février dans la fenêtre. Calculé à la
    date de la demande : la même saisie en jours peut donc être acceptée un
    jour et refusée un autre, à un jour près.
    """
    return years_to_days(MAX_RETENTION_YEARS, start)


def max_retention(unit: str) -> int:
    """Plafond dans l'unité saisie : 5 années, ou leur équivalent exact en jours."""
    return MAX_RETENTION_YEARS if unit == YEARS else max_retention_days()


def _legacy_field(key: str):
    """Champ du format historique : accepté sous son nom d'origine (alias),
    affiché "deprecated" dans le schéma, jamais exporté."""
    return Field(
        default=None,
        alias=key,
        exclude=True,
        description=(
            f"Deprecated (implicit days): use {key}_{DAYS} or {key}_{YEARS}. "
            f"Removed on {LEGACY_REMOVAL_DATE}."
        ),
        json_schema_extra={"deprecated": True},
    )


class BucketRetention(BaseModel):
    retention_enabled: bool = False
    default_days: int | None = None
    default_years: int | None = None
    minimum_days: int | None = None
    minimum_years: int | None = None
    maximum_days: int | None = None
    maximum_years: int | None = None
    # Format historique (déprécié) : visible dans le contrat sous ``default``,
    # ``minimum``, ``maximum``, recopié vers *_days par _accept_legacy_format.
    legacy_default: int | None = _legacy_field("default")
    legacy_minimum: int | None = _legacy_field("minimum")
    legacy_maximum: int | None = _legacy_field("maximum")
    # Posé par _accept_legacy_format quand le client a envoyé l'ancien format.
    # Jamais exporté (state, Terraform) : sert à remonter une notice de dépréciation.
    legacy_format: bool = Field(default=False, exclude=True)

    # -- helpers ------------------------------------------------------------
    @staticmethod
    def _unit_of(get) -> str | None:
        """Unité effectivement saisie ("days", "years") ou None si rien n'est saisi."""
        for unit in _UNITS:
            if any(get(f"{k}_{unit}") is not None for k in _RETENTION_KEYS):
                return unit
        return None

    # -- 1. Auto-enable si un paramètre est fourni sans le flag ---------------
    # Un ``retention_enabled`` à null (attribut Terraform optional non renseigné)
    # vaut "non fourni" : sinon le champ bool le refuserait.
    # Les validateurs "before" s'exécutent du dernier défini au premier : les
    # clés historiques sont donc regardées ici aussi, sans dépendre de l'ordre.
    @model_validator(mode="before")
    @classmethod
    def _auto_enable_retention(cls, values):
        if not isinstance(values, dict):
            return values
        if "retention_enabled" in values and values["retention_enabled"] is None:
            values = {k: v for k, v in values.items() if k != "retention_enabled"}
        retention_flag_given = "retention_enabled" in values
        any_retention_param = any(
            values.get(name) is not None
            for k in _RETENTION_KEYS
            for name in (k, *(f"{k}_{unit}" for unit in _UNITS))
        )
        if not retention_flag_given and any_retention_param:
            values["retention_enabled"] = True
        return values

    # -- 1 bis. Format historique (déprécié) : default/minimum/maximum = jours
    # Premier validateur "after" : les champs legacy sont déjà typés (int),
    # ils sont recopiés vers *_days puis le modèle ne raisonne que sur les
    # champs suffixés. Le mélange des deux formats est refusé.
    @model_validator(mode="after")
    def _accept_legacy_format(self) -> "BucketRetention":
        legacy = {k: v for k in _RETENTION_KEYS if (v := getattr(self, f"legacy_{k}")) is not None}
        if not legacy or self.legacy_format:  # rien de legacy, ou instance déjà convertie (revalidation)
            return self
        suffixed = [
            f"{k}_{unit}"
            for k in _RETENTION_KEYS
            for unit in _UNITS
            if getattr(self, f"{k}_{unit}") is not None
        ]
        if suffixed:
            raise ValueError(
                "Retention must use either the legacy fields (default, minimum, maximum) "
                f"or the unit-suffixed fields ({', '.join(suffixed)}), not both."
            )
        logger.warning(
            "Legacy retention payload (implicit days) received: %s. "
            "Use default_days / minimum_days / maximum_days (or *_years) instead.",
            legacy,
        )
        for k, v in legacy.items():
            setattr(self, f"{k}_{DAYS}", v)
        self.legacy_format = True
        return self

    # -- 2. Tous les contrôles du payload, erreurs accumulées -----------------
    # Un seul validateur "after" qui n'échoue qu'à la fin : le client reçoit
    # la liste complète des problèmes de son payload en une seule réponse.
    @model_validator(mode="after")
    def _validate(self) -> "BucketRetention":
        errors = []

        used = [unit for unit in _UNITS
                if any(getattr(self, f"{k}_{unit}") is not None for k in _RETENTION_KEYS)]
        if len(used) > 1:
            errors.append(
                "Retention must be set either in days (…_days) or in years (…_years) "
                "for all three attributes, not a mix of both."
            )

        for k in _RETENTION_KEYS:
            for unit in _UNITS:
                v = getattr(self, f"{k}_{unit}")
                if v is not None and v <= 0:
                    errors.append(f"{k}_{unit} must be superior to 0.")

        # Bornes et plafond : seulement quand l'unité est non ambiguë.
        if len(used) == 1:
            unit = used[0]
            limit = max_retention(unit)
            mn, df, mx = self.minimum, self.default, self.maximum

            for name, v in (("minimum", mn), ("default", df), ("maximum", mx)):
                if v is not None and v > limit:
                    errors.append(
                        f"{name}_{unit} ({v} {unit}) cannot be superior to "
                        f"{MAX_RETENTION_YEARS} years ({limit} {unit}, leap years included)."
                    )
            if mn is not None and mx is not None and mn > mx:
                errors.append("Retention minimum cannot be superior to maximum.")
            if df is not None:
                if mn is not None and df < mn:
                    errors.append("Retention default cannot be inferior to minimum.")
                if mx is not None and df > mx:
                    errors.append("Retention default cannot be superior to maximum.")

        if errors:
            raise ValueError(" | ".join(errors))
        return self

    # -- Lecture : l'unité saisie et les valeurs brutes -----------------------
    @property
    def unit(self) -> str | None:
        return self._unit_of(lambda name: getattr(self, name))

    def _value(self, key: str) -> int | None:
        unit = self.unit
        return getattr(self, f"{key}_{unit}") if unit else None

    @property
    def default(self) -> int | None:
        return self._value("default")

    @property
    def minimum(self) -> int | None:
        return self._value("minimum")

    @property
    def maximum(self) -> int | None:
        return self._value("maximum")

    @property
    def max_allowed(self) -> int | None:
        """Plafond applicable dans l'unité saisie : 5 années, ou leur équivalent en jours."""
        unit = self.unit
        return max_retention(unit) if unit else None

    def in_days(self, key: str) -> int | None:
        """Valeur de l'attribut normalisée en jours, quelle que soit l'unité saisie."""
        value = self._value(key)
        if value is None:
            return None
        return value if self.unit == DAYS else years_to_days(value)

    def __iter__(self):
        yield "unit", self.unit
        yield "default", self.default
        yield "minimum", self.minimum
        yield "maximum", self.maximum

    def is_empty(self) -> bool:
        return self.unit is None

    def to_cos_payload(self) -> dict:
        """Dict prêt pour le retention_rule Terraform, dans l'unité saisie par le client."""
        if self.is_empty():
            return {}
        return {k: v for k, v in self if v is not None}

    def as_sent(self) -> dict:
        """Bornes sous leur nom suffixé (``default_years`` …), pour écho dans le state client."""
        unit = self.unit
        if unit is None:
            return {}
        return {
            f"{k}_{unit}": getattr(self, f"{k}_{unit}")
            for k in _RETENTION_KEYS
            if getattr(self, f"{k}_{unit}") is not None
        }
