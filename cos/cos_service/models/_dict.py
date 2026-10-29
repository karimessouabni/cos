"""Vues dict partagées par les modèles : toutes les colonnes, plus les relations demandées."""
from typing import Any, Iterable


def as_dict(row: Any) -> Any:
    """Relation en dict : ``to_dict()`` si le modèle en a un, sinon ``dict(row)`` ; None reste None."""
    if row is None:
        return None
    if isinstance(row, list):
        return [as_dict(item) for item in row]
    return row.to_dict() if hasattr(row, "to_dict") else dict(row)


def row_to_dict(row: Any, relations: Iterable[str] = ()) -> dict:
    data = {column.name: getattr(row, column.name) for column in row.__table__.columns}
    for name in relations:
        data[name] = as_dict(getattr(row, name))
    return data
