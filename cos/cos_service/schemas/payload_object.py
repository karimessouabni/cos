"""Objets optionnels d'un payload (``retention``, ``backup`` …).

Un objet JSON envoyé vide vaut « non fourni » : il ne demande rien, et le reste
du payload s'applique comme s'il était absent. C'est ce que le provider
Terraform envoie quand le client ne renseigne pas le bloc (``{}`` ou tous les
attributs à null).
"""


def provided(obj):
    """L'objet tel quel s'il porte au moins une valeur, sinon None.

    Vide : ``{}``, ou un objet dont tous les champs envoyés valent null. Un champ
    envoyé avec une valeur, même ``false``, est une demande : l'objet est gardé.
    """
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj if any(value is not None for value in obj.values()) else None
    fields_set = getattr(obj, "model_fields_set", None)  # modèle pydantic : les champs envoyés
    if fields_set is not None:
        return obj if any(getattr(obj, name) is not None for name in fields_set) else None
    is_empty = getattr(obj, "is_empty", None)
    if callable(is_empty):
        return None if is_empty() else obj
    return obj if any(value is not None for value in vars(obj).values()) else None
