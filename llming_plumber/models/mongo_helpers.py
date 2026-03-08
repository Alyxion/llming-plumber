from __future__ import annotations

from typing import Any

from bson import ObjectId
from pydantic import BaseModel


def doc_to_model[T: BaseModel](doc: dict[str, Any], model_cls: type[T]) -> T:
    """Convert a MongoDB document to a Pydantic model instance.

    Maps ``_id`` (ObjectId) to ``id`` (str) so that models can use a plain
    ``id: str`` field instead of dealing with ObjectId directly.
    """
    data = dict(doc)
    if "_id" in data:
        data["id"] = str(data.pop("_id"))
    return model_cls.model_validate(data)


def model_to_doc(model: BaseModel) -> dict[str, Any]:
    """Convert a Pydantic model instance to a MongoDB-ready dict.

    If the model has a non-empty ``id`` field the value is converted to
    ``_id: ObjectId``.  An empty or missing ``id`` is omitted so MongoDB
    will auto-generate an ``_id``.
    """
    data = model.model_dump(mode="python")
    id_value = data.pop("id", None)
    if id_value:
        data["_id"] = ObjectId(id_value)
    return data
