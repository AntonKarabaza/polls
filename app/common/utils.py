import datetime
import decimal
import json
from typing import Any, Optional


class PythonObjectEncoder(json.JSONEncoder):
    """Custom Python to JSON encoder."""
    def default(self, obj: Any) -> Any:
        if isinstance(obj, (list, dict, str, int, float, bool, type(None))):
            return super(PythonObjectEncoder, self).default(obj)
        elif isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, datetime.datetime):
            return str(obj)


def to_json(obj: Any, indent: Optional[int] = 2) -> str:
    """Serialize obj to a JSON formatted string.

    :param obj: Some object.
    :type obj: Any.
    :param indent: Indent of a new line in JSON formatted string.
    :type indent: Optional[int].
    :return: JSON formatted string.
    :rtype: str.
    """
    return json.dumps(obj, cls=PythonObjectEncoder, indent=indent)
