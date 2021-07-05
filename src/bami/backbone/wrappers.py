from functools import wraps
from typing import Type

from bami.backbone.payload import DataClassPayloadMixin


def data_wrapper(data_model_class: Type[DataClassPayloadMixin]):
    def decorator(func):
        @wraps(func)
        def wrapper(self, p, *unpacked):
            dm = data_model_class.from_payload(unpacked[0])
            return func(self, p, dm)

        return wrapper

    return decorator
