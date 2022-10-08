# Module for meta classes
import threading


class SingletonMeta(type):
    """Thread-safe implementation of Singleton."""

    def __init__(cls, *args, **kwargs):
        cls.__instance = None
        cls.__lock = threading.Lock()
        cls.get_instance = classmethod(lambda c: c.__instance)  # Global access point
        super().__init__(*args, **kwargs)

    def __call__(cls, *args, **kwargs):
        with cls.__lock:
            if not cls.__instance:
                cls.__instance = super().__call__(*args, **kwargs)
        return cls.__instance
