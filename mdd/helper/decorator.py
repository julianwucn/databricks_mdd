import logging
import inspect
from functools import wraps

class DecoratorUtils:
    @staticmethod
    def add_logger():
        """Class decorator to add a logger to the class."""
        def decorator(cls):
            cls.logger = logging.getLogger(f"mdd.{cls.__name__}")
            return cls
        return decorator

    @staticmethod
    def _get_clean_call_trace(self_obj, target_func: str, max_depth=10) -> str:
        """
        Returns a clean call trace (e.g., foo -> bar), showing only methods of the same instance
        and excluding the decorator wrapper itself.

        Args:
            self_obj: Instance (`self`) of the current class
            target_func: The actual function being decorated
            max_depth: Stack depth

        Returns:
            str: Clean trace string
        """
        trace = []
        stack = inspect.stack()

        for frame in stack[1:max_depth + 1]:
            frame_self = frame.frame.f_locals.get("self")
            func_name = frame.function

            # Keep only frames from this object
            if frame_self is self_obj:
                # Exclude decorator's internal `wrapper` frame
                if func_name != "wrapper":
                    trace.append(func_name)

        # Add the actual decorated function name at the end
        if not trace or trace[-1] != target_func:
            trace.append(target_func)

        return " -> ".join(trace) if trace else target_func

    @staticmethod
    def log_function(debug_attr: str = "debug"):
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                if getattr(self, debug_attr, False):
                    trace = DecoratorUtils._get_clean_call_trace(self_obj=self, target_func=func.__name__)
                    self.logger.debug(f"function start: {trace}")
                    result = func(self, *args, **kwargs)
                    self.logger.debug(f"function end: {trace}")
                    return result
                return func(self, *args, **kwargs)
            return wrapper
        return decorator
