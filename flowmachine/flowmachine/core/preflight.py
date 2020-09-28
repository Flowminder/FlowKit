import inspect
from collections import defaultdict
from functools import wraps

import typing

from flowmachine.core.errors.flowmachine_errors import QueryErroredException


def pre_flight(method):
    method.__hooks__ = getattr(method, "__hooks__", {})
    method.__hooks__["pre_flight"] = method

    @wraps(method)
    def _impl(self):
        return method(self)

    return _impl


def resolve_hooks(cls) -> typing.Dict[str, typing.List[typing.Callable]]:
    """Add in the decorated processors
    By doing this after constructing the class, we let standard inheritance
    do all the hard work.
    """
    mro = inspect.getmro(cls)

    hooks = defaultdict(list)

    for attr_name in dir(cls):
        # Need to look up the actual descriptor, not whatever might be
        # bound to the class. This needs to come from the __dict__ of the
        # declaring class.
        for parent in mro:
            try:
                attr = parent.__dict__[attr_name]
            except KeyError:
                continue
            else:
                break
        else:
            # In case we didn't find the attribute and didn't break above.
            # We should never hit this - it's just here for completeness
            # to exclude the possibility of attr being undefined.
            continue

        try:
            hook_config = attr.__hook__
        except AttributeError:
            pass
        else:
            for key in hook_config.keys():
                # Use name here so we can get the bound method later, in
                # case the processor was a descriptor or something.
                hooks[key].append(attr_name)

    return hooks


class Preflight:
    def preflight(self):
        errors = []
        for dependency in [*self.dependencies, self]:
            for hook in resolve_hooks(dependency)["preflight"]:
                try:
                    hook()
                except Exception as e:
                    errors.append(e)
        if len(errors) > 0:
            raise QueryErroredException(
                f"Pre-flight failed for '{self.query_id}'. Errors: {errors}"
            )
