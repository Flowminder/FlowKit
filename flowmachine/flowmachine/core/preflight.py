import inspect
from collections import defaultdict

import typing

import networkx as nx
import structlog

from flowmachine.core.dependency_graph import (
    get_dependency_links,
    _assemble_dependency_graph,
)
from flowmachine.core.errors.flowmachine_errors import (
    PreFlightFailedException,
)

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


def pre_flight(method):
    method.__hooks__ = getattr(method, "__hooks__", {})
    method.__hooks__["pre_flight"] = method
    return method


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
            hook_config = attr.__hooks__
        except AttributeError:
            pass
        else:
            for key in hook_config:
                # Use name here so we can get the bound method later, in
                # case the processor was a descriptor or something.
                hooks[key].append(attr_name)

    return hooks


class Preflight:
    def preflight(self):
        logger.debug("Starting pre-flight checks.", query=str(self))
        errors = dict()
        dep_graph = _assemble_dependency_graph(
            dependencies=get_dependency_links(self),
            attrs_func=lambda x: dict(query=x),
        )
        deps = [dep_graph.nodes[id]["query"] for id in nx.topological_sort(dep_graph)][
            ::-1
        ]

        for dependency in deps:
            for hook in resolve_hooks(dependency.__class__)["pre_flight"]:
                logger.debug(
                    "Running hook",
                    query=str(self),
                    hook=hook,
                    dependency=str(dependency),
                )
                try:
                    getattr(dependency, hook)()
                except Exception as e:
                    errors.setdefault(str(dependency), list()).append(e)
        if len(errors) > 0:
            logger.debug(
                "Pre-flight failed.",
                query=str(self),
                query_id=self.query_id,
                errors=errors,
            )
            raise PreFlightFailedException(self.query_id, errors)