from __future__ import print_function, division, absolute_import

from . import _ext, _reduction, _stdlib, _toolz
from ._ext import chained, repartition
from ._reduction import mean, var, std


rules = _stdlib.rules | _toolz.rules | _reduction.rules | _ext.rules
rules.add(lambda _, func, __: callable(func), lambda _, func, obj: func(obj))


def apply(transformation, obj):
    """Apply the given transformation to a ``dask.bag.Bag``."""
    return rules(obj, transformation)
