from __future__ import print_function, division, absolute_import

import math

from ._util import RuleSet, eq, a


class reduction(object):
    """Represent a reduction suited for parallelization with `dask.bag`.
    """
    pass


def mean(l):
    """Calculate the mean of a list of values."""
    l = iter(l)

    s0 = 1
    try:
        s1 = next(l)

    except StopIteration:
        raise RuntimeError('cannot compute the mean of an empty iterable')

    for x in l:
        s1 += x
        s0 += 1

    return s1 / s0


def var(*args, **kwargs):
    if not args:
        return _var(**kwargs)

    return _var(**kwargs)(*args)


def std(*args, **kwargs):
    if not args:
        return _std(**kwargs)

    return _std(**kwargs)(*args)


class _var(object):
    def __init__(self, ddof=0.0):
        self.ddof = ddof

    def __call__(self, seq):
        s2, s1, s0 = 0.0, 0.0, 0.0

        for x in seq:
            s2 += x ** 2.0
            s1 += x
            s0 += 1

        return ((s2 / s0) - (s1 / s0) ** 2.0) * s0 / (s0 - self.ddof)


class _std(object):
    def __init__(self, ddof=0.0):
        self.ddof = ddof

    def __call__(self, seq):
        return math.sqrt(_var(self.ddof)(seq))


rules = RuleSet()
rules.add(eq(mean), lambda _, __, o: o.mean())
rules.add(a(_var), lambda _, f, o: o.var(f.ddof))
rules.add(a(_std), lambda _, f, o: o.std(f.ddof))
rules.add(eq(var), lambda _, f, o: o.var())
rules.add(eq(std), lambda _, f, o: o.std())
