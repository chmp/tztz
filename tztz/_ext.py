from __future__ import print_function, division, absolute_import

from ._util import RuleSet, a


class chained(object):
    """Represent the composition of functions.

    When the resulting object is called with a single argument, the passed
    object is transformed by passing it through all given functions.
    For example::

        a = chained(
            math.sqrt,
            math.log,
            math.cos,
        )(5.0)

    is equivalent to::

        a = 5.0
        a = math.sqrt(a)
        a = math.log(a)
        a = math.cos(a)

    Different chains can be composed via ``+``.
    For example, the chain above can be written as::

        chained(math.sqrt, math.log) + chained(math.cos)

    """
    def __init__(self, *funcs):
        self.funcs = funcs

    def __call__(self, obj):
        for func in self.funcs:
            obj = func(obj)

        return obj

    def __repr__(self):
        return 'tztz.chained({})'.format(', '.join(repr(func) for func in self.funcs))

    def __iter__(self):
        return iter(self.funcs)


def _db_chained(rules, chain, obj):
    for func in chain:
        obj = rules(func, obj)

    return obj


class repartition(object):
    """Express repartition of a ``dask.bag.Bag``, for non bags it is a nop-op.

    :param int n:
        the number of partitions
    """
    def __init__(self, n):
        self.n = n

    def __call__(self, obj):
        return obj


rules = RuleSet()
rules.add(a(chained), _db_chained)
rules.add(a(repartition), lambda _, f, o: o.repartition(f.n))
