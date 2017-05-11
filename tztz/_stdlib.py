from __future__ import print_function, division, absolute_import

import collections
import itertools as it

from ._util import RuleSet, eq


class DaskDict(object):
    def __init__(self, items):
        if isinstance(items, DaskDict):
            items = items._items

        self._items = items

    def items(self):
        return self._items

    def keys(self):
        return self._items.map(lambda t: t[0])

    def values(self):
        return self._items.map(lambda t: t[1])

    def copy(self):
        "dask bags are immutable"
        return self

    def compute(self, **kwargs):
        return dict(self._items.compute(**kwargs))


class DaskCounter(DaskDict):
    def _call(self, func):
        def impl(items):
            counter = collections.Counter()
            for k, v in items:
                counter[k] = v
            return func(counter)

        return self._items.repartition(1).map_partitions(impl)

    def elements(self):
        return self._call(lambda obj: list(obj.elements()))

    def most_common(self, n):
        return self._call(lambda obj: list(obj.most_common(n)))


rules = RuleSet()

rules.add(eq(any), lambda _, __, obj: obj.any())
rules.add(eq(all), lambda _, __, obj: obj.all())
rules.add(eq(min), lambda _, __, obj: obj.min())
rules.add(eq(max), lambda _, __, obj: obj.max())
rules.add(eq(sum), lambda _, __, obj: obj.sum())
rules.add(eq(len), lambda _, __, obj: obj.count())
rules.add(eq(set), lambda _, __, obj: obj.distinct())
rules.add(eq(list), lambda _, __, obj: obj)
rules.add(eq(dict), lambda _, __, obj: DaskDict(obj))
rules.add(eq(dict.items), lambda _, __, obj: DaskDict(obj).items())
rules.add(eq(it.chain.from_iterable), lambda _, __, obj: obj.concat())
rules.add(eq(collections.Counter), lambda _, __, obj: DaskCounter(obj.frequencies()))

# TODO: add warning that this is inefficient for large collections
rules.add(eq(sorted), lambda _, __, obj: obj.repartition(1).map_partitions(sorted))
