from __future__ import print_function, division, absolute_import

import toolz
from toolz import curried

from ._util import RuleSet, eq, _raise
from ._stdlib import DaskDict


def _map_items(impl):
    "impl: func(f, k, v)"
    return lambda _, f, o: DaskDict(DaskDict(o).items().map(lambda t: impl(f.args[0], t[0], t[1])))


def _filter_items(impl):
    "impl: func(f, k, v)"
    return lambda _, f, o: DaskDict(DaskDict(o).items().filter(lambda t: impl(f.args[0], t[0], t[1])))


def _toolz_item_filter(_, func, obj):
    return DaskDict(DaskDict(obj).items().filter(func.args[0]))


def _toolz_join(_, func, obj):
    # between toolz and dask the roles between left / right are reversed
    def impl(leftkey, leftseq, rightkey):
        return obj.join(leftseq, on_self=rightkey, on_other=leftkey)

    return impl(*func.args, **func.keywords)


def _call(impl, f):
    return impl(*f.args, **f.keywords)


rules = RuleSet()
rules.add(eq(curried.map), lambda _, f, o: _call(o.map, f))
rules.add(eq(curried.filter), lambda _, f, o: _call(o.filter, f))
rules.add(eq(curried.pluck), lambda _, f, o: _call(o.pluck, f))
rules.add(eq(curried.join), _toolz_join)
rules.add(eq(curried.countby), lambda _, f, o: DaskDict(_call(o.map, f).frequencies()))
rules.add(eq(curried.groupby), lambda _, f, o: DaskDict(_call(o.groupby, f)))
rules.add(eq(curried.keymap), _map_items(lambda f, k, v: (f(k), v)))
rules.add(eq(curried.valmap), _map_items(lambda f, k, v: (k, f(v))))
rules.add(eq(curried.keyfilter), _filter_items(lambda f, k, v: f(k)))
rules.add(eq(curried.valfilter), _filter_items(lambda f, k, v: f(v)))
rules.add(eq(curried.itemfilter), _toolz_item_filter)
rules.add(eq(curried.mapcat), lambda _, f, o: o.map(f.args[0]).concat())
rules.add(eq(curried.random_sample), lambda _, f, o: _call(o.random_sample, f))
rules.add(eq(curried.reduce), lambda _, f, o: _call(o.fold, f))
rules.add(eq(curried.reduceby), lambda _, f, o: DaskDict(_call(o.foldby, f)))
rules.add(eq(curried.remove), lambda _, f, o: _call(o.remove, f))
rules.add(eq(curried.topk), lambda _, f, o: _call(o.topk, f))
rules.add(eq(curried.unique), lambda _, f, o: _call(o.distinct, f))
rules.add(eq(toolz.unique), lambda _, __, o: o.distinct())

rules.add(eq(curried.accumulate), _raise(NotImplementedError))
rules.add(eq(curried.assoc), _raise(NotImplementedError))
rules.add(eq(curried.assoc_in), _raise(NotImplementedError))
rules.add(eq(curried.cons), _raise(NotImplementedError))
rules.add(eq(curried.do), _raise(NotImplementedError))
rules.add(eq(curried.drop), _raise(NotImplementedError))
rules.add(eq(curried.excepts), _raise(NotImplementedError))
rules.add(eq(curried.get), _raise(NotImplementedError))
rules.add(eq(curried.get_in), _raise(NotImplementedError))
rules.add(eq(curried.interpose), _raise(NotImplementedError))
rules.add(eq(curried.iterate), _raise(NotImplementedError))
rules.add(eq(curried.nth), _raise(NotImplementedError))
rules.add(eq(curried.partial), _raise(NotImplementedError))
rules.add(eq(curried.partition), _raise(NotImplementedError))
rules.add(eq(curried.partition_all), _raise(NotImplementedError))
rules.add(eq(curried.partitionby), _raise(NotImplementedError))
rules.add(eq(curried.sliding_window), _raise(NotImplementedError))
rules.add(eq(curried.sorted), _raise(NotImplementedError))
rules.add(eq(curried.tail), _raise(NotImplementedError))
rules.add(eq(curried.take), _raise(NotImplementedError))
rules.add(eq(curried.take_nth), _raise(NotImplementedError))
rules.add(eq(curried.update_in), _raise(NotImplementedError))
