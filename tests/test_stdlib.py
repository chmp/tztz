from __future__ import print_function, division, absolute_import

from util import params

import collections
import itertools as it
import operator as op

import dask.bag as db

from tztz import apply, chained


examples = {
    'any 1': (any, [True, False, False]),
    'any 2': (any, [False, False, False]),
    'any 3': (any, [True, True, True]),
    'all 1': (all, [True, False, False]),
    'all 2': (all, [False, False, False]),
    'all 3': (all, [True, True, True]),
    'sum': (sum, list(range(20))),
    'min': (min, list(range(20))),
    'max': (max, list(range(20))),
    'len': (len, list(range(20))),
    'list': (list, list(range(20))),
    'dict': (dict, [(1, 2), (3, 4), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13)]),
    'dict keys': (
        chained(dict, op.methodcaller('keys'), list),
        [(1, 2), (3, 4), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13)]
    ),
    'dict values': (
        chained(dict, op.methodcaller('values'), list),
        [(1, 2), (3, 4), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13)]
    ),
    'dict items': (
        chained(dict, op.methodcaller('items'), list),
        [(1, 2), (3, 4), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13)]
    ),
    'dict.items 2': (
        chained(dict, dict.items, list),
        [(1, 2), (3, 4), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13)]
    ),
    'itertools.chain.from_iterable': (
        chained(it.chain.from_iterable, list),
        [(1, 2), (3, 4), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13)]
    ),
    'collections.Counter': (chained(collections.Counter, dict), [1, 2, 2, 3, 3, 3, 4, 4, 4]),
    'collections.Counter.elements': (
        chained(collections.Counter, op.methodcaller('elements'), sorted),
        [1, 2, 2, 3, 3, 3, 4, 4, 4, 4],
    ),
    'collections.Counter.most_common': (
        chained(collections.Counter, op.methodcaller('most_common', 2), sorted),
        [1, 2, 2, 3, 3, 3, 4, 4, 4, 4],
    ),
    'set': (chained(set, sorted), [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]),
}


@params('func, arg', examples)
def test_input_outputs(func, arg):
    actual_no_dask = func(arg)
    actual_dask = apply(db.from_sequence(arg, npartitions=3), func).compute()

    assert actual_dask == actual_no_dask
