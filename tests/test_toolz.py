from __future__ import print_function, division, absolute_import

import operator as op

from util import params

import dask.bag as db
import toolz
from toolz import first, second
import toolz.curried as curried

from tztz import apply, chained


examples = {
    'map': (chained(curried.map(lambda x: 2 * x), list), [1, 2, 3, 4, 5, 6]),
    'filter': (chained(curried.filter(lambda x: x % 2 == 0), list), range(10)),
    'remove': (chained(curried.remove(lambda x: x % 2 == 0), list), range(10)),

    # example from toolz docs
    'pluck-single': (
        chained(curried.pluck('name'), list),
        [{'id': 1, 'name': 'Cheese'}, {'id': 2, 'name': 'Pies'}],
    ),

    # example from toolz docs
    'pluck-mulitple': (
        chained(curried.pluck([0, 1]), list),
        [[1, 2, 3], [4, 5, 7]],
    ),

    # example from toolz docs
    'join': (
        chained(
            curried.join(
                second,
                [('Alice', 'Edith'), ('Alice', 'Zhao'), ('Edith', 'Alice'),
                 ('Zhao', 'Alice'), ('Zhao', 'Edith')],
                first
            ),
            curried.map(lambda t: (t[0][0], t[1][1])),
            sorted,
        ),
        [('Alice', 'NYC'), ('Alice', 'Chicago'), ('Dan', 'Syndey'),
         ('Edith', 'Paris'), ('Edith', 'Berlin'), ('Zhao', 'Shanghai')]
    ),

    'count_by': (curried.countby(lambda x: x % 2 == 0), range(20)),

    'groupby': (
        chained(
            curried.groupby(lambda x: x % 2 == 0),
            curried.valmap(sorted),
        ),
        range(20),
    ),
    'keymap': (
        chained(dict, curried.keymap(lambda x: 2 * x)),
        dict.items({1: 2, 3: 4, 5: 6, 7: 8, 9: 10}),
    ),
    'valmap': (
        chained(dict, curried.valmap(lambda x: 2 * x)),
        dict.items({1: 2, 3: 4, 5: 6, 7: 8, 9: 10}),
    ),
    'keyfilter': (
        chained(dict, curried.keyfilter(lambda x: x > 5)),
        dict.items({1: 2, 3: 4, 5: 6, 7: 8, 9: 10}),
    ),
    'valfilter': (
        chained(dict, curried.valfilter(lambda x: x > 5)),
        dict.items({1: 2, 3: 4, 5: 6, 7: 8, 9: 10}),
    ),
    'itemfilter': (
        chained(dict, curried.itemfilter(lambda i: i[0] % 2 == 0 and i[1] < 4)),
        dict.items({1: 2, 2: 3, 3: 4, 4: 5})
    ),
    # example taken from toolz docs
    'mapcat': (
        chained(
            curried.mapcat(lambda s: [c.upper() for c in s]),
            list,
        ),
        [["a", "b"], ["c", "d", "e"]],
    ),
    'reduce': (curried.reduce(op.add), range(20)),
    'reduceby': (curried.reduceby(lambda x: x % 2 == 0, op.add), range(20)),
    'topk': (chained(curried.topk(5), list), range(20)),
    'curried.unique': (chained(curried.unique, sorted, ), [1, 1, 2, 3, 4, 4]),
    'unique': (chained(toolz.unique, sorted, ), [1, 1, 2, 3, 4, 4]),
}


@params('func,arg', examples)
def test_input_outputs(func, arg):
    actual_no_dask = func(arg)
    actual_dask = apply(db.from_sequence(arg, npartitions=3), func).compute()

    assert actual_dask == actual_no_dask
