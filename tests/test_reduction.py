from __future__ import print_function, division, absolute_import

from util import params

import dask.bag as db

from tztz import apply, mean, var, std


examples = {
    'mean': (mean, range(20)),
    'var': (var, range(20)),
    'std': (std, range(20)),
    'var (1)': (var(ddof=1.0), range(20)),
    'std (1)': (std(ddof=1.0), range(20)),
}


@params('func, arg', examples)
def test_input_outputs(func, arg):
    actual_no_dask = func(arg)
    actual_dask = apply(db.from_sequence(arg, npartitions=3), func).compute()

    assert actual_dask == actual_no_dask
