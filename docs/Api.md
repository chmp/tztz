# Parallel Data Pipelines

Often data processing involves multiple transformations of data into more and
more complex objects.

Python offers a number of utility functions for lists, like ``map``,
``filter``, and ``reduce``. For example say you wanted to compute the square
root  of the sum the square of all even numbers between 0 and 99. In terms of
said utility functions you can express this operation as

```python
from functools import reduce
import math
import operator as op

data = range(100)
math.sqrt(
    reduce(
        op.add,
        map(lambda x: x ** 2.0, filter(lambda x: x % 2 == 0, data))
    )
)
```

These operations can be rewritten by using the functionality offered by
`flowly` and `toolz` into:

```python
from toolz.curried import filter, map, reduce
from flowly.tz import chained

transform = chained(
    filter(lambda x: x % 2 == 0),
    map(lambda x: 2 * x),
    reduce(op.add),
    math.sqrt,
)

data = range(100)
transform(data)
```

[`tztz.chained`](#tztzchained) represents the application of multiple
functions, one after the other, and the curried namespace of [toolz][toolz]
allows to bind the first argument of said utility functions without executing
them immediately.

The second variant arguably simplifies the structure of the program and has to
additional benefit of being easier to compose. ``transform`` can easily be
placed into larger chains of transformations without having to be changed.
Finally, the second variant separates definition of the operations from
the execution. This way the operations can be reinterpreted, i.e., for parallel
execution.


`tztz` can reinterpret many existing elements of computation graphs to be
executed on top of dask bags. Applying the transformation to a `dask.bag.Bag`
is a simple matter of calling [`tztz.apply`](#tztzapply) and calling
`.compute()` on the result:

```python
import dask.bag as db
from tztz import apply

data = db.from_sequence([1, 2, 3, 4], npartitions=2)
result = apply(transform, data)
print(result.compute())
```

Since dask also supports distributed executors, computing the result on a
cluster becomes a matter of simply writing::

    from distributed import Client
    client = Client('127.0.0.1:8786')
    print(result.compute(get=client.get))

The DAG primitives that are understood can easily be adapted by specifying the
``rules`` argument to [apply](#tztzapply). Out of the box, the following
DAG primitives are supported:


# Reference

###  tztz.apply
`tztz.apply()`

Apply the given transformation to a `dask.bag.Bag`.



###  tztz.chained
`tztz.chained(*funcs)`

Represent the composition of functions.

When the resulting object is called with a single argument, the passed
object is transformed by passing it through all given functions.
For example:

```
a = chained(
    math.sqrt,
    math.log,
    math.cos,
)(5.0)
```

is equivalent to:

```
a = 5.0
a = math.sqrt(a)
a = math.log(a)
a = math.cos(a)
```



###  tztz.mean
`tztz.mean()`

Calculate the mean of a list of values.



###  tztz.repartition
`tztz.repartition()`

Express repartition of a `dask.bag.Bag`, for non bags it is a nop-op.

#### Parameters

* **n** (*int*):
  the number of partitions



###  tztz.var
`tztz.var(*args, **kwargs)`

<undocumented>



[toolz]: http://toolz.readthedocs.io/en/latest/