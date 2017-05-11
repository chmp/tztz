from __future__ import print_function, division, absolute_import

import pytest


def params(spec, m):
    return pytest.mark.parametrize(
        spec, [v for (_, v) in sorted(m.items())], ids=sorted(m.keys()),
    )
