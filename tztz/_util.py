from __future__ import print_function, division, absolute_import

from toolz import curry


class RuleSet(object):
    def __init__(self, rules=()):
        self.rules = list(rules)

    def add(self, match, apply, help=None):
        self.rules.append(dict(match=match, apply=apply, help=help))

    def __or__(self, other):
        return RuleSet(self.rules + other.rules)

    def __call__(self, *args):
        for rule in self.rules:
            match = rule['match']
            does_match = match(self, *args)

            if does_match:
                apply = rule['apply']
                return apply(self, *args)

        raise RuntimeWarning('no matching rule')


def eq(obj):
    if isinstance(obj, curry):
        return lambda _, other, __: isinstance(other, curry) and other.func == obj.func

    return lambda _, other, __: other == obj


def a(t):
    return lambda _, other, __: isinstance(other, t)


def _raise(exc, *args, **kwargs):
    def impl(*_, **__):
        raise exc(*args, **kwargs)

    return impl
