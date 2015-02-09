from functools import partial
from pyLibrary.dot import listwrap, Dict, unwrap


def parametrize(names, values):
    def output(func):
        def p_func(self, *args, **kwargs):
            for i, v in enumerate(values):
                n = listwrap(names)
                v = listwrap(v)

                func(self, **{nn: vv for nn, vv in zip(n, v)})

        return p_func

    return output
