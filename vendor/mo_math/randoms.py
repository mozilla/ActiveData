# encoding: utf-8
#

from __future__ import absolute_import, division, unicode_literals

import random
import string

builtin_range = range
SIMPLE_ALPHABET = string.ascii_letters + string.digits
SEED = random.Random()


def set_seed(seed):
    global SEED
    SEED = random.Random(seed)


def string(length, alphabet=SIMPLE_ALPHABET):
    result = ""
    for _ in builtin_range(length):
        result += SEED.choice(alphabet)
    return result


def hex(length):
    return string(length, string.digits + "ABCDEF")


def base64(length, extra="+/"):
    return string(length, SIMPLE_ALPHABET + extra)


def filename():
    return base64(20, extra="-_")


def int(*args):
    return SEED.randrange(*args)


def range(start, stop, *args):
    return SEED.randrange(start, stop, *args)


def float(*args):
    if args:
        return SEED.random() * args[0]
    else:
        return SEED.random()


def sample(data, count):
    num = len(data)
    return [data[int(num)] for _ in builtin_range(count)]


def combination(data):
    output = []
    data = list(data)
    num = len(data)
    for i in builtin_range(num):
        n = int(num - i)
        output.append(data[n])
        del data[n]
    return output


def bytes(count):
    output = bytearray(SEED.randrange(256) for _ in builtin_range(count))
    return output


def weight(weights):
    """
    RETURN RANDOM INDEX INTO WEIGHT ARRAY, GIVEN WEIGHTS
    """
    total = sum(weights)

    p = SEED.random()
    acc = 0
    for i, w in enumerate(weights):
        acc += w / total
        if p < acc:
            return i
    return len(weights) - 1
