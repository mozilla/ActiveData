import re


def match(pattern, text):
    result = re.match(pattern, text)
    return result.groups()

