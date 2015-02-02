from urlparse import urlparse
from pyLibrary.dot.dicts import Dict


def URL(value):

    output = urlparse(value)

    return Dict(
        protocol=output.scheme,
        host=output.netloc,
        port=output.port,
        path=output.path,
        query=output.query,
        fragmen=output.fragment
    )
