# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import flask
from mo_dots import coalesce

from mo_future import binary_type
from pyLibrary.env.big_data import ibytes2icompressed

TOO_SMALL_TO_COMPRESS = 510  # DO NOT COMPRESS DATA WITH LESS THAN THIS NUMBER OF BYTES


def gzip_wrapper(func, compress_lower_limit=None):
    compress_lower_limit = coalesce(compress_lower_limit, TOO_SMALL_TO_COMPRESS)

    def output(*args, **kwargs):
        response = func(*args, **kwargs)
        accept_encoding = flask.request.headers.get('Accept-Encoding', '')
        if 'gzip' not in accept_encoding.lower():
            return response

        resp = response.data
        response.headers['Content-Encoding'] = 'gzip'
        if isinstance(resp, binary_type) and len(resp) > compress_lower_limit:
            response.set_data(b''.join(ibytes2icompressed([resp])))
        else:
            response.data = ibytes2icompressed(resp)

        return response

    return output


