# Copied from clickhouse_sqlalchemy/drivers/http/transport.py
import re

from datetime import datetime
from decimal import Decimal
from functools import partial

from ipaddress import IPv4Address, IPv6Address

import requests

from .exceptions import DatabaseException, HTTPException  # NOTE (oev81): imports changed
from .utils import FORMAT_SUFFIX, parse_tsv  # NOTE (oev81): imports changed


DEFAULT_DDL_TIMEOUT = None
DATE_NULL = '0000-00-00'
DATETIME_NULL = '0000-00-00 00:00:00'

EXTRACT_SUBTYPE_RE = re.compile(r'^[^\(]+\((.+)\)$')


def date_converter(x):
    if x != DATE_NULL:
        return datetime.strptime(x, '%Y-%m-%d').date()
    return None


def datetime_converter(x):
    if x == DATETIME_NULL:
        return None
    elif len(x) > 19:
        return datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')
    else:
        return datetime.strptime(x, '%Y-%m-%d %H:%M:%S')


def nullable_converter(subtype_str, x):
    if x is None:
        return None

    converter = _get_type(subtype_str)
    return converter(x) if converter else x


def nothing_converter(x):
    return None


converters = {
    'Int8': int,
    'UInt8': int,
    'Int16': int,
    'UInt16': int,
    'Int32': int,
    'UInt32': int,
    'Int64': int,
    'UInt64': int,
    'Int128': int,
    'UInt128': int,
    'Int256': int,
    'UInt256': int,
    'Float32': float,
    'Float64': float,
    'Decimal': Decimal,
    'Date': date_converter,
    'DateTime': datetime_converter,
    'DateTime64': datetime_converter,
    'IPv4': IPv4Address,
    'IPv6': IPv6Address,
    'Nullable': nullable_converter,
    'Nothing': nothing_converter,
}


def _get_type(type_str):
    result = converters.get(type_str)

    if result is not None:
        return result

    # sometimes type_str is DateTime64(x)
    if type_str.startswith('DateTime64'):
        return converters['DateTime64']

    if type_str.startswith('Decimal'):
        return converters['Decimal']

    if type_str.startswith('Nullable('):
        subtype_str = EXTRACT_SUBTYPE_RE.match(type_str).group(1)
        return partial(converters['Nullable'], subtype_str)

    return None


class RequestsTransport(object):
    def __init__(
            self,
            db_url, db_name, username, password,
            timeout=None, ch_settings=None,
            **kwargs):

        self.db_url = db_url
        self.db_name = db_name
        self.auth = (username, password)
        self.timeout = timeout  # NOTE (oev81): changed here
        self.verify = kwargs.pop('verify', True)
        self.cert = kwargs.pop('cert', None)
        self.headers = {
            key[8:]: value
            for key, value in kwargs.items()
            if key.startswith('header__')
        }

        self.unicode_errors = kwargs.pop('unicode_errors', 'escape')

        ch_settings = dict(ch_settings or {})
        self.ch_settings = ch_settings

        ddl_timeout = kwargs.pop('ddl_timeout', DEFAULT_DDL_TIMEOUT)
        if ddl_timeout is not None:
            self.ch_settings['distributed_ddl_task_timeout'] = int(ddl_timeout)

        # By default, keep connection open between queries.
        http = kwargs.pop('http_session', requests.Session)
        self.http = http() if callable(http) else http

        super(RequestsTransport, self).__init__()

    def execute(self, query, params=None):
        """
        Query is returning rows and these rows should be parsed or
        there is nothing to return.
        """
        query = query.rstrip('; \r\n')  # NOTE (oev81): added

        # NOTE (oev81): adding FORMAT_SUFFIX not needed if
        # default_format parameter or X-ClickHouse-Format header are specified.

        # query += FORMAT_SUFFIX

        r = self._send(query, params=params, stream=True)
        lines = r.iter_lines()

        try:
            names = parse_tsv(next(lines), self.unicode_errors)
            types = parse_tsv(next(lines), self.unicode_errors)
        except StopIteration:
            # Empty result; e.g. a DDL request.
            return

        convs = [_get_type(type_) for type_ in types]

        # NOTE (oev81): changed here
        rows_gen = (
            [
                (conv(x) if conv else x)
                for x, conv in zip(parse_tsv(line, self.unicode_errors), convs)
            ]
            for line in lines
        )

        # NOTE (oev81): changed here
        return names, types, rows_gen

    def raw(self, query, params=None, stream=False):
        """
        Performs raw query to database. Returns its output
        :param query: Query to execute
        :param params: Additional params should be passed during query.
        :param stream: If flag is true, Http response from ClickHouse will be
            streamed.
        :return: Query execution result
        """
        return self._send(query, params=params, stream=stream).text

    def _send(self, data, params=None, stream=False):
        data = data.encode('utf-8')
        params = params or {}
        params['database'] = self.db_name
        params.update(self.ch_settings)

        # TODO: retries, prepared requests
        r = self.http.post(
            self.db_url, auth=self.auth, params=params, data=data,
            stream=stream, timeout=self.timeout, headers=self.headers,
            verify=self.verify, cert=self.cert
        )
        if r.status_code != 200:
            orig = HTTPException(r.text)
            orig.code = r.status_code
            raise DatabaseException(orig)
        return r

    def close(self):
        # NOTE (oev81): added
        if hasattr(self.http, 'close'):
            self.http.close()
