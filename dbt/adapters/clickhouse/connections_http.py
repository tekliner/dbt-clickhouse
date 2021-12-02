from typing import Optional, Any, Tuple

import agate
import io  # NOTE (oev81): added
import time
import dbt.exceptions

from decimal import Decimal
from dataclasses import dataclass
from contextlib import contextmanager

from clickhouse_driver import Client, errors

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import Connection
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.version import __version__ as dbt_version

from . import http


@dataclass
class ClickhouseCredentials(Credentials):
    protocol: str = 'https'
    host: str = 'localhost'
    port: Optional[int] = None
    user: Optional[str] = 'default'
    database: Optional[str] = None
    schema: Optional[str] = 'default'
    password: str = ''
    cluster: Optional[str] = None
    secure: bool = False
    verify: bool = False

    @property
    def type(self):
        return 'clickhouse'

    @property
    def unique_field(self):
        return self.host

    def __post_init__(self):
        if self.database is not None and self.database != self.schema:
            raise dbt.exceptions.RuntimeException(
                f'    schema: {self.schema} \n'
                f'    database: {self.database} \n'
                f'    cluster: {self.cluster} \n'
                f'On Clickhouse, database must be omitted or have the same value as'
                f' schema.'
            )
        self.database = None

    def _connection_keys(self):
        return ('protocol', 'host', 'port', 'user', 'schema', 'secure', 'verify')


class ClickhouseConnectionManager(SQLConnectionManager):
    TYPE = 'clickhouse'
    OK_STATUS = 'OK'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except Exception as e:
            logger.debug('Error running SQL: {}', sql)
            logger.debug('Rolling back transaction.')
            self.release()
            if isinstance(e, dbt.exceptions.RuntimeException):
                raise

            raise dbt.exceptions.RuntimeException(e) from e

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = cls.get_credentials(connection.credentials)

        try:
            handle = http.transport.RequestsTransport(
                db_url=f'{credentials.protocol}://{credentials.host}:{credentials.port}',
                db_name='default',
                username=credentials.user,
                password=credentials.password,
                timeout=(10, 1800),
                ch_settings={
                    'session_id': http.sessionid.generate_session_id(),  # NOTE (oev81): added
                    'session_timeout': 5*60,  # NOTE (oev81): added
                    'default_format': 'TabSeparatedWithNamesAndTypes',  # NOTE (oev81): added
                },
            )

            connection.handle = handle
            connection.state = 'open'
        except Exception as e:
            logger.debug(
                'Got an error when attempting to open a clickhouse connection: \'{}\'',
                str(e),
            )

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def cancel(self, connection):
        connection_name = connection.name

        logger.debug('Do nothing on Cancelling query \'{}\'', connection_name)

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[str, agate.Table]:
        sql = self._add_query_comment(sql)
        conn = self.get_thread_connection()
        client = conn.handle

        with self.exception_handler(sql):
            logger.debug(
                'On {connection_name}: {sql}',
                connection_name=conn.name,
                sql=f'{sql}...',
            )

            pre = time.time()

            response = client.execute(sql)

            status = self.OK_STATUS

            logger.debug(
                'SQL status: {status} in {elapsed:0.2f} seconds',
                status=status,
                elapsed=(time.time() - pre),
            )

            if fetch:
                table = self._get_table_from_response(response)
            else:
                table = dbt.clients.agate_helper.empty_table()

            return status, table

    def _get_table_from_response(self, response) -> agate.Table:
        if response is None:
            return dbt.clients.agate_helper.empty_table()

        column_names, column_types, rows_iterator = response

        data = [
            dict(zip(column_names, row))
            for row in rows_iterator
        ]

        return dbt.clients.agate_helper.table_from_data_flat(data, column_names)

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        sql = self._add_query_comment(sql)
        conn = self.get_thread_connection()
        client = conn.handle

        csv_or_none = self._convert_bindings_to_csv(bindings)

        with self.exception_handler(sql):
            logger.debug(
                'On {connection_name}: {sql}',
                connection_name=conn.name,
                sql=f'{sql}...',
            )

            # TODO: Convert to allow clickhouse type
            # format_bindings = []
            # for row in bindings.rows:
            #     format_row = []
            #     for v in row.values():
            #         if isinstance(v, Decimal):
            #             v = int(v)
            #         format_row.append(v)
            #     format_bindings.append(format_row)

            pre = time.time()

            client.execute(sql, data=csv_or_none)  # NOTE (oev81): changed

            status = self.OK_STATUS

            logger.debug(
                'SQL status: {status} in {elapsed:0.2f} seconds',
                status=status,
                elapsed=(time.time() - pre),
            )

    # NOTE (oev81): method added
    def _convert_bindings_to_csv(
        self,
        bindings: Optional[Any],
    ) -> Optional[str]:
        if bindings is None:
            return None

        if isinstance(bindings, agate.Table):
            return self._convert_agate_table_to_csv(bindings)

        raise Exception(
            f'Unexpected bindings type={type(bindings)}'
        )

    # NOTE (oev81): method added
    def _convert_agate_table_to_csv(
        self,
        table: agate.Table,
    ) -> str:
        buff = io.StringIO()
        table.to_csv(buff)

        return buff.getvalue()

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor):
        return 'OK'

    def begin(self):
        pass

    def commit(self):
        pass
