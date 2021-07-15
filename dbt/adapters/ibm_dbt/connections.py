from dataclasses import dataclass

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager

import pyodbc

from contextlib import contextmanager
import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.connection import Connection
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger

@dataclass
class IBMDbtCredentials(Credentials):
    # Add credentials members here, like:
    # host: str
    # port: int
    # driver: str
    system: str
    database: str
    username: str
    password: str

    @property
    def type(self):
        return 'ibm_dbt'

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'.
        # Omit fields like 'password'!
        # raise NotImplementedError
        return ('driver', 'system', 'database', 'naming', 'username')


class IBMDbtConnectionManager(SQLConnectionManager):
    TYPE = 'ibm_dbt'
    

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except pyodbc.DatabaseError as exc:
            self.release()
            logger.debug('pyodbc error: {}'.format(str(exc)))
            logger.debug("Error running SQL: {}".format(sql))
            raise dbt.exceptions.DatabaseException(str(exc))
        except Exception as exc:
            self.release()
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            raise dbt.exceptions.RuntimeException(str(exc))

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = connection.credentials

        try:
            con_str = "DRIVER={IBM i Access ODBC Driver}"
            con_str += f";SYSTEM={credentials.system}"
            con_str += f";DATABASE={credentials.database}"
            con_str += f";NAM=0"
            con_str += f";UID={credentials.username}"
            con_str += f";PWD={credentials.password}"

            handle = pyodbc.connect(con_str)

            connection.state = 'open'
            connection.handle = handle

        except Exception as exc:
            connection.state = 'fail'
            connection.handle = None
            logger.debug("Error connecting to database: {}".format(str(exc), b=con_str))
            raise dbt.exceptions.FailedToConnectException(str(exc))

        return connection

    @classmethod
    def cancel(self, connection):
        connection_name = connection.name

        logger.info("Cancelling query '{}' ".format(connection_name))

        try:
            connection.handle.close()
        except Exception as e:
            logger.error('Error closing connection for cancel request')
            raise Exception(str(e))

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:

        message = 'OK'
        rows = cursor.rowcount

        return AdapterResponse(
            _message=message,
            rows_affected=rows
        )

    def add_begin_query(self):
        pass
