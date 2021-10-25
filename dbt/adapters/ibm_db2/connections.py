from contextlib import contextmanager

import pyodbc
import os
import time
from typing import Any, Optional, Tuple, Union
import agate
from dbt.helpers.reader import read_key

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.ibm_db2 import __version__
from dbt.contracts.connection import AdapterResponse

from dataclasses import dataclass
from dbt.logger import GLOBAL_LOGGER as logger

# pyodbc.SQL_AUTOCOMMIT = True

@dataclass
class IBMDBtCredentials(Credentials):
    # Add credentials members here, like:
    driver: str
    system: str
    database: str

    UID: Optional[str] = None
    PWD: Optional[str] = None
    NAM: Optional[int] = 0

    _ALIASES = {
        "user": "UID",
        "username": "UID",
        "pass": "PWD",
        "password": "PWD",
        "naming": "NAM"
    }

    @property
    def type(self) -> str:
        return 'ibm_db2'

    @property
    def unique_field(self):
        """
        Hashed and included in anonymous telemetry to track adapter adoption.
        Pick a field that can uniquely identify one team/organization building with this adapter
        """
        return self.UID

    def _connection_keys(self) -> Tuple[str, ...]:
        # return an iterator of keys to pretty-print in 'dbt debug'.
        # Omit fields like 'password'!
        # raise NotImplementedError
        return ('driver', 'database', 'NAM', 'UID')


class IBMDBtConnectionManager(SQLConnectionManager):
    TYPE = 'ibm_db2'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except pyodbc.DatabaseError as e:
            logger.debug(f"Database error: {str(e)} {sql}")

            try:
                # attempt to release the connection
                self.release()
            except pyodbc.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt.exceptions.DatabaseException(str(e).strip()) from e

        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            logger.debug("Rolling back transaction.")
            self.release()
            if isinstance(e, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.RuntimeException(e)

    @classmethod
    def open(cls, connection):

        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

        try:
            con_str = []
            con_str.append(f"DRIVER={{{credentials.driver}}}")
            con_str.append(f"SYSTEM={read_key(credentials.system)}")
            con_str.append(f"DATABASE={read_key(credentials.database)}")
            con_str.append(f"NAM={credentials.NAM}")
            con_str.append(f"UID={read_key(credentials.UID)}")
            con_str.append(f"PWD={read_key(credentials.PWD)}")

            con_str_concat = ';'.join(con_str)

            index = []
            for i, elem in enumerate(con_str):
                if 'pwd=' in elem.lower():
                    index.append(i)

            if len(index) !=0 :
                con_str[index[0]]="PWD=***"

            con_str_display = ';'.join(con_str)

            logger.debug(f"Using connection string: {con_str_display}")

            handle = pyodbc.connect(
                con_str_concat,
                autocommit=True
            )

            connection.state = "open"
            connection.handle = handle
            logger.debug(f"Connected to db: {credentials.database}")

        except pyodbc.Error as e:
            logger.debug(f"Could not connect to db: {e}")

            connection.handle = None
            connection.state = "fail"

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def cancel(self, connection):
        logger.debug("Cancel query")
        pass

    def add_begin_query(self):
        pass

    def add_commit_query(self):
        pass

    def add_query(self, sql: str, auto_begin: bool=True, bindings: Optional[Any]=None, abridge_sql_log: bool=False):

        connection = self.get_thread_connection()

        if auto_begin and connection.transaction_open is False:
            self.begin()

        logger.debug('Using {} connection "{}".'.format(self.TYPE, connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                logger.debug("On {}: {}....".format(connection.name, sql[0:512]))
            else:
                logger.debug("On {}: {}".format(connection.name, sql))
            pre = time.time()

            cursor = connection.handle.cursor()

            # pyodbc does not handle a None type binding!
            if bindings is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, bindings)

            logger.debug(
                "SQL status: {} in {:0.2f} seconds".format(
                    self.get_response(cursor), (time.time() - pre)
                )
            )
            
            return connection, cursor

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
    

    def execute(self, sql: str, auto_begin: bool=True, fetch: bool=False) -> Tuple[Union[AdapterResponse, str], agate.Table]:
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            # Get the result of the first non-empty result set (if any)
            while cursor.description is None:
                if not cursor.nextset(): 
                    break
            table = self.get_result_from_cursor(cursor)
        else:
            table = dbt.clients.agate_helper.empty_table()
        # Step through all result sets so we process all errors
        while cursor.nextset(): 
            pass
        return response, table