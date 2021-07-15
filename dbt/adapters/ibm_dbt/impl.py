from dbt.adapters.sql import SQLAdapter
from dbt.adapters.ibm_dbt import IBMDbtConnectionManager
from dbt.adapters.ibm_dbt.relation import IBMDB2Relation


class IBMDbtAdapter(SQLAdapter):
    ConnectionManager = IBMDbtConnectionManager
    Relation = IBMDB2Relation

    @classmethod
    def date_function(cls):
        return 'datenow()'

    def is_cancelable(cls):
        return False

    def debug_query(self):
        self.execute('select 1 as one from sysibm.sysdummy1')