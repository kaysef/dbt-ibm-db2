from dbt.adapters.ibm_db2.connections import IBMDBtConnectionManager
from dbt.adapters.ibm_db2.connections import IBMDBtCredentials
from dbt.adapters.ibm_db2.impl import IBMDBtAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import ibm_db2


Plugin = AdapterPlugin(
    adapter=IBMDBtAdapter,
    credentials=IBMDBtCredentials,
    include_path=ibm_db2.PACKAGE_PATH)
