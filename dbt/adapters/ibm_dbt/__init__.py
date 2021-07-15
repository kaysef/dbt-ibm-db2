from dbt.adapters.ibm_dbt.connections import IBMDbtConnectionManager
from dbt.adapters.ibm_dbt.connections import IBMDbtCredentials
from dbt.adapters.ibm_dbt.impl import IBMDbtAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import ibm_dbt


Plugin = AdapterPlugin(
    adapter=IBMDbtAdapter,
    credentials=IBMDbtCredentials,
    include_path=ibm_dbt.PACKAGE_PATH)
