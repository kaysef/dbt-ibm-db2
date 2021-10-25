from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation, Policy

@dataclass
class IBMDBtQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class IBMDBtIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class IBMDBtRelation(BaseRelation):
    quote_policy: IBMDBtQuotePolicy = IBMDBtQuotePolicy()
    include_policy: IBMDBtIncludePolicy = IBMDBtIncludePolicy()

    @staticmethod
    def add_ephemeral_prefix(name: str):
        return f'DBT_CTE__{name}'