from dataclasses import dataclass

import dbt.exceptions
from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.dataclass_schema import StrEnum


@dataclass
class ClickhouseQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class ClickhouseIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


class ClickHouseRelationDropType(StrEnum):
    Table = "table"
    Dictionary = "dictionary"


@dataclass(frozen=True, eq=False, repr=False)
class ClickhouseRelation(BaseRelation):
    quote_policy: ClickhouseQuotePolicy = ClickhouseQuotePolicy()
    include_policy: ClickhouseIncludePolicy = ClickhouseIncludePolicy()
    quote_character: str = ""
    can_exchange: bool = False
    table_engine: str = ""
    drop_type: str = ClickHouseRelationDropType.Table

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise dbt.exceptions.RuntimeException(
                f'Cannot set database {self.database} in clickhouse!'
            )

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise dbt.exceptions.RuntimeException(
                'Got a clickhouse relation with schema and database set to '
                'include, but only one can be set'
            )
        return super().render()
