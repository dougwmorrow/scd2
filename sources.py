"""Source system registry: Oracle (DNA) and SQL Server (CCM, EPICOR) connection factories."""

from dataclasses import dataclass
from enum import Enum
from urllib.parse import quote_plus

import config


class SourceType(Enum):
    ORACLE = "ORACLE"
    SQL_SERVER = "SQL_SERVER"


@dataclass(frozen=True)
class SourceSystem:
    name: str
    source_type: SourceType
    host: str
    port: int
    user: str
    password: str
    service_or_database: str

    def connectorx_uri(self) -> str:
        pwd = quote_plus(self.password)
        usr = quote_plus(self.user)
        if self.source_type == SourceType.ORACLE:
            return f"oracle://{usr}:{pwd}@{self.host}:{self.port}/{self.service_or_database}"
        return f"mssql://{usr}:{pwd}@{self.host}:{self.port}/{self.service_or_database}?TrustServerCertificate=true"

    def oracledb_connect_params(self) -> dict:
        if self.source_type != SourceType.ORACLE:
            raise ValueError(f"oracledb_connect_params only valid for Oracle sources, not {self.name}")
        return {
            "user": self.user,
            "password": self.password,
            "dsn": f"{self.host}:{self.port}/{self.service_or_database}",
        }


# --- Source Registry ---
_SOURCES: dict[str, SourceSystem] = {
    "DNA": SourceSystem(
        name="DNA",
        source_type=SourceType.ORACLE,
        host=config.ORACLE_HOST,
        port=config.ORACLE_PORT,
        user=config.ORACLE_USER,
        password=config.ORACLE_PASSWORD,
        service_or_database=config.ORACLE_SERVICE,
    ),
    "CCM": SourceSystem(
        name="CCM",
        source_type=SourceType.SQL_SERVER,
        host=config.CCM_SERVER_HOST,
        port=config.CCM_SERVER_PORT,
        user=config.CCM_SERVER_USER,
        password=config.CCM_SERVER_PASSWORD,
        service_or_database="CCM",
    ),
    "EPICOR": SourceSystem(
        name="EPICOR",
        source_type=SourceType.SQL_SERVER,
        host=config.EPICOR_SERVER_HOST,
        port=config.EPICOR_SERVER_PORT,
        user=config.EPICOR_SERVER_USER,
        password=config.EPICOR_SERVER_PASSWORD,
        service_or_database="EPICOR",
    ),
}


def get_source(name: str) -> SourceSystem:
    key = name.upper()
    if key not in _SOURCES:
        raise ValueError(f"Unknown source: {name}. Available: {list(_SOURCES.keys())}")
    return _SOURCES[key]


def register_source(source: SourceSystem) -> None:
    _SOURCES[source.name.upper()] = source
