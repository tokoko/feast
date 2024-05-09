from urllib import parse
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, List, Literal, Optional, Union

import ibis
import pandas as pd
import pyarrow
import pyarrow as pa
from ibis.expr.types import Table
from pydantic import StrictStr

from feast.data_format import DeltaFormat, ParquetFormat
from feast.data_source import DataSource
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.ibis import (
    get_historical_features_ibis,
    offline_write_batch_ibis,
    pull_all_from_table_or_query_ibis,
    pull_latest_from_table_or_query_ibis,
    write_logged_features_ibis,
)
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.type_map import pa_to_mssql_type
from feast.infra.offline_stores.contrib.mssql_offline_store.mssqlserver_source import (
    MsSqlServerSource,
)

def get_ibis_connection(config: RepoConfig):
    connection_params = parse.urlparse(config.offline_store.connection_string)
    additional_kwargs = dict(parse.parse_qsl(connection_params.query))
    return ibis.mssql.connect(
        user=connection_params.username,
        password=connection_params.password,
        host=connection_params.hostname,
        port=connection_params.port,
        database=connection_params.path.strip('/'),
        **additional_kwargs,
    )

def _build_data_source_reader(config: RepoConfig):
    con = get_ibis_connection(config)

    def _read_data_source(data_source: DataSource) -> Table:
        assert isinstance(data_source, MsSqlServerSource)
        return con.table(data_source.table_ref)

    return _read_data_source


# TODO
def _write_data_source(table: pyarrow.Table, data_source: DataSource):
    pass


class MsSqlServerOfflineStoreConfig(FeastConfigBaseModel):
    """Offline store config for SQL Server"""

    type: Literal["mssql"] = "mssql"
    """ Offline store type selector"""

    connection_string: StrictStr = "mssql+pyodbc://sa:yourStrong(!)Password@localhost:1433/feast_test?driver=ODBC+Driver+17+for+SQL+Server"
    """Connection string containing the host, port, and configuration parameters for SQL Server
     format: SQLAlchemy connection string, e.g. mssql+pyodbc://sa:yourStrong(!)Password@localhost:1433/feast_test?driver=ODBC+Driver+17+for+SQL+Server"""


class MsSqlServerOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        return pull_latest_from_table_or_query_ibis(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
            data_source_reader=_build_data_source_reader(config),
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        # TODO avoid this conversion
        if type(entity_df) == str:
            con = get_ibis_connection(config)
            entity_df = con.sql(entity_df).execute()

        return get_historical_features_ibis(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
            data_source_reader=_build_data_source_reader(config),
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        return pull_all_from_table_or_query_ibis(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            start_date=start_date,
            end_date=end_date,
            data_source_reader=_build_data_source_reader(config),
        )

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        offline_write_batch_ibis(
            config=config,
            feature_view=feature_view,
            table=table,
            progress=progress,
            data_source_writer=_write_data_source,
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        write_logged_features_ibis(
            config=config,
            data=data,
            source=source,
            logging_config=logging_config,
            registry=registry,
        )
