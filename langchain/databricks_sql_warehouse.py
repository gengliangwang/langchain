"""Databricks SQL connector"""
from __future__ import annotations

import os
from typing import Any, Optional

from sql_database import SQLDatabase


class DatabricksSQLWarehouse(SQLDatabase):
    """Databricks SQL connector"""

    @classmethod
    def create(
        cls,
        hostname: str,
        http_path: str,
        databricks_token: Optional[str],
        catalog: str = "spark_catalog",
        schema: str = "default",
        **kwargs: Any,
    ) -> SQLDatabase:
        try:
            from databricks import sql
        except ImportError:
            raise ValueError(
                "databricks-sql-connector package not found, please install with `pip install databricks-sql-connector`"
            )

        _databricks_token = (
            databricks_token
            if databricks_token is not None
            else os.getenv("DATABRICKS_TOKEN")
        )
        if _databricks_token is None:
            raise ValueError(
                "Did not find databricks_token, please add an environment variable `DATABRICKS_TOKEN`" +
                " which contains it, or pass `databricks_token` as a named parameter."
            )

        database_uri = (
            f"databricks://token:{_databricks_token}@{hostname}?"
            + f"http_path={http_path}&catalog={catalog}&schema={schema}"
        )
        return super().from_uri(database_uri=database_uri, engine_args=None, **kwargs)
