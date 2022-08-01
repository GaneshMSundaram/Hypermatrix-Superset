# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import logging
from flask import current_app
from flask import current_app
from superset.models import core as models
from sqlalchemy import (

                create_engine,

            )
import pandas as pd
from superset.utils import core as utils, csv
from superset.db_engine_specs import BaseEngineSpec
import sqlparse
logger = logging.getLogger(__name__)


class PostgresDatabaseDAO:

    @staticmethod
    def get_dataframe(self) -> pd.DataFrame:
        db_uri = (
            current_app.config["SQLALCHEMY_KNOWLEDGE_DATABASE_URI"]
        )
        limit: int = 100,
        show_cols: bool = False,
        indent: bool = True,
        latest_partition: bool = False,
        engine = create_engine(db_uri)
        """start = engine.select_star(
            self,
            table_name,
            schema=schema,
            engine=engine,
            indent=indent,
            show_cols=False,
            limit=limit,
            latest_partition=latest_partition,

        )
         #eng = self.get_sqla_engine(schema=schema, source=utils.QuerySource.SQL_LAB)
        """

        return pd.read_sql("select * from kb_config.vw_table_col_metadata", engine)

