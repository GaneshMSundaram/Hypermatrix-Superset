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
# pylint: disable=too-many-lines
import json
import logging
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional
from zipfile import ZipFile

from flask import g, request, Response, send_file
from flask_appbuilder.api import expose, protect, rison, safe
from flask_appbuilder.models.sqla.interface import SQLAInterface
from marshmallow import ValidationError
from sqlalchemy.exc import NoSuchTableError, OperationalError, SQLAlchemyError

from superset import (
    app,
    appbuilder,
    conf,
    db,
    event_logger,
    is_feature_enabled,
    results_backend,
    results_backend_use_msgpack,
    sql_lab,
    viz,
)
from superset.utils import core as utils, csv
from superset.commands.importers.exceptions import NoValidFilesFoundError
from superset.commands.importers.v1.utils import get_contents_from_bundle
from superset.constants import MODEL_API_RW_METHOD_PERMISSION_MAP, RouteMethod
from superset.databases.commands.create import CreateDatabaseCommand
from superset.databases.commands.delete import DeleteDatabaseCommand
from superset.databases.commands.exceptions import (
    DatabaseConnectionFailedError,
    DatabaseCreateFailedError,
    DatabaseDeleteDatasetsExistFailedError,
    DatabaseDeleteFailedError,
    DatabaseInvalidError,
    DatabaseNotFoundError,
    DatabaseUpdateFailedError,
    InvalidParametersError,
)
from superset.databases.commands.export import ExportDatabasesCommand
from superset.databases.commands.importers.dispatcher import ImportDatabasesCommand
from superset.databases.commands.test_connection import TestConnectionDatabaseCommand
from superset.databases.commands.update import UpdateDatabaseCommand
from superset.databases.commands.validate import ValidateDatabaseParametersCommand
from superset.databases.dao import DatabaseDAO
from superset.databases.decorators import check_datasource_access
from superset.databases.filters import DatabaseFilter
from superset.databases.schemas import (
    database_schemas_query_schema,
    DatabaseFunctionNamesResponse,
    DatabasePostSchema,
    DatabasePutSchema,
    DatabaseRelatedObjectsResponse,
    DatabaseTestConnectionSchema,
    DatabaseValidateParametersSchema,
    get_export_ids_schema,
    SchemasResponseSchema,
    SelectStarResponseSchema,
    TableMetadataResponseSchema,
)
from superset.databases.utils import get_table_metadata
from superset.db_engine_specs import get_available_engine_specs
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType
from superset.extensions import security_manager
from superset.models.core import Database
from superset.typing import FlaskResponse
from superset.utils.core import error_msg_from_exception
from superset.views.base import json_error_response, json_success
from superset.views.base_api import (
    BaseSupersetModelRestApi,
    requires_form_data,
    requires_json,
    statsd_metrics,
)
import urllib.request

from superset.views.core import Superset
from superset.databases.postgresdao import PostgresDatabaseDAO
from superset.databases.sqlBuilder import SQLBuilder

logger = logging.getLogger(__name__)


class DatabaseRestApi(BaseSupersetModelRestApi):
    datamodel = SQLAInterface(Database)

    include_route_methods = RouteMethod.REST_MODEL_VIEW_CRUD_SET | {
        RouteMethod.EXPORT,
        RouteMethod.IMPORT,
        "table_metadata",
        "select_star",
        "schemas",
        "test_connection",
        "related_objects",
        "function_names",
        "available",
        "validate_parameters",
        "schemas_greeting",
        "database_metadata",
        "sqlbuilder_metadata"
    }
    resource_name = "database"
    class_permission_name = "Database"
    method_permission_name = MODEL_API_RW_METHOD_PERMISSION_MAP
    allow_browser_login = True
    base_filters = [["id", DatabaseFilter, lambda: []]]
    show_columns = [
        "id",
        "database_name",
        "cache_timeout",
        "expose_in_sqllab",
        "allow_run_async",
        "allow_file_upload",
        "configuration_method",
        "allow_ctas",
        "allow_cvas",
        "allow_dml",
        "backend",
        "force_ctas_schema",
        "allow_multi_schema_metadata_fetch",
        "impersonate_user",
        "encrypted_extra",
        "extra",
        "parameters",
        "parameters_schema",
        "server_cert",
        "sqlalchemy_uri",
    ]
    list_columns = [
        "allow_file_upload",
        "allow_ctas",
        "allow_cvas",
        "allow_dml",
        "allow_multi_schema_metadata_fetch",
        "allow_run_async",
        "allows_cost_estimate",
        "allows_subquery",
        "allows_virtual_table_explore",
        "backend",
        "changed_on",
        "changed_on_delta_humanized",
        "created_by.first_name",
        "created_by.last_name",
        "database_name",
        "explore_database_id",
        "expose_in_sqllab",
        "extra",
        "force_ctas_schema",
        "id",
    ]
    add_columns = [
        "database_name",
        "sqlalchemy_uri",
        "cache_timeout",
        "expose_in_sqllab",
        "allow_run_async",
        "allow_file_upload",
        "allow_ctas",
        "allow_cvas",
        "allow_dml",
        "configuration_method",
        "force_ctas_schema",
        "impersonate_user",
        "allow_multi_schema_metadata_fetch",
        "extra",
        "encrypted_extra",
        "server_cert",
    ]
    edit_columns = add_columns

    list_select_columns = list_columns + ["extra", "sqlalchemy_uri", "password"]
    order_columns = [
        "allow_file_upload",
        "allow_dml",
        "allow_run_async",
        "changed_on",
        "changed_on_delta_humanized",
        "created_by.first_name",
        "database_name",
        "expose_in_sqllab",
    ]
    # Removes the local limit for the page size
    max_page_size = -1
    add_model_schema = DatabasePostSchema()
    edit_model_schema = DatabasePutSchema()

    apispec_parameter_schemas = {
        "database_schemas_query_schema": database_schemas_query_schema,
        "get_export_ids_schema": get_export_ids_schema,
    }
    openapi_spec_tag = "Database"
    openapi_spec_component_schemas = (
        DatabaseFunctionNamesResponse,
        DatabaseRelatedObjectsResponse,
        DatabaseTestConnectionSchema,
        DatabaseValidateParametersSchema,
        TableMetadataResponseSchema,
        SelectStarResponseSchema,
        SchemasResponseSchema,
    )

    @expose("/", methods=["POST"])
    @protect()
    @safe
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}.post",
        log_to_statsd=False,
    )
    @requires_json
    def post(self) -> Response:
        """Creates a new Database
        ---
        post:
          description: >-
            Create a new Database.
          requestBody:
            description: Database schema
            required: true
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/{{self.__class__.__name__}}.post'
          responses:
            201:
              description: Database added
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      id:
                        type: number
                      result:
                        $ref: '#/components/schemas/{{self.__class__.__name__}}.post'
            302:
              description: Redirects to the current digest
            400:
              $ref: '#/components/responses/400'
            401:
              $ref: '#/components/responses/401'
            404:
              $ref: '#/components/responses/404'
            500:
              $ref: '#/components/responses/500'
        """
        try:
            item = self.add_model_schema.load(request.json)
        # This validates custom Schema with custom validations
        except ValidationError as error:
            return self.response_400(message=error.messages)
        try:
            new_model = CreateDatabaseCommand(g.user, item).run()
            # Return censored version for sqlalchemy URI
            item["sqlalchemy_uri"] = new_model.sqlalchemy_uri
            item["expose_in_sqllab"] = new_model.expose_in_sqllab

            # If parameters are available return them in the payload
            if new_model.parameters:
                item["parameters"] = new_model.parameters

            return self.response(201, id=new_model.id, result=item)
        except DatabaseInvalidError as ex:
            return self.response_422(message=ex.normalized_messages())
        except DatabaseConnectionFailedError as ex:
            return self.response_422(message=str(ex))
        except DatabaseCreateFailedError as ex:
            logger.error(
                "Error creating model %s: %s",
                self.__class__.__name__,
                str(ex),
                exc_info=True,
            )
            return self.response_422(message=str(ex))

    @expose("/<int:pk>", methods=["PUT"])
    @protect()
    @safe
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}.put",
        log_to_statsd=False,
    )
    @requires_json
    def put(self, pk: int) -> Response:
        """Changes a Database
        ---
        put:
          description: >-
            Changes a Database.
          parameters:
          - in: path
            schema:
              type: integer
            name: pk
          requestBody:
            description: Database schema
            required: true
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/{{self.__class__.__name__}}.put'
          responses:
            200:
              description: Database changed
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      id:
                        type: number
                      result:
                        $ref: '#/components/schemas/{{self.__class__.__name__}}.put'
            400:
              $ref: '#/components/responses/400'
            401:
              $ref: '#/components/responses/401'
            403:
              $ref: '#/components/responses/403'
            404:
              $ref: '#/components/responses/404'
            422:
              $ref: '#/components/responses/422'
            500:
              $ref: '#/components/responses/500'
        """
        try:
            item = self.edit_model_schema.load(request.json)
        # This validates custom Schema with custom validations
        except ValidationError as error:
            return self.response_400(message=error.messages)
        try:
            changed_model = UpdateDatabaseCommand(g.user, pk, item).run()
            # Return censored version for sqlalchemy URI
            item["sqlalchemy_uri"] = changed_model.sqlalchemy_uri
            if changed_model.parameters:
                item["parameters"] = changed_model.parameters
            return self.response(200, id=changed_model.id, result=item)
        except DatabaseNotFoundError:
            return self.response_404()
        except DatabaseInvalidError as ex:
            return self.response_422(message=ex.normalized_messages())
        except DatabaseConnectionFailedError as ex:
            return self.response_422(message=str(ex))
        except DatabaseUpdateFailedError as ex:
            logger.error(
                "Error updating model %s: %s",
                self.__class__.__name__,
                str(ex),
                exc_info=True,
            )
            return self.response_422(message=str(ex))

    @expose("/<int:pk>", methods=["DELETE"])
    @protect()
    @safe
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}" f".delete",
        log_to_statsd=False,
    )
    def delete(self, pk: int) -> Response:
        """Deletes a Database
        ---
        delete:
          description: >-
            Deletes a Database.
          parameters:
          - in: path
            schema:
              type: integer
            name: pk
          responses:
            200:
              description: Database deleted
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      message:
                        type: string
            401:
              $ref: '#/components/responses/401'
            403:
              $ref: '#/components/responses/403'
            404:
              $ref: '#/components/responses/404'
            422:
              $ref: '#/components/responses/422'
            500:
              $ref: '#/components/responses/500'
        """
        try:
            DeleteDatabaseCommand(g.user, pk).run()
            return self.response(200, message="OK")
        except DatabaseNotFoundError:
            return self.response_404()
        except DatabaseDeleteDatasetsExistFailedError as ex:
            return self.response_422(message=str(ex))
        except DatabaseDeleteFailedError as ex:
            logger.error(
                "Error deleting model %s: %s",
                self.__class__.__name__,
                str(ex),
                exc_info=True,
            )
            return self.response_422(message=str(ex))

    @expose("/<int:pk>/schemas_greeting/<schema_name>")
    @protect()
    @safe
    @rison(database_schemas_query_schema)
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}" f".schemas",
        log_to_statsd=False,
    )
    def schemas_greeting(self, pk: int, schema_name: str, **kwargs: Any) -> FlaskResponse:
       return self.response(200, message="Hello")

       ########################################## Get Database metadata #########################################

    @expose("/database_metadata/")
    @protect()
    @safe
    # @rison(database_schemas_query_schema)
    # @check_datasource_access
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args,
                      **kwargs: f"{self.__class__.__name__}" f".database_metadata",
        log_to_statsd=False,
    )
    def database_metadata(
        self, **kwargs: Any
    ) -> FlaskResponse:
        """Database metadata
          ---
          get:
            description: Endpoint to fetch database metadata including tables & columns of each table
            responses:
              200:
                description: JSON database metadata
                content:
                  application/json:
                    schema:
                      $ref: "#/components/schemas/TableMetadataResponseSchema"
              400:
                $ref: '#/components/responses/400'
              401:
                $ref: '#/components/responses/401'
              404:
                $ref: '#/components/responses/404'
              422:
                $ref: '#/components/responses/422'
              500:
                $ref: '#/components/responses/500'
          """
        try:
            schema_name = "kb_config"
            df = PostgresDatabaseDAO.get_dataframe(self)
            if df.empty:
                return {}

            group_grid_name = df.groupby(['gridConfigurationId'])
            unq_schemas = df['gridConfigurationId'].unique()
            schema_name_payload: List[Dict[str, Any]] = []

            for name, group in group_grid_name:
                group_table_name = group.groupby('targetTableName')
                payload_tables: List[Dict[str, Any]] = []
                for (idx, table_row) in group_table_name:
                    payload_columns: List[Dict[str, Any]] = []
                    for i, row in table_row.iterrows():
                        grid_name = row['gridConfigurationName']
                        payload_columns.append({
                            "name": row['columnName'],
                            "label": row['labelName'],
                            "type": row['DataTypeName'],
                            "longType": row['DataTypeName'],
                            "keys": [],
                            "comment": None})
                    table_name = idx
                    table = {
                        "value": table_name,
                        "schema": schema_name,
                        "title": table_name,
                        "label": table_name,
                        "type": 'table',
                        "extra": None,
                        "columns": payload_columns}

                    payload_tables.append(table)
                schema = {
                    "value": name,
                    "schema": grid_name,
                    "type": 'schema',
                    "tables": payload_tables,

                }

                schema_name_payload.append(schema)
            table_final_payload = {
                "schemalength": len(unq_schemas),
                "options": schema_name_payload
            }

            return json_success(json.dumps(table_final_payload))
        except SQLAlchemyError as ex:
            self.incr_stats("error", self.table_metadata.__name__)
        except Exception as ex:
            logger.error(
                "Error when fetching table and column metadata %s: %s",
                self.__class__.__name__,
                str(ex),
                exc_info=True,
            )
            return self.response_422(message=str(ex))

    @expose("/sqlbuilder_metadata/<sql_json>")
    @protect()
    @safe
    # @rison(database_schemas_query_schema)
    # @check_datasource_access
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args,
                      **kwargs: f"{self.__class__.__name__}" f".sqlbuilder_metadata",
        log_to_statsd=False,
    )
    def sqlbuilder_metadata(
        self, sql_json: str, **kwargs: Any
    ) -> FlaskResponse:
        """SQL Builder
                ---
                get:
                  description: Get database table metadata
                  parameters:
                  - in: path
                    schema:
                      type: string
                    name: sql_json
                    description: The SQL Json schema
                  responses:
                    200:
                      description: Table metadata information
                      content:
                        application/json:
                          schema:
                            $ref: "#/components/schemas/TableMetadataResponseSchema"
                    400:
                      $ref: '#/components/responses/400'
                    401:
                      $ref: '#/components/responses/401'
                    404:
                      $ref: '#/components/responses/404'
                    422:
                      $ref: '#/components/responses/422'
                    500:
                      $ref: '#/components/responses/500'
                """
        try:
            # print(json.load(sql_json))
            print(json.loads(sql_json))
            return json_success(
                json.dumps(SQLBuilder.build_sql(self, json.loads(sql_json))))
        except SQLAlchemyError as ex:
            self.incr_stats("error", self.table_metadata.__name__)
        except Exception as ex:
            logger.error(
                "Error when fetching table and column metadata %s: %s",
                self.__class__.__name__,
                str(ex),
                exc_info=True,
            )
            return self.response_422(message=str(ex))

    @expose("/<int:pk>/schemas/")
    @protect()
    @safe
    @rison(database_schemas_query_schema)
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}" f".schemas",
        log_to_statsd=False,
    )
    def schemas(self, pk: int, **kwargs: Any) -> FlaskResponse:
        """Get all schemas from a database
        ---
        get:
          description: Get all schemas from a database
          parameters:
          - in: path
            schema:
              type: integer
            name: pk
            description: The database id
          - in: query
            name: q
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/database_schemas_query_schema'
          responses:
            200:
              description: A List of all schemas from the database
              content:
                application/json:
                  schema:
                    $ref: "#/components/schemas/SchemasResponseSchema"
            400:
              $ref: '#/components/responses/400'
            401:
              $ref: '#/components/responses/401'
            404:
              $ref: '#/components/responses/404'
            500:
              $ref: '#/components/responses/500'
        """
        database = self.datamodel.get(pk, self._base_filters)
        if not database:
            return self.response_404()
        try:
            schemas = database.get_all_schema_names(
                cache=database.schema_cache_enabled,
                cache_timeout=database.schema_cache_timeout,
                force=kwargs["rison"].get("force", False),
            )
            schemas = security_manager.get_schemas_accessible_by_user(database, schemas)
            return self.response(200, result=schemas)
        except OperationalError:
            return self.response(
                500, message="There was an error connecting to the database"
            )

    @expose("/<int:pk>/table/<table_name>/<schema_name>/", methods=["GET"])
    @protect()
    @check_datasource_access
    @safe
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}"
        f".table_metadata",
        log_to_statsd=False,
    )
    def table_metadata(
        self, database: Database, table_name: str, schema_name: str
    ) -> FlaskResponse:
        """Table schema info
        ---
        get:
          description: Get database table metadata
          parameters:
          - in: path
            schema:
              type: integer
            name: pk
            description: The database id
          - in: path
            schema:
              type: string
            name: table_name
            description: Table name
          - in: path
            schema:
              type: string
            name: schema_name
            description: Table schema
          responses:
            200:
              description: Table metadata information
              content:
                application/json:
                  schema:
                    $ref: "#/components/schemas/TableMetadataResponseSchema"
            400:
              $ref: '#/components/responses/400'
            401:
              $ref: '#/components/responses/401'
            404:
              $ref: '#/components/responses/404'
            422:
              $ref: '#/components/responses/422'
            500:
              $ref: '#/components/responses/500'
        """
        self.incr_stats("init", self.table_metadata.__name__)
        try:
            table_info = get_table_metadata(database, table_name, schema_name)
        except SQLAlchemyError as ex:
            self.incr_stats("error", self.table_metadata.__name__)
            return self.response_422(error_msg_from_exception(ex))
        self.incr_stats("success", self.table_metadata.__name__)
        return self.response(200, **table_info)

    @expose("/<int:pk>/select_star/<table_name>/", methods=["GET"])
    @expose("/<int:pk>/select_star/<table_name>/<schema_name>/", methods=["GET"])
    @protect()
    @check_datasource_access
    @safe
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}.select_star",
        log_to_statsd=False,
    )
    def select_star(
        self, database: Database, table_name: str, schema_name: Optional[str] = None
    ) -> FlaskResponse:
        """Table schema info
        ---
        get:
          description: Get database select star for table
          parameters:
          - in: path
            schema:
              type: integer
            name: pk
            description: The database id
          - in: path
            schema:
              type: string
            name: table_name
            description: Table name
          - in: path
            schema:
              type: string
            name: schema_name
            description: Table schema
          responses:
            200:
              description: SQL statement for a select star for table
              content:
                application/json:
                  schema:
                    $ref: "#/components/schemas/SelectStarResponseSchema"
            400:
              $ref: '#/components/responses/400'
            401:
              $ref: '#/components/responses/401'
            404:
              $ref: '#/components/responses/404'
            422:
              $ref: '#/components/responses/422'
            500:
              $ref: '#/components/responses/500'
        """
        self.incr_stats("init", self.select_star.__name__)
        try:
            result = database.select_star(
                table_name, schema_name, latest_partition=True, show_cols=True
            )
        except NoSuchTableError:
            self.incr_stats("error", self.select_star.__name__)
            return self.response(404, message="Table not found on the database")
        self.incr_stats("success", self.select_star.__name__)
        return self.response(200, result=result)

    @expose("/test_connection", methods=["POST"])
    @protect()
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}"
        f".test_connection",
        log_to_statsd=False,
    )
    @requires_json
    def test_connection(self) -> FlaskResponse:
        """Tests a database connection
        ---
        post:
          description: >-
            Tests a database connection
          requestBody:
            description: Database schema
            required: true
            content:
              application/json:
                schema:
                  $ref: "#/components/schemas/DatabaseTestConnectionSchema"
          responses:
            200:
              description: Database Test Connection
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      message:
                        type: string
            400:
              $ref: '#/components/responses/400'
            422:
              $ref: '#/components/responses/422'
            500:
              $ref: '#/components/responses/500'
        """
        try:
            item = DatabaseTestConnectionSchema().load(request.json)
        # This validates custom Schema with custom validations
        except ValidationError as error:
            return self.response_400(message=error.messages)
        TestConnectionDatabaseCommand(g.user, item).run()
        return self.response(200, message="OK")

    @expose("/<int:pk>/related_objects/", methods=["GET"])
    @protect()
    @safe
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}"
        f".related_objects",
        log_to_statsd=False,
    )
    def related_objects(self, pk: int) -> Response:
        """Get charts and dashboards count associated to a database
        ---
        get:
          description:
            Get charts and dashboards count associated to a database
          parameters:
          - in: path
            name: pk
            schema:
              type: integer
          responses:
            200:
            200:
              description: Query result
              content:
                application/json:
                  schema:
                    $ref: "#/components/schemas/DatabaseRelatedObjectsResponse"
            401:
              $ref: '#/components/responses/401'
            404:
              $ref: '#/components/responses/404'
            500:
              $ref: '#/components/responses/500'
        """
        database = DatabaseDAO.find_by_id(pk)
        if not database:
            return self.response_404()
        data = DatabaseDAO.get_related_objects(pk)
        charts = [
            {
                "id": chart.id,
                "slice_name": chart.slice_name,
                "viz_type": chart.viz_type,
            }
            for chart in data["charts"]
        ]
        dashboards = [
            {
                "id": dashboard.id,
                "json_metadata": dashboard.json_metadata,
                "slug": dashboard.slug,
                "title": dashboard.dashboard_title,
            }
            for dashboard in data["dashboards"]
        ]
        sqllab_tab_states = [
            {"id": tab_state.id, "label": tab_state.label, "active": tab_state.active}
            for tab_state in data["sqllab_tab_states"]
        ]
        return self.response(
            200,
            charts={"count": len(charts), "result": charts},
            dashboards={"count": len(dashboards), "result": dashboards},
            sqllab_tab_states={
                "count": len(sqllab_tab_states),
                "result": sqllab_tab_states,
            },
        )

    @expose("/export/", methods=["GET"])
    @protect()
    @safe
    @statsd_metrics
    @rison(get_export_ids_schema)
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}.export",
        log_to_statsd=False,
    )
    def export(self, **kwargs: Any) -> Response:
        """Export database(s) with associated datasets
        ---
        get:
          description: Download database(s) and associated dataset(s) as a zip file
          parameters:
          - in: query
            name: q
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/get_export_ids_schema'
          responses:
            200:
              description: A zip file with database(s) and dataset(s) as YAML
              content:
                application/zip:
                  schema:
                    type: string
                    format: binary
            401:
              $ref: '#/components/responses/401'
            404:
              $ref: '#/components/responses/404'
            500:
              $ref: '#/components/responses/500'
        """
        token = request.args.get("token")
        requested_ids = kwargs["rison"]
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        root = f"database_export_{timestamp}"
        filename = f"{root}.zip"

        buf = BytesIO()
        with ZipFile(buf, "w") as bundle:
            try:
                for file_name, file_content in ExportDatabasesCommand(
                    requested_ids
                ).run():
                    with bundle.open(f"{root}/{file_name}", "w") as fp:
                        fp.write(file_content.encode())
            except DatabaseNotFoundError:
                return self.response_404()
        buf.seek(0)

        response = send_file(
            buf,
            mimetype="application/zip",
            as_attachment=True,
            attachment_filename=filename,
        )
        if token:
            response.set_cookie(token, "done", max_age=600)
        return response

    @expose("/import/", methods=["POST"])
    @protect()
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}.import_",
        log_to_statsd=False,
    )
    @requires_form_data
    def import_(self) -> Response:
        """Import database(s) with associated datasets
        ---
        post:
          requestBody:
            required: true
            content:
              multipart/form-data:
                schema:
                  type: object
                  properties:
                    formData:
                      description: upload file (ZIP)
                      type: string
                      format: binary
                    passwords:
                      description: >-
                        JSON map of passwords for each featured database in the
                        ZIP file. If the ZIP includes a database config in the path
                        `databases/MyDatabase.yaml`, the password should be provided
                        in the following format:
                        `{"databases/MyDatabase.yaml": "my_password"}`.
                      type: string
                    overwrite:
                      description: overwrite existing databases?
                      type: boolean
          responses:
            200:
              description: Database import result
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      message:
                        type: string
            400:
              $ref: '#/components/responses/400'
            401:
              $ref: '#/components/responses/401'
            422:
              $ref: '#/components/responses/422'
            500:
              $ref: '#/components/responses/500'
        """
        upload = request.files.get("formData")
        if not upload:
            return self.response_400()
        with ZipFile(upload) as bundle:
            contents = get_contents_from_bundle(bundle)

        if not contents:
            raise NoValidFilesFoundError()

        passwords = (
            json.loads(request.form["passwords"])
            if "passwords" in request.form
            else None
        )
        overwrite = request.form.get("overwrite") == "true"

        command = ImportDatabasesCommand(
            contents, passwords=passwords, overwrite=overwrite
        )
        command.run()
        return self.response(200, message="OK")

    @expose("/<int:pk>/function_names/", methods=["GET"])
    @protect()
    @safe
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}"
        f".function_names",
        log_to_statsd=False,
    )
    def function_names(self, pk: int) -> Response:
        """Get function names supported by a database
        ---
        get:
          description:
            Get function names supported by a database
          parameters:
          - in: path
            name: pk
            schema:
              type: integer
          responses:
            200:
              description: Query result
              content:
                application/json:
                  schema:
                    $ref: "#/components/schemas/DatabaseFunctionNamesResponse"
            401:
              $ref: '#/components/responses/401'
            404:
              $ref: '#/components/responses/404'
            500:
              $ref: '#/components/responses/500'
        """
        database = DatabaseDAO.find_by_id(pk)
        if not database:
            return self.response_404()
        return self.response(200, function_names=database.function_names,)

    @expose("/available/", methods=["GET"])
    @protect()
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}" f".available",
        log_to_statsd=False,
    )
    def available(self) -> Response:
        """Return names of databases currently available
        ---
        get:
          description:
            Get names of databases currently available
          responses:
            200:
              description: Database names
              content:
                application/json:
                  schema:
                    type: array
                    items:
                      type: object
                      properties:
                        name:
                          description: Name of the database
                          type: string
                        engine:
                          description: Name of the SQLAlchemy engine
                          type: string
                        available_drivers:
                          description: Installed drivers for the engine
                          type: array
                          items:
                            type: string
                        default_driver:
                          description: Default driver for the engine
                          type: string
                        preferred:
                          description: Is the database preferred?
                          type: boolean
                        sqlalchemy_uri_placeholder:
                          description: Example placeholder for the SQLAlchemy URI
                          type: string
                        parameters:
                          description: JSON schema defining the needed parameters
                          type: object
            400:
              $ref: '#/components/responses/400'
            500:
              $ref: '#/components/responses/500'
        """
        preferred_databases: List[str] = app.config.get("PREFERRED_DATABASES", [])
        available_databases = []
        for engine_spec, drivers in get_available_engine_specs().items():
            if not drivers:
                continue

            payload: Dict[str, Any] = {
                "name": engine_spec.engine_name,
                "engine": engine_spec.engine,
                "available_drivers": sorted(drivers),
                "preferred": engine_spec.engine_name in preferred_databases,
            }

            if hasattr(engine_spec, "default_driver"):
                payload["default_driver"] = engine_spec.default_driver  # type: ignore

            # show configuration parameters for DBs that support it
            if (
                hasattr(engine_spec, "parameters_json_schema")
                and hasattr(engine_spec, "sqlalchemy_uri_placeholder")
                and getattr(engine_spec, "default_driver") in drivers
            ):
                payload[
                    "parameters"
                ] = engine_spec.parameters_json_schema()  # type: ignore
                payload[
                    "sqlalchemy_uri_placeholder"
                ] = engine_spec.sqlalchemy_uri_placeholder  # type: ignore

            available_databases.append(payload)

        # sort preferred first
        response = sorted(
            (payload for payload in available_databases if payload["preferred"]),
            key=lambda payload: preferred_databases.index(payload["name"]),
        )

        # add others
        response.extend(
            sorted(
                (
                    payload
                    for payload in available_databases
                    if not payload["preferred"]
                ),
                key=lambda payload: payload["name"],
            )
        )

        return self.response(200, databases=response)

    @expose("/validate_parameters", methods=["POST"])
    @protect()
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}"
        f".validate_parameters",
        log_to_statsd=False,
    )
    @requires_json
    def validate_parameters(self) -> FlaskResponse:
        """validates database connection parameters
        ---
        post:
          description: >-
            Validates parameters used to connect to a database
          requestBody:
            description: DB-specific parameters
            required: true
            content:
              application/json:
                schema:
                  $ref: "#/components/schemas/DatabaseValidateParametersSchema"
          responses:
            200:
              description: Database Test Connection
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      message:
                        type: string
            400:
              $ref: '#/components/responses/400'
            422:
              $ref: '#/components/responses/422'
            500:
              $ref: '#/components/responses/500'
        """
        try:
            payload = DatabaseValidateParametersSchema().load(request.json)
        except ValidationError as ex:
            errors = [
                SupersetError(
                    message="\n".join(messages),
                    error_type=SupersetErrorType.INVALID_PAYLOAD_SCHEMA_ERROR,
                    level=ErrorLevel.ERROR,
                    extra={"invalid": [attribute]},
                )
                for attribute, messages in ex.messages.items()
            ]
            raise InvalidParametersError(errors) from ex

        command = ValidateDatabaseParametersCommand(g.user, payload)
        command.run()
        return self.response(200, message="OK")


    #Method to fetch the list of tables for given database
    @staticmethod
    def get_table_list(self,  db_id: int,
        schema: str,
        substr: str,
        force_refresh: str = "false",
        exact_match: str = "false",):

      # Guarantees database filtering by security access
      query = db.session.query(Database)
      query = DatabaseFilter("id", SQLAInterface(Database, db.session)).apply(
            query, None
        )
      database = query.filter_by(id=db_id).one_or_none()
      if not database:
        return []

      force_refresh_parsed = force_refresh.lower() == "true"
      exact_match_parsed = exact_match.lower() == "true"
      schema_parsed = utils.parse_js_uri_path_item(schema, eval_undefined=True)
      substr_parsed = utils.parse_js_uri_path_item(substr, eval_undefined=True)

      if schema_parsed:
          tables = (
              database.get_all_table_names_in_schema(
                  schema=schema_parsed,
                  force=force_refresh_parsed,
                  cache=database.table_cache_enabled,
                  cache_timeout=database.table_cache_timeout,
              )
              or []
          )
          views = (
              database.get_all_view_names_in_schema(
                  schema=schema_parsed,
                  force=force_refresh_parsed,
                  cache=database.table_cache_enabled,
                  cache_timeout=database.table_cache_timeout,
              )
              or []
          )
      else:
          tables = database.get_all_table_names_in_database(
              cache=True, force=False, cache_timeout=24 * 60 * 60
          )
          views = database.get_all_view_names_in_database(
              cache=True, force=False, cache_timeout=24 * 60 * 60
          )
      tables = security_manager.get_datasources_accessible_by_user(
          database, tables, schema_parsed
      )
      views = security_manager.get_datasources_accessible_by_user(
          database, views, schema_parsed
      )

      def get_datasource_label(ds_name: utils.DatasourceName) -> str:
          return (
              ds_name.table if schema_parsed else f"{ds_name.schema}.{ds_name.table}"
          )

      def is_match(src: str, target: utils.DatasourceName) -> bool:
          target_label = get_datasource_label(target)
          if exact_match_parsed:
              return src == target_label
          return src in target_label

      if substr_parsed:
          tables = [tn for tn in tables if is_match(substr_parsed, tn)]
          views = [vn for vn in views if is_match(substr_parsed, vn)]

      if not schema_parsed and database.default_schemas:
          user_schemas = (
              [g.user.email.split("@")[0]] if hasattr(g.user, "email") else []
          )
          valid_schemas = set(database.default_schemas + user_schemas)

          tables = [tn for tn in tables if tn.schema in valid_schemas]
          views = [vn for vn in views if vn.schema in valid_schemas]

      max_items = app.config["MAX_TABLE_NAMES"] or len(tables)
      total_items = len(tables) + len(views)
      max_tables = len(tables)
      max_views = len(views)
      if total_items and substr_parsed:
          max_tables = max_items * len(tables) // total_items
          max_views = max_items * len(views) // total_items

      dataset_tables = {table.name: table for table in database.tables}

      table_options = [
          {
              "value": tn.table,
              "schema": tn.schema,
              "label": get_datasource_label(tn),
              "title": get_datasource_label(tn),
              "type": "table",
              "extra": dataset_tables[f"{tn.schema}.{tn.table}"].extra_dict
              if (f"{tn.schema}.{tn.table}" in dataset_tables)
              else None,
          }
          for tn in tables[:max_tables]
      ]
      table_options.extend(
          [
              {
                  "value": vn.table,
                  "schema": vn.schema,
                  "label": get_datasource_label(vn),
                  "title": get_datasource_label(vn),
                  "type": "view",
              }
              for vn in views[:max_views]
          ]
      )
      table_options.sort(key=lambda value: value["label"])
      payload = {"tableLength": len(tables) + len(views), "options": table_options}
      return payload
