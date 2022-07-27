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
from collections import defaultdict

import pandas as pd
import sqlalchemy as sqlalch
import networkx as nx
from superset.databases.JoinPathAlgo_new import build_graph, join_path_algo
# For sql query builder
from UniversalSqlBuilder import UniversalSqlBuilder
from flask import current_app
import re
logger = logging.getLogger(__name__)


class SQLBuilder:
    main_select = ""
    my_dict = defaultdict(list)
    @staticmethod
    def build_sql(self, data):

        table_lis = list()
        table_lis = get_tables(data['dimensionData'], table_lis)
        table_join_lis = list()
        table_lis = get_tables(data['measureData'], table_lis)

        where_clause = data['conditionData']
        global schema
        global main_select
        global my_dict
        schema = data['schemaName']
        my_dict = defaultdict(list)
        table_lis = get_tables_from_where_clause(where_clause.split("="), table_lis)
        z = 0
        select_column = []
        db_uri = (
            current_app.config["SQLALCHEMY_KNOWLEDGE_DATABASE_URI"]
        )
        engine = sqlalch.create_engine(
            db_uri)
        sql_data_from_query = pd.read_sql(
            "Select LeftTableName as S_Col, RightTableName as T_Col, LeftColumnName as LT_Cols, RightColumnName as "
            "RT_Cols from kb_config.vw_tabledef_links_metadata where GridConfigurationName = 'kb_bi_retn_dashboard'",
            engine)
        sql_data_from_query = sql_data_from_query.rename(
            columns={"s_col": "S.Col", "t_col": "T.Col", "lt_cols": "LT_Cols",
                     "rt_cols": "RT_Cols"})
        myGraph = nx.DiGraph()  # Instantiate Graph object
        obj_build_graph = build_graph(sql_data_from_query, myGraph)
        myGraph = obj_build_graph.buildGraph()
        obj_join_path_algo = join_path_algo(myGraph)
        path, join_paths_across = obj_join_path_algo.contruct_path(
            nodes_to_be_linked=table_lis)
        if len(table_lis) > 0:
            main_select = "Select "

        z = 0
        sql2 = ""
        for x in join_paths_across:
            additional_select = ""
            left_table = get_table(0, x)  # left table
            left_table_col = re.sub(r'.*[(]|,[)]', '', x[0])
            right_table = get_table(1, x)
            right_table_col = re.sub(r'.*[(]|,[)]', '', x[1])
            if z == 0:
                select_clause = build_selectClause(
                    [v1 for v1 in data['dimensionData'] if v1['table'] == left_table],
                    left_table_col, left_table)
                agg_clause = build_aggregationClause(
                    [v1 for v1 in data['measureData'] if v1['table'] == left_table])

                where_clause_table_exists = my_dict.get(left_table)
                additional_where_select = ""
                if where_clause_table_exists is not None:
                    additional_where_select = where_clause_table_exists[:-1]

                # additional_select = build_additional_select_columns(left_table, join_paths_across, )
                if len(agg_clause) > 0:
                    agg_clause = "," + agg_clause
                if len(additional_where_select) > 0:
                    additional_where_select = "," + additional_where_select

                right_where_clause_table_exists = my_dict.get(right_table)

                right_where_clause_table_exists = ""
                if right_where_clause_table_exists is not None:
                    right_where_clause_table_exists = right_where_clause_table_exists[
                                                      :-1]
                sql2 = "(" + UniversalSqlBuilder.table(
                    schema + "." + left_table).select(
                    select_clause + agg_clause + additional_where_select).get() + " ) as " + left_table
                select_clause = build_selectClause(
                    [v1 for v1 in data['dimensionData'] if v1['table'] == right_table],
                    right_table_col, right_table)
                agg_clause = build_aggregationClause(
                    [v1 for v1 in data['measureData'] if v1['table'] == right_table])
                if len(agg_clause) > 0:
                    agg_clause = "," + agg_clause
                if len(right_where_clause_table_exists) > 0:
                    right_where_clause_table_exists = "," + right_where_clause_table_exists
                join_clause = build_join_clause(x, left_table, right_table)
                sql2 = sql2 + " inner join (" + UniversalSqlBuilder.table(
                    schema + "." + right_table).select(
                    select_clause + agg_clause + right_where_clause_table_exists).get() + " ) as " + right_table + build_join_clause(
                    x,
                    left_table,
                    right_table) + additional_select

                table_join_lis.append(left_table)
                table_join_lis.append(right_table)
            else:
                left_tab_count = table_join_lis.count(left_table)
                where_clause_table_exists = my_dict.get(left_table)
                additional_where_select = ""
                if where_clause_table_exists is not None:
                    additional_where_select = where_clause_table_exists[:-1]

                if len(additional_where_select) > 0:
                    additional_where_select = "," + additional_where_select

                if left_tab_count == 0:
                    table_join_lis.append(left_table)
                    select_clause = build_selectClause(
                        [v1 for v1 in data['dimensionData'] if
                         v1['table'] == left_table],
                        left_table_col, left_table)
                    agg_clause = build_aggregationClause(
                        [v1 for v1 in data['measureData'] if v1['table'] == left_table])
                    if len(agg_clause) > 0:
                        agg_clause = "," + agg_clause
                    sql2 = sql2 + " inner join (" + UniversalSqlBuilder.table(
                        schema + "." + left_table).select(
                        select_clause + agg_clause + additional_where_select).get() + " ) as " + left_table + build_join_clause(
                        x,
                        left_table,
                        right_table) + additional_select
                right_where_clause_table_exists = my_dict.get(right_table)
                right_where_clause_table_exists = ""
                if right_where_clause_table_exists is not None:
                    right_where_clause_table_exists = right_where_clause_table_exists[
                                                      :-1]
                right_tab_count = table_join_lis.count(right_table)
                if len(right_where_clause_table_exists) > 0:
                    right_where_clause_table_exists = "," + right_where_clause_table_exists

                if right_tab_count == 0:

                    select_clause = build_selectClause(
                        [v1 for v1 in data['dimensionData'] if
                         v1['table'] == right_table],
                        right_table_col, right_table)
                    agg_clause = build_aggregationClause(
                        [v1 for v1 in data['measureData'] if
                         v1['table'] == right_table])
                    if len(agg_clause) > 0:
                        agg_clause = "," + agg_clause
                    sql2 = sql2 + " inner join (" + UniversalSqlBuilder.table(
                        schema + "." + right_table).select(
                        select_clause + agg_clause + right_where_clause_table_exists).get() + " ) as " + right_table + build_join_clause(
                        x,
                        left_table,
                        right_table) + additional_select
            z = z + 1
        main_select = main_select[:-1] + " from (" + sql2 + ") where " + where_clause
        main_select = main_select.replace('/', '"')
        print(main_select)
        return main_select


def get_table(index, table_list):
    return re.sub(r'[(].*', '', table_list[index])


def get_tables_from_where_clause(where_clause, table_lst: list()):
    global my_dict
    for wc in where_clause:
        if '(' in wc:
            split_array = re.sub(r'.*[(]|[)]', '', wc).split(".")
            table_count = table_lst.count(split_array[0])
            already_exists = my_dict.get(split_array[0])
            if already_exists is None and table_count == 0:
                my_dict[split_array[0]] = split_array[1] + ","
            elif table_count == 0:
                my_dict[split_array[0]].append(split_array[1])
            if table_count == 0:
                table_lst.append(split_array[0])
    return table_lst


def build_join_clause(join_data, left_table, right_table):
    additional_select = ""
    inner_join_statement = " on "
    left_table_cols = re.sub(r'.*[(]|[)]', '', join_data[0]).split(",")
    right_table_cols = re.sub(r'.*[(]|[)]', '', join_data[1]).split(",")
    index = 0
    for c in left_table_cols:
        if c != "":
            inner_join_statement = inner_join_statement + left_table + "." + c + " = " + right_table + "." + \
                                   right_table_cols[index] + " and "
            additional_select = additional_select + c + "'" + c + '"'","
            index = index + 1
    return inner_join_statement[:-4]


def build_selectClause(select_list, join_column, table_name):
    joined_select_string = ""

    global main_select
    for w in select_list:
        joined_select_string = w['columns'] + "  As "'"' + w['aliasName'] + '"'" , "
        main_select = main_select + w['table'] + "."'"' + w['aliasName'] + '"'","

    left_table_cols = join_column.split(",")
    for jc in left_table_cols:
        pjc = re.sub(r'.*[(]|[)]', '', jc)
        if select_list.count(pjc) == 0 and pjc != "":
            joined_select_string = joined_select_string + pjc + " as "'"' + pjc + '"'","
    return joined_select_string[:-1]


def build_aggregationClause(select_list):
    joined_select_string = ""
    global main_select
    for w in select_list:
        joined_select_string = w['operator2'] + " (" + w['columns'] + ")  as "'"' + w[
            'aliasName'] + '"'" ,"
        main_select = main_select + w['table'] + "."'"' + w['aliasName'] + '"'","
    return joined_select_string[:-1]


def get_tables(json_data, table_lst: list()):
    for d in json_data:
        tab = d.get('table')
        table_count = table_lst.count(tab)
        if table_count == 0:
            table_lst.append(tab)
    return table_lst


def build_additional_select_columns(table_to_parse, table_join_array, column_considered_already, is_left_table):
    additional_sel_statement = ""
    for tbl in table_join_array:
        if is_left_table:
            table = get_table(0, tbl)
            index = 0
        else:
            table = get_table(1, tbl)
            index = 1
        if table == table_to_parse:
            table_cols = re.sub(r'.*[(]|[)]', '', tbl[index]).split(",")
            for c in table_cols:
                if c != column_considered_already:
                    additional_sel_statement = additional_sel_statement + table + "." + c + " ,"
    return additional_sel_statement[:-1]
