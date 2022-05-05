
from superset.databases.sql_query_builder import SqlQueryBuilder


class GetSqlQuery:

    def get_sql_query(self, 
        tables_list: dict, 
        table_list_alias: dict,
        inner_query_select_columns: dict,
        inner_query_select_columns_alias: dict,
        inner_query_select_where: dict,
        inner_query_select_groupby: dict,
        inner_query_select_aggregate: dict,
        # outer_query_select_columns: dict,
        outer_query_select_columns_alias: dict,
        table_join_path: list
        ):

        
        ################ Make list of all inner select queries ##################

        resultant_queries = ''
        inner_queries = {}
        sql_query_builder =  SqlQueryBuilder()
        for x in tables_list:
            table = tables_list[x]
            table_alias = table_list_alias[x]
            columns = inner_query_select_columns[x]
            columns_alias = inner_query_select_columns_alias[x]
            select_query = sql_query_builder.select_(table, table_alias, columns, columns_alias)
            inner_queries[x] = select_query

        #################### Code to inner join with all inner queries ###################

        is_visited_table_join = tables_list.copy()

        # make all value as false initially
        for key in is_visited_table_join:
            is_visited_table_join[key] = False

        for item in table_join_path:
            tables_name = item[0]
            columns_details = item[1]
            left_table, right_table = tables_name[0], tables_name[1]
            left_table_columns, right_table_columns = columns_details[0], columns_details[1]

            table_alias_to_join = [table_list_alias[left_table], table_list_alias[right_table]]
            sql_query_to_join = [inner_queries[left_table], inner_queries[right_table]]

            if(resultant_queries == '' and is_visited_table_join[left_table] == False and is_visited_table_join[right_table] == False):
                resultant_queries = sql_query_builder.join_(sql_query_to_join, table_alias_to_join, [left_table_columns, right_table_columns])
                is_visited_table_join[left_table] = is_visited_table_join[right_table] = True
            
            elif(is_visited_table_join[left_table] == False and is_visited_table_join[right_table] == True):
                resultant_queries = sql_query_builder.join_([resultant_queries,inner_queries[left_table]] , table_alias_to_join, [left_table_columns, right_table_columns])
                is_visited_table_join[left_table] = True

            elif(is_visited_table_join[left_table] == True and is_visited_table_join[right_table] == False):
                resultant_queries = sql_query_builder.join_([resultant_queries,inner_queries[right_table]] , table_alias_to_join, [left_table_columns, right_table_columns])
                is_visited_table_join[right_table] = True


        #################### Code to build outer most select query ###################
        resultant_queries = sql_query_builder.outer_select_in_join_(outer_query_select_columns_alias, resultant_queries)

        print(resultant_queries)
        return resultant_queries
