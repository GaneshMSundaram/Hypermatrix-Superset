

class SqlQueryBuilder:

    ################################ select statement ################################
    def select_(self, table_name: str, table_name_alias: str, columns: list, columns_alias: list):
        columns_to_be_selected = ''
        for column,alias in zip(columns,columns_alias):
            x = table_name + '.' + column +  ' as ' + "'" + alias + "'" + ','
            columns_to_be_selected += x
        
        # remove last comma
        columns_to_be_selected = columns_to_be_selected[:-1]
        select = '(SELECT ' + columns_to_be_selected + ' FROM ' + table_name + ') as ' + table_name_alias 
        return select

    ################################ Join statement ################################
    def join_(self, queries: list, table_alias: list, join_columns_name: list):
        query1, query2 = queries[0], queries[1]
        t_alias1, t_alias2 = table_alias[0], table_alias[1]
        join = query1 + ' INNER JOIN ' + query2

        # make join condition string
        condition = ' ON ' 
        for column1, column2 in zip(join_columns_name[0], join_columns_name[1]):
            condition += t_alias1+".'"+column1+"'" + " = " + t_alias2+".'"+column2+"'" + " AND "
        
        # remove last whitespace and AND word from condition statement
        condition = condition.rstrip().rsplit(' ', 1)[0]

        join += condition

        return join

    ################################ outermost select statement in join query ################################
    def outer_select_in_join_(self, tables_with_columns_alias: dict, inner_query: str):
        columns_to_be_selected = ''
        for table_alias in tables_with_columns_alias:
            for column_alias in tables_with_columns_alias[table_alias]: 
                columns_to_be_selected += table_alias + ".'"+column_alias+"',"
        
        columns_to_be_selected = columns_to_be_selected[:-1]

        outer_select = 'SELECT ' + columns_to_be_selected + ' FROM (' + inner_query + ')'

        return outer_select 
