


class Prerequisite_SqlQueryBuilder:

    def get_table_list_with_alias(self, path: list):
        table_list = {}
        table_list_alias = {}
        count = 1

        for i in range(len(path)):
            for j in range(len(path[i])):
                if not path[i][j] in table_list:
                    table_list[path[i][j]] = path[i][j]
                    table_list_alias[path[i][j]] = "T-" + str(count)  # like T-1, T-2 etc
                    count += 1
        
        return table_list, table_list_alias

    
    def get_columns_outer_query(self, client_input: dict):
        required_columns_outer_query = {}
        dimension_data = client_input["dimensionData"]
        for data in dimension_data:
            if(data["table"] not in required_columns_outer_query):
                required_columns_outer_query[data["table"]] = list(data["columns"].split("$&"))
            else:
                temp = required_columns_outer_query[data["table"]] + list(data["columns"].split("$&"))
                required_columns_outer_query[data["table"]] = temp
        return required_columns_outer_query


    
    def get_columns_inner_query(self, join_paths_across: list, output_columns_outer_query: dict): 
        required_columns_inner_query = {}
        table_join_path = []

        for item in join_paths_across:
            ltl, rtl = item[0], item[1]

            # Doing same step 2 times for left & right tables, for simplicity
            # Need to remove duplicate code later
            ltl = ltl.split('(')
            table1, col_list_table1 = ltl[0], ltl[1].replace(')', '').split(',')   
            col_list_table1 = ' '.join(col_list_table1).split()   # remove any empty string in list

            rtl = rtl.split('(')
            table2, col_list_table2 = rtl[0], rtl[1].replace(')', '').split(',')
            col_list_table2 = ' '.join(col_list_table2).split()   # remove any empty string in list

            # Add join path algo output path in array of tuple format for easy access
            table_join_path.append([(table1, table2), (col_list_table1, col_list_table2)])

            if(table1 not in required_columns_inner_query):
                required_columns_inner_query[table1] = col_list_table1
            else:
                temp = required_columns_inner_query[table1] + col_list_table1
                required_columns_inner_query[table1] = temp

            if(table2 not in required_columns_inner_query):
                required_columns_inner_query[table2] = col_list_table2
            else:
                temp = required_columns_inner_query[table2] + col_list_table2
                required_columns_inner_query[table2] = temp

        #Entend required_columns_inner_query so that used in innermost select query
        for item in output_columns_outer_query:
            temp = required_columns_inner_query[item] + output_columns_outer_query[item]
            required_columns_inner_query[item] = temp

        return required_columns_inner_query, table_join_path
