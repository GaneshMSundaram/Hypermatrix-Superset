
import pandas as pd
import sqlalchemy as sqlalch
import networkx as nx

from superset.databases.join_path_algorithms import build_graph, join_path_algo
# from join_path_algorithms import build_graph, join_path_algo

class JoinPathAlgoWrapper:

    engine = sqlalch.create_engine('postgresql://postgres:postgres123@postgresql.cqjxk9hplpul.us-east-2.rds.amazonaws.com:5432/join_path_algo_poc')
    graph = ''
    path = []
    join_paths_across = []

    # def __init__(self, table_nodes_to_be_linked):
    #     self.table_nodes_to_be_linked = table_nodes_to_be_linked

     
    def build_graph_connection(self, table_nodes_to_be_linked):
        # try:
            sql_data_from_query = pd.read_sql("Select lefttablename as S_Col, righttablename as T_Col, leftcolumnnames as LT_Cols, rightcolumnnames as RT_Cols from join_path_algo.join_path_tb_links", self.engine)
            sql_data_from_query = sql_data_from_query.rename(columns={"s_col": "S.Col", "t_col": "T.Col", "lt_cols" : "LT_Cols", "rt_cols" : "RT_Cols"})
            # Build Graph object 
            self.graph = nx.DiGraph() # Instantiate Graph object
            obj_build_graph = build_graph(sql_data_from_query, self.graph)
            self.graph = obj_build_graph.buildGraph()

            obj_join_path_algo = join_path_algo(self.graph)
            self.path, self.join_paths_across = obj_join_path_algo.contruct_path(nodes_to_be_linked = table_nodes_to_be_linked)

            return self.graph, self.path, self.join_paths_across
        # except Exception as ex:
        #     return ex

        

        