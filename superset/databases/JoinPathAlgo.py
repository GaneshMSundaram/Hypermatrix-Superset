# -*- coding: utf-8 -*-
"""
Created on Mon Feb 21 13:02:11 2022

@author: User
"""

# Libraries
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt

# ********* Class to read Target table link data ********************************
class read_target_table_links:
    def __init__(self):
        pass
    # **************************************************
    def read_target_tables(self, collegeID, GridID):
        df_table_links = pd.read_csv('TableLinks_2.csv') 
        df_table_links = df_table_links[(df_table_links['CollegeID'] == collegeID) & (df_table_links['GridID'])]
        return df_table_links

# ===============================================================================
    
# ********* Class - Build Graph - Add Properties to node / links ****************
class build_graph:
    def __init__(self, df_table_links, myGraph):
        self.df_table_links = df_table_links
        self.myGraph = myGraph
    
    # **************************************************
    def get_edge_relationship(self, table_link_details, order='front'):
        try:
            link_details = []
            if order.lower() == 'front':
                sTbl, tTbl = table_link_details['S.Col'], table_link_details['T.Col']
                link_details = [tuple(table_link_details['LT_Cols']), tuple((table_link_details['RT_Cols']))]
            else:
                sTbl, tTbl = table_link_details['T.Col'], table_link_details['S.Col']
                link_details = [tuple(table_link_details['RT_Cols']), tuple((table_link_details['LT_Cols']))]
            link_details = {"link": link_details}
  
            return sTbl, tTbl, link_details
        except Exception as e:
            print('Err in gte_edge_relationship - ' + str(e))
            raise e
    
    # **************************************************
    def addEdgeWithProperties(self, G, node1, node2, edge_properties):
        try:
            G.add_edge(node1, node2)
            nx.set_edge_attributes(G, {(node1, node2): edge_properties})
        except Exception as e:
            print('Err in addEdgeWithProperties - ' + str(e))
            raise e
            
    # **************************************************
    def addBiGraph(self, graph, df_table_link_details):
        try:
            # Front link
            sTbl, tTbl, table_link_details = self.get_edge_relationship(df_table_link_details, order='front')
            self.addEdgeWithProperties(self.myGraph, sTbl, tTbl, table_link_details)
            
            # reverse link
            sTbl, tTbl, table_link_details = self.get_edge_relationship(df_table_link_details, order='reverse')
            self.addEdgeWithProperties(self.myGraph, sTbl, tTbl, table_link_details)
        except Exception as e:
            print('Err in addBiGraph - ' + str(e))
            raise e
    
    # **************************************************
    def buildGraph(self):
        # Build graph
        try:
            # loop thru data frame
            for i in range(len(self.df_table_links)):
                self.addBiGraph(self.myGraph, dict(self.df_table_links.loc[i].copy()))
                
            return self.myGraph
        except Exception as e:
            print('Err in buildGraph - ' + str(e))
            raise e
    
# ==================================================================================================

# ********* Class - Find shortest path to link given tables ****************
class join_path_algo:
    def __init__(self, myGraph):        
        self.myGraph = myGraph
        
    def find_paths(self, allPaths):
        try:
            res = list(zip(allPaths, allPaths[1:]))
            res = [sorted(x) for x in res]
            deduplicated_list = []
            [deduplicated_list.append(item) for item in res if item not in deduplicated_list]
            
            return deduplicated_list
        except Exception as e:
                print('Err in find_path - ' + str(e))
                raise e
    # **************************************************
    def find_shortest_node_links(self, nodes_to_be_linked):
        try:
            path = nx.approximation.traveling_salesman_problem(self.myGraph, nodes=nodes_to_be_linked, cycle=False)
            all_paths = self.find_paths(path)
            
            return all_paths    
        except Exception as e:
            print('Err in find_shortest_path - ' + str(e))
            raise e
    # **************************************************
    def construct_where_clause(self, sTable, tTable):    
        try:
            link = self.myGraph[sTable][tTable]
            join_cond = []
            
            join_cond.append(sTable + str(link['link'][0]).replace("'", ""))
            join_cond.append(tTable + str(link['link'][1]).replace("'", ""))
            
            return join_cond
        except Exception as e:
            print('Err in construct_where_clause - ' + str(e))
            raise e
    # **************************************************
    def contruct_path(self, nodes_to_be_linked) :
        try:
            path = self.find_shortest_node_links(nodes_to_be_linked)
            join_paths_across = []
            
            for table_join in path:
                # print(table_join)
                sTable, tTable = table_join[0], table_join[1]
                join_path = self.construct_where_clause(sTable, tTable)
                join_paths_across.append(join_path)
                
            return path, join_paths_across
        except Exception as e:
            print('Err in construct_path - ' + str(e))
            raise e
# ==============================================================================================

# ************ COde to run the Build graph & find shortest path ***************

# Read Target table links
# obj_read_target_table_links = read_target_table_links()
# df_table_links = obj_read_target_table_links.read_target_tables('NEWACE', 'Grid-2')

# myGraph = nx.DiGraph() # Instantiate Graph object

# # # Build Target table links as Graph
# obj_build_graph = build_graph(df_table_links, myGraph)
# myGraph = obj_build_graph.buildGraph()
# nx.draw(myGraph, with_labels=True)
# plt.show()


# # Find shortest path for connecting user given tables
# obj_join_path_algo = join_path_algo(myGraph)
# path, join_paths_across = obj_join_path_algo.contruct_path(nodes_to_be_linked=['enrollment_fact', 'enrollment_term_dim_2'])
# print(path, join_paths_across)
