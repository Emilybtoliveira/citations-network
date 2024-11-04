from graphdatascience import GraphDataScience
from dotenv import load_dotenv  
import neo4j
from neo4j import GraphDatabase
import os
import matplotlib.pyplot as plt
import networkx as nx
import re
import pandas as pd
from pyvis.network import Network
import numpy as np

load_dotenv()

DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

gds = GraphDataScience(DB_SERVER, auth=(DB_USER, DB_PASSWORD), database=DB_NAME)
communities_stats_file = './results/communities-stats.txt'
ic_authors_stats_file = './results/ic_authors_stats.txt'
authors_stats_file = './results/authors_stats.txt'
ic_comms_file = './results/ic_communities.txt'

def write_file(file_path, text):
    with open(file_path, "a") as file:
        file.write(text)

def read_file(file_name):
    with open(file_name, "r") as file:
        content = file.read()
    
    return content

class ArticlesGraph:
    proj_full_collab_graph = None
    proj_partial_undirected_collab_graph = None
    proj_undirected_collab_graph = None
    proj_full_citation_graph = None
    nx_collab_graph = None

    def __init__(self, proj_full_collab_graph, proj_full_citation_graph, proj_partial_undirected_collab_graph, proj_undirected_collab_graph):
        self.proj_full_collab_graph = proj_full_collab_graph
        self.proj_full_citation_graph = proj_full_citation_graph
        self.proj_partial_undirected_collab_graph = proj_partial_undirected_collab_graph
        self.proj_undirected_collab_graph = proj_undirected_collab_graph

        self.__generate_nx_collab_graph__()

    def __generate_nx_collab_graph__(self):
        results = gds.run_cypher(
        """
            MATCH (n:Researcher)-[r:COLLABORATED]->(m:Researcher)
            RETURN n.scopus_id AS source, m.scopus_id AS target
        """
        )
        #print(results)

        self.nx_collab_graph = nx.Graph()  
        
        for record in results.itertuples():
            source = record.source
            target = record.target
            self.nx_collab_graph.add_edge(source, target)

        #print(self.nx_collab_graph.number_of_nodes(), self.nx_collab_graph.number_of_edges())

    def node_betweenness(self):
        per_node_betweenness = gds.run_cypher(
            """
                CALL gds.betweenness.stream('undirected_collab_graph')
                YIELD nodeId, score
                RETURN gds.util.asNode(nodeId).scopus_id AS scopus_id, score
                ORDER BY score DESC
            """
        )
        print(f"Per node betweenness: {per_node_betweenness}")
        # TODO: Visualizar distribuição

        return per_node_betweenness

    def edge_betweenness(self):       
        """ per_edge_betweenness = nx.edge_betweenness_centrality(self.nx_collab_graph)
        #print(per_edge_betweenness)

        with open("./edge-betweenness.txt", "w") as file:
            file.write(str(per_edge_betweenness))
            file.close() """   

        bet_file = read_file("./results/edge-betweenness.txt")

        pattern = r"\((\d+), (\d+)\): ([\d\.e\-]+)"
        matches = re.findall(pattern, bet_file)

        df = pd.DataFrame(matches, columns=["source", "target", "score"])

        df["source"] = df["source"].astype(int)
        df["target"] = df["target"].astype(int)
        df["score"] = df["score"].astype(float)
        df.sort_values(by="score", ascending=False, inplace=True)
        df.reset_index(inplace=True, drop=True)
        print(f"Per edge betweenness: {df}")
        # TODO: Visualizar distribuição

        return df

    def graph_components(self):
        weak_components = gds.wcc.stats(self.proj_full_collab_graph)
        print(f'Count of weak components: {weak_components["componentCount"]}')

        components_per_node = gds.wcc.write(proj_full_collab_graph, writeProperty = 'weak_component')
        print(components_per_node)

        # TODO: Visualizar os componentes

    def nodes_clustering_coef(self):
        per_node_clustering = gds.run_cypher(
        """
            CALL gds.localClusteringCoefficient.stream('undirected_collab_graph')
            YIELD nodeId, localClusteringCoefficient
            RETURN gds.util.asNode(nodeId).scopus_id AS scopus_id, localClusteringCoefficient
            ORDER BY localClusteringCoefficient DESC, scopus_id
        """)
        print(f"Per node clustering coefficient: {per_node_clustering}")
        # TODO: Gerar distribuição

    def global_clustering_coef(self):
        global_clustering = gds.localClusteringCoefficient.stats(self.proj_undirected_collab_graph)["averageClusteringCoefficient"]
        print(f"Global clustering coefficient: {global_clustering}")

    def nodes_distance(self):
        pass
        # all_nodes_distance =  gds.allShortestPaths.stream(self.proj_undirected_collab_graph)
        # print(f"All nodes distance: {all_nodes_distance}")    

    def removing_nodes(self):
        """ 
            Removes the 10 largest betweenness nodes
        """
        per_node_betweenness = self.node_betweenness()

        for i in range(10):
            node_id, bet = per_node_betweenness['scopus_id'][i], per_node_betweenness['score'][i]
            print(f"Removing {node_id} with betweenness {bet}")

            self.nx_collab_graph.remove_node(node_id)

            weak_components = nx.number_connected_components(self.nx_collab_graph)
            print(f'Count of weak components: {weak_components}')

            degree_sequence = [degree for node, degree in self.nx_collab_graph.degree()]
            df_degree = pd.DataFrame(degree_sequence, columns=['degree'])
            print(f'New degree distribution: {df_degree.describe()}')
        
        self.__generate_nx_collab_graph__()
        
    def removing_edges(self):
        """ 
            Removes the 10 largest betweenness edges
        """
        edge_betweenness = self.edge_betweenness()

        for i in range(10):
            source, target, bet = edge_betweenness["source"][i], edge_betweenness["target"][i], edge_betweenness["score"][i]
            print(f"Removing {source, target} with betweenness {bet}")

            self.nx_collab_graph.remove_edge(source, target)

            weak_components = nx.number_connected_components(self.nx_collab_graph)
            print(f'Count of weak components: {weak_components}')
            

            all_shortest_paths = nx.all_pairs_shortest_path_length(self.nx_collab_graph)
            distances = []
            for node, paths in all_shortest_paths:
                for target_node, distance in paths.items():
                    if distance > 0:
                        distances.append(distance)

            distances_distrib = pd.DataFrame(distances, columns=['distance'])
            print(f"Nodes distances distrib: {distances_distrib.describe()}")
        self.__generate_nx_collab_graph__()

    def nodes_degrees(self):
        degree_distribution = proj_full_collab_graph.degree_distribution()
        print(degree_distribution)
        """ fig, ax = plt.subplots()
        ax.hist(degree_distribution)
        plt.show() """

        print(f'Mean degree: {round(degree_distribution["mean"])}')

    def graph_vis(self):
        #TODO: pyvis
        nt = Network('500px', '500px')
        nt.from_nx(self.nx_collab_graph)
        nt.show('nx.html', notebook=False)

    def community_detection(self):
        # graph_communities = gds.louvain.stream(proj_undirected_collab_graph)
        # graph_communities_stats = gds.louvain.stats(proj_undirected_collab_graph)
        graph_communities_write = gds.louvain.write(proj_undirected_collab_graph, writeProperty = 'community')
        # print(graph_communities)
        # print(graph_communities_stats)
        print(graph_communities_write)
    
    def get_communities_stats(self):
        ic_communities = []

        # pegando todos os ids das comunidades criadas
        results = gds.run_cypher(
        """
            MATCH (n:Researcher)
            RETURN n.community AS community
        """ )
        communities = results["community"].unique()
        # print(communities)
            
        for community in communities:            
            # para cada comunidade, criando um dicionario com informações importantes (qtd de citações por tipo e se é grupo do ic)
            community_dict = {
                "id": community,
                "internat_cite": 0,
                "nat_cite": 0
            }

            citations = gds.run_cypher(
               f"""
                   MATCH (n:Researcher)<-[r:CITED]-(m:Researcher) WHERE 
                   (n.community={community} AND 
                   m.community<>{community})
                   RETURN r.doi AS doi, m.affiliation_type AS citing_type
                """)           

            count_cite_type = citations["citing_type"].value_counts()

            if count_cite_type.get("National") != None:
                community_dict["nat_cite"] = count_cite_type.get("National")
            if count_cite_type.get("International") != None:
                community_dict["internat_cite"] = count_cite_type.get("International")

            # verificando se a comunidade tem autores que fazem parte do ic
            results = gds.run_cypher(
            f"""
                MATCH (n:Researcher) WHERE (n.community={community} AND 
                n.affiliation CONTAINS 'Campinas' AND 
                n.affiliation CONTAINS 'Institute of Computing') 
                RETURN COUNT(n) AS count
            """ )
            
            count_ic_members = results["count"][0]
            if count_ic_members > 1: #threshold para considerar um grupo como sendo do IC
               ic_communities.append(communities)
               community_dict["ic_members"] = count_ic_members            
            
            # print(community_dict)

            write_file(communities_stats_file, f'community id: { community_dict["id"]}\n')
            write_file(communities_stats_file, f'international citations: {community_dict["internat_cite"]}\n')
            write_file(communities_stats_file, f'national citations: {community_dict["nat_cite"]}')
            if community_dict.get("ic_members") != None: write_file(communities_stats_file, f'\nIC members count: {community_dict["ic_members"]}')
            write_file(communities_stats_file, ";\n\n")

        print(f"Total number of communities: {len(communities)}")            
        print(f"IC communities count: {len(ic_communities)}")
        
        for i in range(len(ic_communities)): 
            print(ic_communities[i], end=" ")
            write_file(ic_comms_file, (ic_communities[i]+ " "))

    def generate_communities_rankings(self, full=False, international=True):
        ranking = []

        communities_stats = read_file(communities_stats_file)
        ic_communities = read_file(ic_comms_file).split("\n")

        for line in communities_stats.split(";"):
            line_split = line.strip().split('\n')
            
            if line_split[0] != '':
                community_id = line_split[0].split(": ")[1]
                inter_count = line_split[1].split(": ")[1]
                nat_count = line_split[2].split(": ")[1]
            
                ranking.append((community_id, int(inter_count), int(nat_count)))
    

        if international:
            ranking.sort(key=lambda x: x[1], reverse=True)
            print("Ranking Internacional:")
            for i in range(len(ranking)):
                if ranking[i][0] in ic_communities or full:
                    print(f"{i+1}º -- Community ID: {ranking[i][0]}, International Citations: {ranking[i][1]}, National Citations: {ranking[i][2]}")
        else:   
            ranking.sort(key=lambda x: x[2], reverse=True)
            print("\nRanking Nacional:")
            for i in range(len(ranking)):   
                if ranking[i][0] in ic_communities or full:       
                    print(f"{i+1}º -- Community ID: {ranking[i][0]}, International Citations: {ranking[i][1]}, National Citations: {ranking[i][2]}")
    
    def get_ic_authors_stats(self):
        file_line = read_file(ic_comms_file)
        ic_comms = file_line.split("\n")

        file = open(ic_authors_stats_file, "w") #limpando o arquivo
        file.close()

        for community_id in ic_comms:
            results = gds.run_cypher(
            f"""
                MATCH (n:Researcher) 
                WHERE n.community={int(community_id)} 
                RETURN n.scopus_id, n.affiliation
            """ )

            # print(results)

            write_file(ic_authors_stats_file, f"community id: {community_id}\n")

            for author in results.itertuples():
                author_scopus = author[1]
                author_affiliation = author[2]
                
                cite_count = gds.run_cypher(f""" MATCH (n:Researcher)<-[r:CITED]-(m) WHERE n.scopus_id={author_scopus} return COUNT(r) AS count""" )
                cite_count = cite_count["count"][0]

                if 'Institute of Computing' in author_affiliation and 'Campinas' in author_affiliation:
                    write_file(ic_authors_stats_file, f"(IC)author:{author_scopus} citations:{cite_count}\n")
                else:
                    write_file(ic_authors_stats_file, f"author:{author_scopus} citations:{cite_count}\n")
            
            write_file(ic_authors_stats_file, ";\n")

    def generate_authors_rankings(self, full=False):
        authors_stats = read_file(ic_authors_stats_file)
        ic_authors = []
        
        for line in authors_stats.split(";"): #calcula o ranking por grupo
            ic_authors = []
            ranking = []

            if(line != ''):
                line_split = line.strip().split('\n')
                # print(line_split)

                for i, author_line in enumerate(line_split):
                    if i == 0:
                        community_id = author_line.split(": ")[1]
                    if i != 0:
                        author_info = author_line.split(" ")
                        author_id = author_info[0].split(":")[1]
                        author_citations = author_info[1].split(":")[1]

                        ranking.append((author_id, int(author_citations)))    

                        if 'IC' in author_line:
                            ic_authors.append(author_id)
                
                ranking.sort(key=lambda x: x[1], reverse=True)
                print(f"\nRanking dos autores do grupo {community_id}:")
                for i in range(len(ranking)):
                    if ranking[i][0] in ic_authors or full:
                        print(f"{i+1}º -- Author: {ranking[i][0]}, Citations: {ranking[i][1]}")

try:    
    proj_full_collab_graph, result = gds.graph.project(
        "full_collab_graph",
        ["Researcher"],                   #  Node projection
        ["COLLABORATED"],                  #  Relationship projection
        nodeProperties=["scopus_id"]
    ) 
    # print(result)
    #print(proj_full_collab_graph.memory_usage())

    proj_full_citation_graph, result = gds.graph.project(
        "full_citation_graph",
        ["Researcher"],                   #  Node projection
        ["CITED"]                  #  Relationship projection
    ) 
    # print(result)
    #print(proj_full_citation_graph.memory_usage())
    #print(proj_full_citation_graph.relationship_count())

    proj_partial_undirected_collab_graph, result = gds.graph.project(
        "partial_undirected_collab_graph",
        ["Researcher"],                   #  Node projection
        ["COLLABORATED"],                  #  Relationship projection
        nodeProperties=["scopus_id"]
    ) 
    result = gds.graph.relationships.toUndirected(proj_partial_undirected_collab_graph, "COLLABORATED", "COLLABS")
    # print(result)     
    
    result = gds.graph.filter('undirected_collab_graph', proj_partial_undirected_collab_graph, 'n:Researcher', 'r:COLLABS')
    # print(result) 
    proj_undirected_collab_graph = gds.graph.get('undirected_collab_graph')
    # print(proj_undirected_collab_graph.node_properties())    

    articles_graph = ArticlesGraph(proj_full_collab_graph, proj_full_citation_graph, proj_partial_undirected_collab_graph, proj_undirected_collab_graph)

    # # Calcular o numero de componentes do grafo.
    # articles_graph.graph_components()

    # # Calcular o coeficiente de clusterizacao de cada no e plotar a sua distribuicao.
    # articles_graph.nodes_clustering_coef()

    # # Calcular o coeficiente de clusterizacao global do grafo.
    # articles_graph.global_clustering_coef()

    # # Calcular a distancia media e plotar a distribuicao das distancias entre os nos.
    # articles_graph.nodes_distance()

    # # Calcular o betweenness dos nos e das arestas desse grafo e plotar a distribuicao.
    # # Node Betweenness
    # articles_graph.node_betweenness()
    # # Edge Betweenness
    # articles_graph.edge_betweenness()
    
    # # Mostrar o que ocorre a medida que voce retira nos/arestas com maior betweeness.
    # # Retirando os nós
    # articles_graph.removing_nodes()
    
    # #Retirando as arestas
    # articles_graph.removing_edges()    

    # # Calcular e plotar a distribuição dos grau dos nós e grau médio.
    # articles_graph.nodes_degrees()
    
    # Fazer uma visualização do grafo
    #articles_graph.graph_vis()

    # Detecção de grupos
    # articles_graph.community_detection()
    # articles_graph.get_communities_stats()
    articles_graph.generate_communities_rankings(False, True)
    # articles_graph.get_ic_authors_stats()
    # articles_graph.generate_authors_rankings()

finally:
    proj_full_collab_graph = gds.graph.get("full_collab_graph")
    proj_full_collab_graph.drop()

    proj_full_citation_graph = gds.graph.get("full_citation_graph")
    proj_full_citation_graph.drop()

    proj_partial_undirected_collab_graph = gds.graph.get("partial_undirected_collab_graph")
    proj_partial_undirected_collab_graph.drop()

    proj_undirected_collab_graph = gds.graph.get("undirected_collab_graph")
    proj_undirected_collab_graph.drop()

    gds.close() 