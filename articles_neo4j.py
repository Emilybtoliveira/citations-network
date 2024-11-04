import neo4j
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv  

load_dotenv()

DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
AUTHORS_DIR = "./authors"

def read_csv(path):
    with open(path, "r",  encoding='utf-8-sig') as file:
        csv = file.read()
        file.close()
    return csv

class Neo4j:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.session = self.driver.session(database=DB_NAME)
   
    def close(self):
        self.driver.close()

    def _query(self, query_string):
        result = self.driver.execute_query(query_string)
        #for record in result.records: print(record)
        return result

    def get_node(self, scopus_id):
        query_string = f"MATCH (n:Researcher {{scopus_id: {scopus_id}}}) RETURN n"
        result = self._query(query_string)
        return result.records

    def create_node(self, scopus_id, names, affiliation, aff_type):
        if len(self.get_node(scopus_id)) == 0:
            try:
                query_string = f'CREATE (:Researcher {{scopus_id: {scopus_id}, names: {names}, affiliation_type: "{aff_type}", affiliation: "{affiliation}"}})'
                query_string.replace("'", "\'").replace('"', "\"")
                print(query_string)

                result = self._query(query_string)
                print(result)
            except neo4j.exceptions.CypherSyntaxError:
                print(f"CypherSyntaxError for query {query_string}")
        else:
            print(f"Nó {scopus_id} já existe.")

    def get_arc(self, scopus_id_1, scopus_id_2, doi = None, rel_type=None):
        if doi != None and rel_type != None:
            query_string = f'MATCH (a:Researcher {{scopus_id: {scopus_id_1}}})-[r:{rel_type} {{doi: "{doi}"}}]-(b:Researcher {{scopus_id: {scopus_id_2}}}) RETURN r'
        else:
            query_string = f"MATCH (a:Researcher {{scopus_id: {scopus_id_1}}})-[r]-(b:Researcher {{scopus_id: {scopus_id_2}}}) RETURN r"

        result = self._query(query_string)
        return result.records
        
    def create_collab_arc(self, collab1_scopus_id, collab2_scopus_id, doi, rel_type):
        if len(self.get_arc(collab1_scopus_id, collab2_scopus_id, doi, rel_type)) == 0:
            query_string = f'MATCH (a:Researcher {{scopus_id: {collab1_scopus_id}}}), (b:Researcher {{scopus_id: {collab2_scopus_id}}}) CREATE (a)-[:{rel_type} {{doi: "{doi}"}}]->(b)'
            result = self._query(query_string)
            print(result)
        else:
            print(f"{collab1_scopus_id} e {collab2_scopus_id} já possuem esse relacionamento.")


def generate_graph(db):
    dir_list = os.listdir(AUTHORS_DIR)

    for author_dir in dir_list: #criando os nós
        author_file = os.path.join(AUTHORS_DIR, author_dir, f'{author_dir}.csv')
        author_file_content = read_csv(author_file).split('\n')[1]
        author_file_content = author_file_content.split('&')
        #print(author_file_content)
        
        db.create_node(author_file_content[0], author_file_content[1], author_file_content[2], author_file_content[3])
   
    for author_dir in dir_list: #criando arestas
        author_rel_file = os.path.join(AUTHORS_DIR, author_dir, f'{author_dir}_rel.csv')
        author_rel_file_content = read_csv(author_rel_file).split('\n')

        for i, collab in enumerate(author_rel_file_content):
            if i > 0 and i < (len(author_rel_file_content)-1):
                splitted = collab.split('&')
                
                if len(splitted) == 3 and splitted[0] != "":
                    rel_type = splitted[1]
                    if rel_type == 'was cited':
                        rel_type = 'CITED'
                        db.create_collab_arc(splitted[0], author_dir, splitted[2], rel_type) #ordem inversa
                    elif rel_type == 'cites':
                        rel_type = 'CITED'
                        db.create_collab_arc(author_dir, splitted[0], splitted[2], rel_type)
                    else:
                        rel_type = 'COLLABORATED'
                        db.create_collab_arc(author_dir, splitted[0], splitted[2], rel_type)
                
                

if __name__ == "__main__":
    db = Neo4j(DB_SERVER, DB_USER, DB_PASSWORD)
    
    generate_graph(db)
        
    db.close()