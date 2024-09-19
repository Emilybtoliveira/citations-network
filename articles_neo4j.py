import neo4j
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()
DB_SERVER = os.getenv('DB_SERVER')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

class Neo4j:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def query(self):
        result = self.driver.execute_query(""" MATCH (n) RETURN n """)
        for record in result.records: print(record)


if __name__ == "__main__":
    db = Neo4j(DB_SERVER, DB_USER, DB_PASSWORD)
    try:
        db.query()
    finally:
        db.close()