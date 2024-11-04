# Neo4j testes

## Acessando o banco em Python
https://github.com/neo4j-examples/movies-python-bolt/blob/main/movies_sync.py

```python
import neo4j
from neo4j import GraphDatabase

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
```

## Criando nós individualmente
```cypher
CREATE (:Researcher {name: 'FULANO, C.', filiation: 'UNICAMP'})
CREATE (:Researcher {name: 'OLIVEIRA, E.B', filiation: 'UNICAMP'})
```

## Criando relacionamentos individualmente

```cypher
MATCH (emily:Researcher {name: 'OLIVEIRA, E.B'}), (cicrano:Researcher {name: 'FULANO, C.'}) 
CREATE (emily)-[:CITED {year: '2018'}]->(cicrano)
```

## Criando o grafo em batch

## Selects
**Grafo inteiro**
```cypher
MATCH (n) RETURN n
```

**Nó**

```cypher
MATCH (n:Researcher {name: 'OLIVEIRA, E.B'})
RETURN n
```

**Label**

```cypher
MATCH (res:Researcher)
RETURN res
```

**Relacionamento qualquer**
```
#Não direcionado
MATCH (a:Researcher {scopus_id: 57212487257})-[r]-(b:Researcher {scopus_id: 57203172256})
RETURN r

#Direcionado
MATCH (a:Researcher {scopus_id: 57212487257})-[r]->(b:Researcher {scopus_id: 57203172256})
RETURN r 
MATCH (a:Researcher {scopus_id: 57212487257})<-[r]-b:Researcher {scopus_id: 57203172256})
RETURN r
```

**Relacionamento com propriedade**

```cypher
#Não direcionado
MATCH (a:Researcher {scopus_id: 57212487257})-[r:CITED {doi: "DSF"}]-(b:Researcher {scopus_id: 57203172256})
RETURN r
```

## Deletes
**Grafo inteiro**
```cypher
MATCH (n)
DETACH DELETE n
```

**Nó e arestas**
```cypher
MATCH (n:Researcher {name: 'OLIVEIRA, E.B'})
DETACH DELETE n
```