from utils import connect_to_neo4j
from py2neo import Node, Relationship, NodeMatcher

if __name__ == '__main__':
    graph = connect_to_neo4j()
    graph.delete_all()
