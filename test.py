from utils import connect_to_neo4j
from py2neo import Node, Relationship, NodeMatcher

if __name__ == '__main__':
    graph = connect_to_neo4j()
    node_matcher = NodeMatcher(graph)
    v = node_matcher.match("File", name="test.txt").first()
    u = Node('Socket', name='1.2.3.7')
    a = Node('Process', name='Firefox.exe')
    ua = Relationship(u, 'OPEN', a)
    av = Relationship(a, 'READ', v)
    s = ua | av
    graph.create(s)
