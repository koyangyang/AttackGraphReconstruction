from utils import connect_to_neo4j
from py2neo import Node, Relationship

"""
使用 Cypher 查询语言进行深度遍历
@label: 节点标签
@key: 节点属性
@value: 节点属性值
@n: 深度
"""


def dfs_n_level(label, key, value, n):
    graph = connect_to_neo4j()
    cypher_query = f"MATCH (c:{label}{{{key}: '{value}'}})-[r*{n}..]->(result) return result"
    return graph.run(cypher_query)


"""
使用 Cypher 查询语言进行反向深度遍历
@label: 节点标签
@key: 节点属性
@value: 节点属性值
@n: 深度
"""


def backword_dfs_n_level(label, key, value, n):
    graph = connect_to_neo4j()
    cypher_query = f"MATCH (c:{label}{{{key}: '{value}'}})<-[r*{n}..]-(result) return result"
    return graph.run(cypher_query)


"""
创建两个节点和节点之间的关系
@label1: 节点1标签
@name1: 节点1名称
@label2: 节点2标签
@name2: 节点2名称
@relationship: 节点1和节点2之间的关系
"""


def create_node_relationship(label1, name1, label2, name2, relationship):
    graph = connect_to_neo4j()
    # 创建节点
    a = Node(label1, name=name1)
    b = Node(label2, name=name2)
    # 创建节点之间的关系
    ab = Relationship(a, relationship, b)
    # 将节点和关系加入图数据库
    s = ab
    graph.create(s)
