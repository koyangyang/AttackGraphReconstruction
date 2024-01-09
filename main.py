from neo4j_sql import dfs_n_level, backword_dfs_n_level
from utils import connect_to_neo4j

if __name__ == '__main__':
    graph = connect_to_neo4j()
    # results = dfs_n_level("Socket", "name", "1.2.3.4", 0)
    results = backword_dfs_n_level("Socket", "name", "1.2.3.5", 4)
    for result in results:
        print(result)
