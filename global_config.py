import multiprocessing as mp
import sys

## 1.Neo4j
neo4j_host = 'neo4j+s://xxx:'
neo4j_port = 'xxx'
neo4j_url = neo4j_host + neo4j_port
neo4j_user = 'neo4j'
neo4j_pass = 'xxx'
neo4j_database = 'neo4j'

## 2.Datasets_Management
# num_processes = mp.cpu_count() - 1
num_processes = 1
source_data_path = "./dataset"
trace_source_data = source_data_path + "/trace"
cadets_source_data = source_data_path + "/cadets"
little_test_data = source_data_path + "/little_test"

# csv文件路径
csv_path = "./result/csv"
