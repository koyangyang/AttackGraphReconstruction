from global_config import *
from utils import *
import multiprocessing as mp
import pandas as pd


def run():
    process_all_file_miti_processes(trace_source_data, num_processes)


def process_all_file_miti_processes(file_dir_path, num_processes):
    process_pool = mp.Pool(num_processes)
    for file in os.listdir(file_dir_path):
        file_path = file_dir_path + "/" + file
        process_pool.apply_async(process_single_file_single_process, args=(file_path,))
    process_pool.close()
    process_pool.join()


def process_single_file_single_process(file_path):
    print("开始处理文件%s" % (file_path))
    event_list = []
    subject_list = []
    principal_list = []
    netflow_list = []
    file_list = []
    srcsink_list = []
    unnamepipe_list = []
    memory_list = []
    with open(file_path, "r") as file:
        for line in file:
            type = extract_nodetype(line)
            line = load_json(line)["datum"]
            if type == 'com.bbn.tc.schema.avro.cdm18.Event':
                # 说明这行是event
                node = parse_event_data(line)
                # # 添加到三元组
                event_list.append([node['subject'], node['type'], node["predicateObject"], node["timestamp"]])
            else:
                if type == "com.bbn.tc.schema.avro.cdm18.Subject":
                    node = parse_subject_data(line)
                    subject_list.append([node["uuid"], node["name"], node["ppid"], node["cmdLine"], node["type"]])
                elif type == "com.bbn.tc.schema.avro.cdm18.Principal":
                    node = parse_principal_data(line)
                    principal_list.append([node['uuid'], node['userId'], node["type"]])
                elif type == 'com.bbn.tc.schema.avro.cdm18.NetFlowObject':
                    node = parse_netflow_data(line)
                    netflow_list.append([node["uuid"], node["localAddress"], node["localPort"], node["remoteAddress"],
                                         node["remotePort"], node["type"]])
                elif type == 'com.bbn.tc.schema.avro.cdm18.FileObject':
                    node = parse_fileobject_data(line)
                    file_list.append([node["uuid"], node["path"], node["type"]])
                elif type == 'com.bbn.tc.schema.avro.cdm18.SrcSinkObject':
                    node = parse_srcsinkobject_data(line)
                    srcsink_list.append([node["uuid"], node["base_pid"], node["type"]])
                elif type == 'com.bbn.tc.schema.avro.cdm18.UnnamedPipeObject':
                    node = parse_unnamedpipeobject_data(line)
                    unnamepipe_list.append([node["uuid"], node["pid"], node["type"]])
                elif type == 'com.bbn.tc.schema.avro.cdm18.MemoryObject':
                    node = parse_memoryobject_data(line)
                    memory_list.append([node["uuid"], node["memoryAddress"], node["type"]])
    # 保存为csv文件
    event_df = pd.DataFrame(event_list, columns=["subject", "type", "predicateObject", "timestamp"])
    subject_df = pd.DataFrame(subject_list, columns=["uuid", "name", "ppid", "cmdLine", "type"])
    principal_df = pd.DataFrame(principal_list, columns=["uuid", "userId", "type"])
    netflow_df = pd.DataFrame(netflow_list,
                              columns=["uuid", "localAddress", "localPort", "remoteAddress", "remotePort", "type"])
    file_df = pd.DataFrame(file_list, columns=["uuid", "path", "type"])
    srcsink_df = pd.DataFrame(srcsink_list, columns=["uuid", "base_pid", "type"])
    unnamepipe_df = pd.DataFrame(unnamepipe_list, columns=["uuid", "pid", "type"])
    memory_df = pd.DataFrame(memory_list, columns=["uuid", "memoryAddress", "type"])
    # 判断本地csv文件是否存在，存在则追加数据，不存在则创建
    if os.path.exists(csv_path + '/event.csv'):
        event_df.to_csv(csv_path + '/event.csv', mode='a', header=False, index=False)
    else:
        event_df.to_csv(csv_path + '/event.csv', index=False)
    if os.path.exists(csv_path + '/subject.csv'):
        subject_df.to_csv(csv_path + '/subject.csv', mode='a', header=False, index=False)
    else:
        subject_df.to_csv(csv_path + '/subject.csv', index=False)
    if os.path.exists(csv_path + '/principal.csv'):
        principal_df.to_csv(csv_path + '/principal.csv', mode='a', header=False, index=False)
    else:
        principal_df.to_csv(csv_path + '/principal.csv', index=False)
    if os.path.exists(csv_path + '/netflow.csv'):
        netflow_df.to_csv(csv_path + '/netflow.csv', mode='a', header=False, index=False)
    else:
        netflow_df.to_csv(csv_path + '/netflow.csv', index=False)
    if os.path.exists(csv_path + '/file.csv'):
        file_df.to_csv(csv_path + '/file.csv', mode='a', header=False, index=False)
    else:
        file_df.to_csv(csv_path + '/file.csv', index=False)
    if os.path.exists(csv_path + '/srcsink.csv'):
        srcsink_df.to_csv(csv_path + '/srcsink.csv', mode='a', header=False, index=False)
    else:
        srcsink_df.to_csv(csv_path + '/srcsink.csv', index=False)
    if os.path.exists(csv_path + '/unnamepipe.csv'):
        unnamepipe_df.to_csv(csv_path + '/unnamepipe.csv', mode='a', header=False, index=False)
    else:
        unnamepipe_df.to_csv(csv_path + '/unnamepipe.csv', index=False)
    if os.path.exists(csv_path + '/memory.csv'):
        memory_df.to_csv(csv_path + '/memory.csv', mode='a', header=False, index=False)
    else:
        memory_df.to_csv(csv_path + '/memory.csv', index=False)
    print("文件%s处理完成" % (file_path))


if __name__ == '__main__':
    run()
