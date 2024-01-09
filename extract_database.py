from tqdm import tqdm
import re
import hashlib

filelist = ['ta1-cadets-e3-official.json',
            'ta1-cadets-e3-official.json.1',
            'ta1-cadets-e3-official.json.2',
            'ta1-cadets-e3-official-1.json',
            'ta1-cadets-e3-official-1.json.1',
            'ta1-cadets-e3-official-1.json.2',
            'ta1-cadets-e3-official-1.json.3',
            'ta1-cadets-e3-official-1.json.4',
            'ta1-cadets-e3-official-2.json',
            'ta1-cadets-e3-official-2.json.1']


def stringtomd5(originstr):
    originstr = originstr.encode("utf-8")
    signaturemd5 = hashlib.sha256()
    signaturemd5.update(originstr)
    return signaturemd5.hexdigest()


def store_netflow(file_path, cur, connect):
    # Parse data from logs
    netobjset = set()
    netobj2hash = {}
    for file in tqdm(filelist):
        with open(file_path + file, "r") as f:
            for line in f:
                if "NetFlowObject" in line:
                    try:
                        res = re.findall(
                            'NetFlowObject":{"uuid":"(.*?)"(.*?)"localAddress":"(.*?)","localPort":(.*?),"remoteAddress":"(.*?)","remotePort":(.*?),',
                            line)[0]

                        nodeid = res[0]
                        srcaddr = res[2]
                        srcport = res[3]
                        dstaddr = res[4]
                        dstport = res[5]

                        nodeproperty = srcaddr + "," + srcport + "," + dstaddr + "," + dstport
                        hashstr = stringtomd5(nodeproperty)
                        netobj2hash[nodeid] = [hashstr, nodeproperty]
                        netobj2hash[hashstr] = nodeid
                        netobjset.add(hashstr)
                    except:
                        pass

    # # Store data into database
    # datalist = []
    # for i in netobj2hash.keys():
    #     if len(i) != 64:
    #         datalist.append([i] + [netobj2hash[i][0]] + netobj2hash[i][1].split(","))
    # sql = '''insert into netflow_node_table
    #                      values %s
    #         '''
    #
    # ex.execute_values(cur, sql, datalist, page_size=10000)
    # connect.commit()
