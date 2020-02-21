import re
import socket
import ipaddress
import json
import multiprocessing as mp
from functools import partial
from contextlib import contextmanager
from tqdm import tqdm
import time
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

def ConnectES():
    user = None
    passwd = None
    if user is None and passwd is None:
        with open("creds.key") as f:
            user = f.readline().strip()
            passwd = f.readline().strip()
    credentials = (user, passwd)
    es = Elasticsearch(['atlas-kibana.mwt2.org:9200'], timeout=240, http_auth=credentials)

    if es.ping() == True:
        return es
    else: print("Connection Unsuccessful")


manager = mp.Manager()
unresolved = manager.dict()
hosts = manager.dict()
empty = manager.list()

#### Fix hosts so that
def FixHosts(item, unres):
    src_item = item['src_host']
    dest_item = item['dest_host']
    isOK = False
    if (src_item != '' and dest_item != ''):
        if src_item in hosts:
            if hosts[src_item] != 'unresolved':
                isOK = True
                item['src_host'] = hosts[src_item]
            else: isOK = False
        else:
#         if not src_item in hosts:
            src = ResolveHost(item['src_host'])
            if ((len(src['resolved']) != 0)):
                isOK = True
                item['src_host'] = src['resolved']
                hosts[src_item] = src['resolved']
            elif ((src['unknown'])):
                isOK = False
                unres[src['unknown'][0]] = src['unknown'][1]
                hosts[src_item] = 'unresolved'
        if not dest_item in hosts:
            dest = ResolveHost(item['dest_host'])
            if ((len(dest['resolved']) != 0)):
                isOK = True
                item['dest_host'] = dest['resolved']
                hosts[dest_item] = dest['resolved']
            elif ((dest['unknown'])):
                isOK = False
                unres[dest['unknown'][0]] = dest['unknown'][1]
                hosts[dest_item] = 'unresolved'
#         else: 
#             if hosts[src_item] != 'unresolved':
#                 isOK = True
#                 item['src_host'] = hosts[src_item]
#             else: isOK = False
    else:
        isOK = False
        empty.append('src_host: '+src_item+' dest_host: '+dest_item+' at '+item['timestamp'])
    if (isOK == True):
        return item

    
### Process the dataset in parallel
def ProcessHosts(data): 
    
    @contextmanager
    def poolcontext(*args, **kwargs):
        pool = mp.Pool(*args, **kwargs)
        yield pool
        pool.terminate()

    with poolcontext(processes=mp.cpu_count()) as pool:  
        start = time.time()
        print('Start:', time.strftime("%H:%M:%S", time.localtime()))
        i = 0
        results = []
        for doc in pool.imap_unordered(partial(FixHosts, unres=unresolved), data):
            if i % 100000 == 0:
                print("next 100K",i)
            if doc is not None:
                results.append(doc)
            i = i + 1
        print("Time elapsed = %s" % (int(time.time() - start)))
        print('Number of unique hosts:', len(hosts))
        
    file1 = open('output.txt', 'w')
    file1.write('******************************** UNRESOLVED HOSTS ********************************')
#     for k,v in dict(unresolved).items():
#         file1.write('\n')
    file1.write(json.dumps(dict(unresolved)))
    
    if len(empty)>0:
        file1.write('\n')
        file1.write('******************************** EMPTY VALUES ********************************')
        for h in empty:
            file1.write(h)     
    file1.close()
    
    return results


### Try to resolve IP addresses that were filled for host names and thus exclude data not relevant to the project
### If it's a host name, verify it's part of the configuration, i.e. search in ps_meta
###   If it's an IP - try to resolve 
###     If it cannot be resolved - serach in ps_meta for the host name
def ResolveHost(host):
    es = ConnectES()
    is_host = re.match("^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$", host)
    h = ''
    u = []

    try:
        # There are data that is collected in ElasticSearch, but it's not relevant to the SAND project.
        # It is neccessary to check if it is part of the configuration by searching in ps_meta index.
        if is_host:
            is_valid = {
                          "size": 1,
                          "_source": ["host"],
                          "query": {
                            "bool":{
                              "must":[
                                {
                                  "match_phrase": {
                                    "host":host
                                        }
                                }
                              ]
                            }
                          }
                        }
#             print(is_valid)
            res = es.search("ps_meta", body=is_valid)
            if res['hits']['hits']:
                h = host
            else:
                u.append(host)
                u.append("Host not found in ps_meta index")
        else:
            # Sometimes host name is not resolved and instead IP address is added. Try to resolve the given IP
            h = socket.gethostbyaddr(host)[0]
    except Exception as inst:
        version = ipaddress.ip_address(host).version 
        if (version == 4):
            v = {'external_address.ipv4_address':host}
        elif (version == 6):
            v = {'external_address.ipv6_address':host}

        # It is possible that the IP got changed but the host is still valid. Check if the ip exists in ps_meta and take the host name. 
        check_hostname = {
                  "_source": ["host"],
                  "size": 1, 
                    "query": {
                      "match" : v
                    }
                }
        res = es.search("ps_meta", body=check_hostname)
        if res['hits']['hits']:
            h = res['hits']['hits'][0]['_source']['host']
#             print('IP',host, 'was found in ps_meta:', h)
        # if it's a unknown hostname the ip check will fail
        u.append(host)
        u.append(inst.args)
    
    return {'resolved': h, 'unknown': u}