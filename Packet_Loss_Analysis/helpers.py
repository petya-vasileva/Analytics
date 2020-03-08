import re
import socket
import ipaddress
import csv
import multiprocessing as mp
from functools import partial
from contextlib import contextmanager
from tqdm import tqdm
import time
import os
import datetime
import pandas as pd

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


def GetHostsMetaData():
    # The query is aggregating results by month, host, ipv4 and ipv6. The order is ascending.
    # That way we ensure the last added host name in the ip_data dictionary is the most recent one. We add it at position 0.
    query = {
      "size" : 0,
      "_source" : False,
      "stored_fields" : "_none_",
      "aggregations" : {
        "perf_meta" : {
          "composite" : {
            "size" : 9999,
            "sources": [
              {
                "ts" : {
                  "terms" : {
                    "script" : {
                      "source" : "InternalSqlScriptUtils.dateTrunc(params.v0,InternalSqlScriptUtils.docValue(doc,params.v1),params.v2)",
                      "lang" : "painless",
                      "params" : {
                        "v0" : "month",
                        "v1" : "timestamp",
                        "v2" : "Z"
                      }
                    },
                    "missing_bucket" : True,
                    "value_type" : "long",
                    "order" : "asc"
                  }
                }
              },
              {
                "host" : {
                  "terms" : {
                    "field" : "host.keyword",
                    "missing_bucket" : True
                    }
                }
              },
              {
                "ipv4" : {
                  "terms" : {
                    "field" : "external_address.ipv4_address",
                    "missing_bucket" : True
                    }
                }
              },
              {
                "ipv6" : {
                  "terms" : {
                    "field" : "external_address.ipv6_address",
                    "missing_bucket" : True
                    }
                }
              }
            ]
          }
        }
      }
    }

    data = es.search("ps_meta", body=query)

    ip_data = {}
    def Add2Dict(ip, host):
        temp = []
        if (ip in ip_data):
            if (host not in ip_data[ip]) and (host != ''):
                temp.append(host)
                temp.extend(ip_data[ip])
                ip_data[ip] = temp
        else:
            ip_data[ip] = [host]


    host_list = []
    for item in data["aggregations"]["perf_meta"]["buckets"]:

        ipv4 = item['key']['ipv4']
        ipv6 = item['key']['ipv6']
        host = item['key']['host']

        if ipv4 is not None:
            Add2Dict(ipv4, host)

        if ipv6 is not None:
            Add2Dict(ipv6, host)

        if host not in host_list:
            host_list.append(host)

    return {'hosts': host_list, 'ips': ip_data}


manager = mp.Manager()
unresolved = manager.dict()
hosts = manager.dict()

es = ConnectES()
meta = GetHostsMetaData()
hosts_meta = meta['hosts']
ips_meta = meta['ips']


#### Fix hosts by:
####   replacing IP addresses with correct host names:
####   removing documents that are not part of the configuration
def FixHosts(item, unres):
    # If host is empty try with the IP
    src_item = item['src_host'] if item['src_host'] != '' else item['src']
    dest_item = item['dest_host'] if item['dest_host'] != '' else item['dest']
    isOK = False

    # If host was checked and resolved already just take it from the shared dictionary
    if src_item in hosts:
        if hosts[src_item] != 'unresolved':
            isOK = True
            item['src_host'] = hosts[src_item]
        else: isOK = False
    # If host is not in hosts already, try to resolve it then add the result to the shared dictionary
    else:
        src = ResolveHost(src_item)
        if (src['resolved']):
            isOK = True
            item['src_host'] = src['resolved']
            hosts[src_item] = src['resolved']
        elif (src['unknown']):
            isOK = False
            unres[src['unknown'][0]] = src['unknown'][1]
            hosts[src_item] = 'unresolved'

    if dest_item in hosts:
        if hosts[dest_item] != 'unresolved':
            isOK = True
            item['dest_host'] = hosts[dest_item]
        else: isOK = False
    else:
        dest = ResolveHost(dest_item)
        if (dest['resolved']):
            isOK = True
            item['dest_host'] = dest['resolved']
            hosts[dest_item] = dest['resolved']
        elif ((dest['unknown'])):
            isOK = False
            unres[dest['unknown'][0]] = dest['unknown'][1]
            hosts[dest_item] = 'unresolved'

    if (isOK == True):
        return item

    
### Process the dataset in parallel
def ProcessHosts(data): 
    @contextmanager
    def poolcontext(*args, **kwargs):
        pool = mp.Pool(*args, **kwargs)
        yield pool
        pool.terminate()

    start = time.time()
    print('Start:', time.strftime("%H:%M:%S", time.localtime()))
    with poolcontext(processes=mp.cpu_count()) as pool:
        i = 0
        results = []
        for doc in pool.imap_unordered(partial(FixHosts, unres=unresolved), data):
            if (i > 0) and (i % 100000 == 0):
                print('Next',i,'items')
            if doc is not None:
                results.append(doc)
            i = i + 1
    print("Time elapsed = %ss" % (int(time.time() - start)))
    print('Number of unique hosts:', len(hosts))

    try:
        not_found = pd.read_csv('not_found.csv')
    except (pd.io.common.EmptyDataError, FileNotFoundError) as error:
        not_found = pd.DataFrame(columns=['host', 'message'])

    for key, val in dict(unresolved).items():
        if not (not_found['host'].str.contains(key).any()):
            not_found.loc[len(not_found)+1] = [key, val]

    not_found.sort_values(['host', 'message'], ascending=[True, False], inplace=True)
    not_found.to_csv('not_found.csv', index=False)

    return results

### Try to resolve IP addresses that were filled for host names and thus exclude data not relevant to the project
### If it's a host name, verify it's part of the configuration, i.e. search in ps_meta
###   If it's an IP - try to resolve 
###     If it cannot be resolved - serach in ps_meta for the host name
def ResolveHost(host):
    is_host = re.match("^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$", host)
    h = ''
    u = []

    try:
        # There are data that is collected in ElasticSearch, but it's not relevant to the SAND project.
        # It is neccessary to check if it is part of the configuration by searching in ps_meta index.
        if is_host:
            if host in hosts_meta:
                h = host
            else:
                u.append(host)
                u.append("Host not found in ps_meta index")
        else:
            # Sometimes host name is not resolved and instead IP address is added. Try to resolve the given IP
            h = socket.gethostbyaddr(host)[0]
    except Exception as inst:
        # It is possible that the IP got changed but the host is still valid. Check if the ip exists in ps_meta and take the host name. 
        if host in ips_meta:
            h = ips_meta[host][0]
        else:
            u.append(host)
            u.append(inst.args)

    return {'resolved': h, 'unknown': u}