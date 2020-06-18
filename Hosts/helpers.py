from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import getpass

def ConnectES():
    user = None
    passwd = None
    if user is None and passwd is None:
        with open("/etc/ps-dash/creds.key") as f:
            user = f.readline().strip()
            passwd = f.readline().strip()
    credentials = (user, passwd)

    es_url = 'localhost:9200' if getpass.getuser() == 'petya' else 'atlas-kibana.mwt2.org:9200'
    es = Elasticsearch([es_url], timeout=240, http_auth=credentials)

    if es.ping() == True:
        return es
    else: print("Connection Unsuccessful")

es = ConnectES()

def getValueField(idx):
    if idx == 'ps_packetloss':
        return 'packet_loss'
    elif idx == 'ps_owd':
        return 'delay_mean'
    elif idx == 'ps_retransmits':
        return 'retransmits'
    elif idx == 'ps_throughput':
        return 'throughput'

    return None