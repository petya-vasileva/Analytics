import re
import socket
import ipaddress


### Try to resolve IP addresses that were filled for host names.
### If it's a host name - take it. 
###   If it's an IP - try to resolve 
###     If it cannot be resolved - serach in ps_meta for the host name
def ResolveHost(es, host):
    is_host = re.match("^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$", host)
    h = ''
    u = []

    try:
        if is_host:
            h = host
        else:
            # Sometimes host name is not resolved and instead IP address is added. Try to resolve the given IP or host name
            h = socket.gethostbyaddr(host)[0]
    except Exception as inst:
        version = ipaddress.ip_address(host).version 
        if (version == 4):
            v = {'external_address.ipv4_address':host}
        elif (version == 6):
            v = {'external_address.ipv6_address':host}

        # It is possible that the IP got changed but the host is still valid. Check if the ip exists in ps_meta and take the host name. 
        check_host = {
                  "_source": ["host"],
                  "size": 1, 
                    "query": {
                      "match" : v
                    }
                }
        res = es.search("ps_meta", body=check_host)
        if res['hits']['hits']:
            h = res['hits']['hits'][0]['_source']['host']
            print('IP',host, 'was found in ps_meta:', h)
        # if it's a unknown hostname the ip check will fail
        u.append(host)
        u.append(inst.args)

    return {'resolved': h, 'unknown': u}