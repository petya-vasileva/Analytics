from datetime import datetime, timedelta
import pandas as pd
import itertools

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import helpers as hp
import time


def AggBySrcDestIP(idx, time_from, time_to):
    val_fld = hp.getValueField(idx)
    query = {
              "size" : 0,
              "_source" : False,
              "query" : {
                "range" : {
                  "timestamp" : {
                    "from" : time_from,
                    "to" : time_to
                  }
                }
              },
              "aggregations" : {
                "groupby" : {
                  "composite" : {
                    "size" : 10000,
                    "sources" : [
                      {
                        "src_host" : {
                          "terms" : {
                            "field" : "src_host",
                            "missing_bucket" : True,
                            "order" : "asc"
                          }
                        }
                      },
                      {
                        "ipv6" : {
                          "terms" : {
                            "field" : "ipv6",
                            "missing_bucket" : True,
                            "order" : "asc"
                          }
                        }
                      },
                      {
                        "dest_host" : {
                          "terms" : {
                            "field" : "dest_host",
                            "missing_bucket" : True,
                            "order" : "asc"
                          }
                        }
                      }
                    ]
                  },
                  "aggregations" : {
                    "mean_field" : {
                      "avg" : {
                        "field" : val_fld
                      }
                    }
                  }
                }
              }
            }

    print("Run query AggBySrcDestIP for {} ".format(idx))
    start = time.time()
    results = hp.es.search( index=idx, body=query)
    print("Query took: %ss" % (int(time.time() - start)))

    data = []
    data1 = []
    for item in results["aggregations"]["groupby"]["buckets"]:
        data1.append(item)
        data.append({'dest_host':item['key']['dest_host'], 'src_host':item['key']['src_host'], 'ipv6':item['key']['ipv6'],
                     val_fld: item['mean_field']['value'], 'num_tests': item['doc_count']})

    return data

def get_ip_host(idx, dateFrom, dateTo):
    def q_ip_host (fld):
        return {
                  "size" : 0,
                  "query" : {  
                    "bool" : {
                      "must" : [
                        {
                          "range" : {
                            "timestamp" : {
                              "from" : dateFrom,
                              "to" : dateTo
                            }
                          }
                        }
                      ]
                    }
                  },
                  "_source" : False,
                  "stored_fields" : "_none_",
                  "aggregations" : {
                    "groupby" : {
                      "composite" : {
                        "size" : 9999,
                        "sources" : [
                          {
                            fld : {
                              "terms" : {
                                "field" : fld,
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          },
                          {
                            str(fld+"_host") : {
                              "terms" : {
                                "field" : str(fld+"_host"),
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          }
                        ]
                      }
                    }
                  }
                }


    for field in ['src', 'dest']:
        results = hp.es.search(index=idx, body=q_ip_host(field))
        res_ip_host = {}
        for item in results["aggregations"]["groupby"]["buckets"]:
            ip = item['key'][field]
            host = item['key'][str(field+'_host')]
            if ((ip in res_ip_host.keys()) and (host is not None) and (host != ip)) or (ip not in res_ip_host.keys()):
                res_ip_host[ip] = host
    return res_ip_host


def get_ip_site(idx, dateFrom, dateTo):
    def q_ip_site (fld):
        return {
                  "size" : 0,
                  "query" : {  
                    "bool" : {
                      "must" : [
                        {
                          "range" : {
                            "timestamp" : {
                              "from" : dateFrom,
                              "to" : dateTo
                            }
                          }
                        }
                      ]
                    }
                  },
                  "_source" : False,
                  "stored_fields" : "_none_",
                  "aggregations" : {
                    "groupby" : {
                      "composite" : {
                        "size" : 9999,
                        "sources" : [
                          {
                            fld : {
                              "terms" : {
                                "field" : fld,
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          },
                          {
                            str(fld+"_site") : {
                              "terms" : {
                                "field" : str(fld+"_site"),
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          },
                          {
                            "ipv6" : {
                              "terms" : {
                                "field" : "ipv6",
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          }
                        ]
                      }
                    }
                  }
                }


    for field in ['src', 'dest']:
        results = hp.es.search(index=idx, body=q_ip_site(field))
        res_ip_site = {}
        for item in results["aggregations"]["groupby"]["buckets"]:
            ip = item['key'][field]
            site = item['key'][str(field+'_site')]
            ipv6 = item['key']['ipv6']
            if ((ip in res_ip_site.keys()) and (site is not None)) or (ip not in res_ip_site.keys()):
                res_ip_site[ip] = [site, ipv6]
    return res_ip_site


def get_host_site(idx, dateFrom, dateTo):
    def q_host_site (fld):
        return {
          "size" : 0,
          "query" : {  
            "bool" : {
              "must" : [
                {
                  "range" : {
                    "timestamp" : {
                      "from" : dateFrom,
                      "to" : dateTo
                    }
                  }
                }
              ]
            }
          },
          "_source" : False,
          "stored_fields" : "_none_",
          "aggregations" : {
            "groupby" : {
              "composite" : {
                "size" : 9999,
                "sources" : [
                  {
                    str(fld+"_site") : {
                      "terms" : {
                        "field" : str(fld+"_site"),
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    str(fld+"_host") : {
                      "terms" : {
                        "field" : str(fld+"_host"),
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  }
                ]
              }
            }
          }
        }

    for field in ['src', 'dest']:
        results = hp.es.search(index=idx, body=q_host_site(field))
        res_host_site = {}
        for item in results["aggregations"]["groupby"]["buckets"]:
            site = item['key'][str(field+"_site")]
            host = item['key'][str(field+'_host')]
            if ((host in res_host_site.keys()) and (site is not None)) or (host not in res_host_site.keys()):
                res_host_site[host] = site
    return res_host_site

def get_metadata(dateFrom, dateTo):
    def q_metadata():
        return {
          "size" : 0,
          "query" : {
            "range" : {
              "timestamp" : {
                "from" : dateFrom,
                "to" : dateTo
              }
            }
          },
          "_source" : False,
          "aggregations" : {
            "groupby" : {
              "composite" : {
                "size" : 9999,
                "sources" : [
                  {
                    "site" : {
                      "terms" : {
                        "field" : "config.site_name.keyword",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "admin_email" : {
                      "terms" : {
                        "field" : "administrator.email",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "admin_name" : {
                      "terms" : {
                        "field" : "administrator.name",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "ipv6" : {
                      "terms" : {
                        "field" : "external_address.ipv6_address",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "ipv4" : {
                      "terms" : {
                        "field" : "external_address.ipv4_address",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "host" : {
                      "terms" : {
                        "field" : "host.keyword",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  }
                ]
              }
            }
          }
        }
    
    results = hp.es.search(index='ps_meta', body=q_metadata())
    res_meta = {}
    for item in results["aggregations"]["groupby"]["buckets"]:
        host = item['key']['host']
        if ((host in res_meta.keys()) and (item['key']['site'] is not None)) or (host not in res_meta.keys()):
            res_meta[host] = {'site': item['key']['site'], 'admin_name': item['key']['admin_name'],
                              'admin_email': item['key']['admin_email'], 'ipv6': item['key']['ipv6'],
                              'ipv4': item['key']['ipv4']}
    return res_meta
