from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time
import socket
import re
from ipwhois import IPWhois

import queries as qrs
import helpers as hp


class HostsMetaData:

    defaultEnd = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M')
    defaultStart = datetime.strftime(datetime.now() - timedelta(days = 1), '%Y-%m-%d %H:%M')
    
    @property
    def dateFrom(self):
        return self._dateFrom

    @dateFrom.setter
    def dateFrom(self, value):
        self._dateFrom = int(time.mktime(datetime.strptime(value, "%Y-%m-%d %H:%M").timetuple())*1000)

    @property
    def dateTo(self):
        return self._dateTo

    @dateTo.setter
    def dateTo(self, value):
        self._dateTo = int(time.mktime(datetime.strptime(value, "%Y-%m-%d %H:%M").timetuple())*1000)


    def __init__(self, index, dateFrom = defaultStart,  dateTo = defaultEnd):
        self.idx = index
        self.dateFrom = dateFrom
        self.dateTo = dateTo
        self.df = self.BuildDataFrame()


    def resolveHost(self, host):
        h = ''
        try:
            if host == '127.0.0.1':
                print("Incorrect value for host")
            h = socket.gethostbyaddr(host)[0]
        except Exception as inst:
            print(str("socket exception: "+inst.args[1]))
            h = host
        return h


    def isHost(self, val):
        return re.match("^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$", val)


    def findHost(self, row, df):
        host_from_index = row['host_index']
        host_from_meta = row['host_meta']

        if self.isHost(host_from_index):
            return host_from_index
        else:
            # if it's an IP, try to get the hostname from a ps_meta records
            if (host_from_meta is not np.nan):
                if self.isHost(host_from_meta):
                    return host_from_meta
            # it is possible that for that IP has another record with the valid hostname
            elif (df[df['ip'] == host_from_index]['host_index'].values[0]):
                return df[df['ip'] == host_from_index]['host_index'].values[0]
            # otherwise we try to resolve the IP from socket.gethostbyaddr(host)
            else: 
                return self.resolveHost(host_from_index)
        return ''


    def getIPWhoIs(self, item):
        val = ''
        try:
            obj = IPWhois(item)
            res = obj.lookup_whois()
            val = res['nets'][0]['name']
            print('getIPWhoIs for:', item, '->', val)
        except Exception as inst:
            if self.isHost(item):
                val = ''
            else: val = inst.args
        return val


    def BuildDataFrame(self):
        print()
        print('Query ', self.idx, ' for the period', self.dateFrom, '-', self.dateTo)
        start = time.time()
        
        # get metadata
        meta_df = pd.DataFrame.from_dict(qrs.get_metadata(self.dateFrom, self.dateTo), orient='index',
                                 columns=['site', 'admin_name', 'admin_email', 'ipv6', 'ipv4']).reset_index().rename(columns={'index': 'host'})


        # in ES there is a limit of up to 10K buckets for aggregation, 
        # thus we need to split the queries and then merge the results
        ip_site_df = pd.DataFrame.from_dict(qrs.get_ip_site(self.idx, self.dateFrom, self.dateTo),
                                            orient='index', columns=['site', 'is_ipv6']).reset_index().rename(columns={'index': 'ip'})
        ip_host_df = pd.DataFrame.from_dict(qrs.get_ip_host(self.idx, self.dateFrom, self.dateTo),
                                            orient='index', columns=['host']).reset_index().rename(columns={'index': 'ip'})
        host_site_df = pd.DataFrame.from_dict(qrs.get_host_site(self.idx, self.dateFrom, self.dateTo),
                                              orient='index', columns=['site']).reset_index().rename(columns={'index': 'host'})

        df = pd.merge(ip_site_df, ip_host_df, how='outer', left_on=['ip'], right_on=['ip'])
        df = pd.merge(df, host_site_df, how='left', left_on=['host', 'site'], right_on=['host', 'site'])
        df = pd.merge(df, meta_df[['host', 'site']], how='left', left_on=['host'], right_on=['host'])

        df = pd.merge(df, meta_df[['host', 'ipv4']], how='left', left_on=['ip'], right_on=['ipv4'])
        df = pd.merge(df, meta_df[['host', 'ipv6']], how='left', left_on=['ip'], right_on=['ipv6'])

        df['host_meta'] = np.where((df['host_y'].isnull()), df['host'], df['host_y'])
        df.drop(['host_y', 'host'], inplace=True, axis=1)
        df = pd.merge(df, meta_df[['host', 'admin_email', 'admin_name']], how='left', left_on=['host_meta'], right_on=['host'])

        # mark hosts and IPs not part of the configuration
        df['ip_in_ps_meta'] = np.where((df['ipv4'].isnull() & df['ipv6'].isnull()), False, True)
        df['host_in_ps_meta'] = np.where((df['host_meta'].isnull()), False, True)

        df.drop(['ipv4', 'ipv6', 'host'], inplace=True, axis=1)
        df.rename(columns={'host_x': 'host_index', 'host_y': 'host_meta'}, inplace=True)

        df['host_has_aliases'] = df.apply(lambda row: True if (row['ip'] in df.loc[df.duplicated('ip', keep=False)]['ip'].values) 
                                                           else False, axis=1)
        # # in some cases there are > 1 rows having the same IP due to slightly different site name or host name takes a CNAME
        df.drop_duplicates(subset='ip', keep="first", inplace=True)

        df['host'] = df.apply(lambda row: self.findHost(row, df), axis=1)

        # get site name if it exists in ps_*, else get site name from ps_meta if it's not null, 
        # otherwise use IPWhoIS network name, but only fro the hosts part of the configuration (present in ps_meta)
        df['site'] = np.where(df['site_x'].isnull(), np.where(df['site_y'].isnull(), '', df['site_y']), df['site_x'])
        df.rename(columns={'site_x': 'site_index', 'site_y': 'site_meta'}, inplace=True)
        df['site'] = df.apply(lambda row: self.getIPWhoIs(row['ip']) 
                                          if (row['site'] == '' and row['host_in_ps_meta'] == True) 
                                          else row['site'], axis=1)

        print("BuildDataFrame took %ss" % (int(time.time() - start)))
        return df


    def RemoveUnnecessaryData(self, idx_df):
        start = time.time()
        valid = self.df[self.df['host_in_ps_meta'] == True]
        host_list = valid['host_index'].values

        def markForDeletion(row):
            if (row['src_host'] in host_list) and (row['dest_host'] in host_list):
                return True
            return False

        idx_df['keep'] = idx_df.apply(lambda row: markForDeletion(row), axis=1)
        idx_df = idx_df[idx_df['keep'] == True].drop(columns=['keep'])
        
        print("Data removal took %ss" % (int(time.time() - start)))
        return idx_df