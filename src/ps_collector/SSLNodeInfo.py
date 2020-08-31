import json
import os
import copy
from .esmond.api.client.perfsonar.query import EventType
from .esmond.api.client.perfsonar.query import Metadata
from .esmond.api.client.perfsonar.query import Summary
from .esmond.api.client.perfsonar.query import QueryLimitException

import requests
requests.packages.urllib3.disable_warnings()

class EventTypeSSL(EventType):
    def __init__(self, EventTypeParent, cert=None, key=None):
        self.cert = cert
        self.key = key
        super(EventTypeSSL, self).__init__(EventTypeParent._data, EventTypeParent.api_url, EventTypeParent.filters)

    def _query_with_limit(self, verify=False):
        """Internal method used by the get_data() methods in the EventType                                                  
        and Summary sub-classes. Make a series of limited queries in a loop                                                 
        and return the compiled results.                                                                                    
        Meant to optimize pulls of large amounts of data."""

        if self.filters.verbose: print(' * looping query for: {0}'.format(self.query_uri))

        # XXX(mmg) - revisit this value?                                                                                    
        LIMIT = 1000
        q_params = copy.copy(self.filters.time_filters)
        q_params['limit'] = LIMIT

        data_payload = []
        while 1:
            if self.cert and self.key:
                self.api_url = self.api_url.replace("http://", "https://", 1)
                if self.filters.verbose: print('Changed api url for: {0}'.format(self.api_url))
                r = requests.get('{0}{1}'.format(self.api_url, self.query_uri),
                                 params=q_params,
                                 headers=self.request_headers,
                                 verify=verify, cert=(self.cert,self.key))
            else:
                r = requests.get('{0}{1}'.format(self.api_url, self.query_uri),
                                 params=q_params ,
                                 headers=self.request_headers)
            self.inspect_request(r)

            if r.status_code == 200 and \
                r.headers['content-type'] == 'application/json':
                data = json.loads(r.text)

                data_payload += data

                if self.filters.verbose: print('  ** got {0} results'.format(len(data)))

                if len(data) < LIMIT:
                    # got less than requested - done                                                                        
                    break
                else:
                    # reset start time to last ts + 1 and loop                                                              
                    q_params['time-start'] = data[-1].get('ts') + 1
                # sanity check - this should not happen other than the unlikely                                             
                # scenario where the final request results is exactly == LIMIT                                              
                if q_params['time-start'] >= q_params['time-end']:
                    self.warn('time start >= time end - exiting query loop')
                    break
            else:
                print('Problems with the the connection to')
                self.http_alert(r)
                raise QueryLimitException
        if self.filters.verbose: print('  *** finished with {0} results'.format(len(data_payload)))

        return data_payload

class SummarySSL(Summary):
    def __init__(self, SummaryParent, cert=None, key=None):
        self.cert = cert
        self.key = key
        super(SummarySSL, self).__init__(SummaryParent._data, SummaryParent.api_url,  SummaryParent.filters,  SummaryParent._data_type)

    def _query_with_limit(self, verify=False):
        """Internal method used by the get_data() methods in the EventType 
        and Summary sub-classes. Make a series of limited queries in a loop 
        and return the compiled results. 
        Meant to optimize pulls of large amounts of data."""
        if self.filters.verbose: print(' * looping query for: {0}'.format(self.query_uri))

        # XXX(mmg) - revisit this value?
        LIMIT = 1000

        q_params = copy.copy(self.filters.time_filters)
        q_params['limit'] = LIMIT

        data_payload = []
        while 1:
            if self.cert and self.key:
                self.api_url = self.api_url.replace("http://", "https://", 1)
                if self.filters.verbose: print('Changed api url for: {0}'.format(self.api_url))
                r = requests.get('{0}{1}'.format(self.api_url, self.query_uri),
                                 params=q_params, 
                                 headers=self.request_headers, 
                                 verify=verify, cert=(self.cert,self.key))
            else:
                r = requests.get('{0}{1}'.format(self.api_url, self.query_uri),
                                 params=q_params ,
                                 headers=self.request_headers)

            self.inspect_request(r)

            if r.status_code == 200 and \
                r.headers['content-type'] == 'application/json':
                data = json.loads(r.text)

                data_payload += data

                if self.filters.verbose: print('  ** got {0} results'.format(len(data)))

                if len(data) < LIMIT:
                    # got less than requested - done
                    break
                else:
                    # reset start time to last ts + 1 and loop
                    q_params['time-start'] = data[-1].get('ts') + 1

                # sanity check - this should not happen other than the unlikely
                # scenario where the final request results is exactly == LIMIT
                if q_params['time-start'] >= q_params['time-end']:
                    self.warn('time start >= time end - exiting query loop')
                    break
            else:
                self.http_alert(r)
                raise QueryLimitException

        if self.filters.verbose: print('  *** finished with {0} results'.format(len(data_payload)))

        return data_payload
