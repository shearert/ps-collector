import os
import requests
import json
from esmond.api.client.perfsonar.query import ApiConnect
from esmond.api.client.perfsonar.query import Metadata
import requesocks

class SocksApiConnect(ApiConnect):

    def get_metadata(self):
        if self.script_alias:
            archive_url = '{0}/{1}/perfsonar/archive/'.format(self.api_url, self.script_alias)
        else:
            archive_url = '{0}/perfsonar/archive/'.format(self.api_url)
        
        session = requesocks.session()
        if os.getenv('SOCKS5'):
            session.proxies = {'http': os.getenv('SOCKS5'), 'https': os.getenv('SOCKS5')}
        session.verify=False
        r = session.get(archive_url, 
           params=dict(self.filters.metadata_filters, **self.filters.time_filters),
           headers = self.request_headers)

        self.inspect_request(r)

        if r.status_code == 200 and \
            r.headers['content-type'] == 'application/json':
            data = json.loads(r.text)
            for i in data:
                yield Metadata(i, self.api_url, self.filters)
        else:
            self.http_alert(r)
            return
            yield


