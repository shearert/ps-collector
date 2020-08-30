from __future__ import print_function

import requests
import json
from urlparse import urlparse
from pushlist import pushlist


class Mesh:
    def __init__(self, base_url = "https://psconfig.opensciencegrid.org/pub/config"):
        self.base_url = base_url

    def get_nodes(self):
        """
        Get all the nodes we should collect statistics

        :returns list: List of nodes in the mesh
        """
        # Get the top level config, with list of URLs
        response = requests.get(self.base_url, timeout=10)
        response_json = response.json()

        nodes = set()

        # Check for old style, top level with "includes"
        if isinstance(response_json, list):
            meshes = self._download_toplevel(response_json)
            for mesh_url in meshes:
                nodes.update(self._download_nodes(mesh_url=mesh_url))
        else:
            # New psconfig style
            nodes.update(self._download_nodes(response_json=response_json))

        # Remove the nodes that exist in the pushlist
        push_nodes = set(pushlist)

        # Use set subtraction to remove the push nodes
        return nodes - push_nodes


    def _download_nodes(self, mesh_url=None, response_json=None):
        """
        Download the nodes from a single mesh
        """
        nodes = set()
        if mesh_url:
            # This param may not do anything for hosts that only show psconfig format
            params = {
                "format": "meshconfig"
            }
            response = requests.get(mesh_url, timeout=10, params=params)
            response_json = response.json()
        
        if 'organizations' in response_json:
            for org in response_json.get('organizations', []):
                for site in org.get('sites', []):
                    for host in site.get('hosts', []):
                        for url in host.get('measurement_archives', []):
                            parsed = urlparse(url['read_url'])
                            nodes.update([parsed.netloc])
        else:
            # New style, psconfig
            for archive in response_json.get('archives', {}).items():
                url = archive[1]['data']['url']
                parsed = urlparse(url)
                nodes.update([parsed.netloc])

        return nodes

    def _download_toplevel(self, response_json):
        """
        Download the list of URLs for the meshes
        """
        to_return = []

        for sub_mesh in response_json:
            to_return.append(sub_mesh['include'][0])
        return to_return




if __name__ == "__main__":
    mesh = Mesh()
    print(mesh.get_nodes())


