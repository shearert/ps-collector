
"""
Install file for the perfSonar collector project.
"""

import setuptools

setuptools.setup(name="ps-collector",
                 version="2.1.0",
                 description="A daemon for aggregating perfSonar measurements",
                 author_email="discuss@sand-ci.org",
                 author="Brian Bockelman",
                 url="https://sand-ci.org",
                 package_dir={"": "src"},
                 packages=["ps_collector"],
                 scripts=['bin/ps-collector'],
                 install_requires=['schedule', 'pika', 'esmond-client'],
                 data_files=[('/etc/ps-collector', ['configs/config.ini', 'configs/logging-config.ini']),
                             ('/etc/ps-collector/config.d',  ['configs/10-site-local.ini']),
                             ('/usr/lib/systemd/system', ['configs/ps-collector.service']),
                             ('/var/lib/ps-collector', ['configs/ps-collector.state'])
                            ]
               
                )
