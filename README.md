
PS Collector
============

The PS Collector queries [perfSONAR](https://www.perfsonar.net/) instances and sends the data onto a RabbitMQ message bus.

The collector is packages in a docker conatiner available on [DockerHub](https://cloud.docker.com/u/sandci/repository/docker/sandci/ps-collector)


## Configuration

The default configuration is in configs/config.ini.  Additional configuration can be added
to config.d directory, usually installed in /etc/ps-collector/config.d/.  An example RabbitMQ
configuration is:

    [rabbitmq]

    username = nma
    password = password
    rabbit_host = example.com
    virtual_host = vhost
    queue = osg-nma-q
    exchange = osg.ps.raw
    routing_key = osg-nma-q

Place this in a file in the config.d directory.

## Running the Collector

An example `docker-compose` file is provided.  It is recommended to modify the `docker-compose.yml` file to run and start the collector.
