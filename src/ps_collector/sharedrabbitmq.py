
import pika
from configparser import ConfigParser


# Singleton from:
# https://python-3-patterns-idioms-test.readthedocs.io/en/latest/Singleton.html
class SharedRabbitMQ:
    def __init__(self, config):
        # Create the connection to the bus
        self.config = config
        self.username = self._readConfig('username')
        self.password = self._readConfig('password')
        self.rabbithost = self._readConfig('rabbit_host')
        self.virtual_host = self._readConfig('virtual_host')
        self.queue = self._readConfig('queue')
        self.exchange = self._readConfig('exchange')
        self.routing_key = self._readConfig('routing_key')
        
        credentials = pika.PlainCredentials(self.username, self.password)
        self.parameters = pika.ConnectionParameters(host=self.rabbithost,virtual_host=self.virtual_host,credentials=credentials)
        self.connection = pika.BlockingConnection(self.parameters)
        
    def _readConfig(self, option):
        return self.config.get('rabbitmq', option)

    def createChannel():
        """
        Create a channel and return it
        """
        return self.conn.channel()


