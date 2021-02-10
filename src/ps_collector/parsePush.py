
import json
import socket
from socket import AddressFamily
import dateutil.parser
from urllib.parse import urlparse
import pika
from .pushlist import pushlist
import multiprocessing
import traceback
from .ttldict import TTLOrderedDict
import functools
import concurrent.futures
import logging


class Host:
    def __init__(self, hostname_or_ip: str):
        self.hostname_or_ip = hostname_or_ip

        self.ipv4_address = ""
        self.ipv6_address = ""
        self.hostname = ""
        self._resolveHost()

    def _resolveHost(self):
        # First, check if we have a hostname or IP
        if self.hostname_or_ip.startswith("http://") or self.hostname_or_ip.startswith("https://"):
            parsed_url = urlparse(self.hostname_or_ip)
            self.hostname_or_ip = parsed_url.netloc

        is_hostname = True
        try:
            socket.inet_pton(AddressFamily.AF_INET, self.hostname_or_ip)
            # If we get here, it's an ipv4 address
            is_hostname = False
        except OSError:
            pass

        try:
            socket.inet_pton(AddressFamily.AF_INET6, self.hostname_or_ip)
            # If we get here, it's an ipv4 address
            is_hostname = False
        except OSError:
            pass

        if is_hostname:
            self.hostname = self.hostname_or_ip
        else:
            # Reverse resolve the hostname
            self.hostname = socket.gethostbyaddr(self.hostname_or_ip)[0]

        try:
            addresses = socket.getaddrinfo(self.hostname_or_ip, None)
            for address in addresses:
                if address[0] == AddressFamily.AF_INET:
                    self.ipv4_address = address[4][0]

                elif address[0] == AddressFamily.AF_INET6:
                    self.ipv6_address = address[4][0]
                # We received an ip address
        except socket.gaierror as gaerror:
            # Failed to resolve the hostname or ip
            pass

    def __str__(self):
        return "ipv4: {}, ipv6: {}, hostname: {}".format(self.ipv4_address, self.ipv6_address, self.hostname)




class MessageBus:

    def __init__(self, config, log: logging.Logger):
        self._parse_func = None
        self.config = config
        self.chan = None
        self.conn = None
        self._last_deliver_tag = None
        self.timer_id = None
        self.log = log
        self.timer_functions = []
        self.threadpool = concurrent.futures.ThreadPoolExecutor(max_workers=20)

    def _autoconfigure(self):
        if self.conn:
            del self.conn
        # Get the config from the enviroment
        needed_configs = ["rabbit_username", "rabbit_password", "rabbit_host", "rabbit_virtual_host", "recv_queue", "send_exchange"]

        # Check that all of the config is in the environment
        if not all(option in self.config for option in needed_configs):
            raise NameError("Not all required Message Bus configuration options were provided in the configuration")

        # Connect to the message bus
        cred = pika.PlainCredentials(self.config['rabbit_username'], self.config['rabbit_password'])
        params = pika.ConnectionParameters(self.config['rabbit_host'],
                                       5672,
                                       self.config['rabbit_virtual_host'],
                                       cred)

        self.conn = pika.BlockingConnection(params)
        self.recv_chan = self.conn.channel()
        self.recv_chan.basic_qos(prefetch_count=256)
        self.send_chan = self.conn.channel()
        for timer_func, timeout in self.timer_functions:
            # Create the partial function
            self.log.debug("Adding a custom timeout function")
            partial = functools.partial(MessageBus._customTimeouts, timer_func, timeout, self.conn)
            self.conn.call_later(timeout, partial)

    @staticmethod
    def _customTimeouts(timer_func, timeout, connection:pika.BlockingConnection):
        """
        Method to call the underlying function, while reseting the timer to run again
        """
        # Create another partial
        partial = functools.partial(MessageBus._customTimeouts, timer_func, timeout, connection)

        # Reset the timer
        connection.call_later(timeout, partial)

        # Call the function
        timer_func()


    def on_message(self, channel, method_frame, header_frame, body):
        """
        On each message, submit the processing to the thread pool
        """
        self.threadpool.submit(self._start_processing, self.conn, channel, method_frame.delivery_tag, body)

    def _start_processing(self, connection: pika.BlockingConnection, channel: pika.channel, delivery_tag, body):
        """
        This should be executed within a new thread
        """
        try:
            if self._parse_func:
                self._parse_func(json.loads(body))
        except Exception as e:
            self.log.exception("Failed to parse message: " + str(body))
            # nack this message
            connection.add_callback_threadsafe(functools.partial(channel.basic_nack, delivery_tag=delivery_tag))
            raise e

        # Acknowledge the message
        connection.add_callback_threadsafe(functools.partial(channel.basic_ack, delivery_tag=delivery_tag))


    def start(self):
        """
        Start the io loop with the message bus
        """
        while True:
            try:
                self._autoconfigure()
                self.recv_chan.queue_declare(self.config['recv_queue'], durable = True, auto_delete = False)
                self.recv_chan.basic_consume(self.config['recv_queue'], self.on_message)
                # Set a timer

                try:
                    self.recv_chan.start_consuming()
                except KeyboardInterrupt:
                    self.recv_chan.stop_consuming()
                    self.conn.close()
                    break
            except pika.exceptions.ConnectionClosedByBroker:
                # Uncomment this to make the example not attempt recovery
                # from server-initiated connection closure, including
                # when the node is stopped cleanly
                #
                # break
                continue
            # Do not recover on channel errors
            except pika.exceptions.AMQPChannelError as err:
                print(("Caught a channel error: {}, retrying...".format(err)))
                continue
            # Recover on all other connection errors
            except pika.exceptions.AMQPConnectionError:
                print("Connection was closed, retrying...")
                continue



    def sendParsed(self, topic: str, parsed_message: dict):
        """
        Send the parsed message

        :param topic: Topic to send the message
        :param parsed_message: A dictionary of the parsed message.  
            Will be json'ified in this function.
        """

        # Thread safe send parsed
        send_partial = functools.partial(self.send_chan.basic_publish, self.config['send_exchange'], topic, json.dumps(parsed_message))
        self.conn.add_callback_threadsafe(send_partial)

    def registerParsed(self, parser_function):
        """
        Register a parser function to receive the pushed messages.  The function must be threadsafe

        :param parser_function: A function of the type function(message: dict)

        """
        self._parse_func = parser_function
    
    def registerTimer(self, timer_function, timeout):
        """
        Register a function to be called after timeout seconds

        :param timer_funciton: Function to be called after timeout seconds of the type: function()
        :param timeout: Number of seconds between calls to timer_function
        """
        # Create the timer
        self.log.debug("Adding timer function")
        self.timer_functions.append((timer_function, timeout))


# Modeled after:
# https://stackoverflow.com/questions/19924104/python-multiprocessing-handling-child-errors-in-parent

class PSPushParser(multiprocessing.Process):
    def __init__(self, config, log):
        super(PSPushParser, self).__init__()
        self._config = config
        self._log = log
        self._log.debug("Creating the push parser")
        self.error_queue = multiprocessing.Queue()
        self._exception = None
        self._message_bus = None
        self.topic_map = {
            'latencybg': {
                'topic': 'perfsonar.raw.histogram-owdelay',
                'datapoint': 'histogram-latency'
            },
            'latency': {
                'topic': 'perfsonar.raw.histogram-owdelay',
                'datapoint': 'histogram-latency'
            },
            'trace': {
                'topic': 'perfsonar.raw.packet-trace',
                'datapoint': 'paths'
            },
            'throughput': {
                'topic': 'perfsonar.raw.throughput',
                'datapoint': ''
            }
        }

        # An expiring dictionary that is used to update the list of MA
        # which we are getting pushed data.
        self.ttldict = TTLOrderedDict(60*5)

    def syncPushList(self):
        # Clear the list
        for num in range(len(pushlist)):
            pushlist.pop()
        self._log.debug("List of hosts pushing to the queue: {}".format(str(self.ttldict.keys())))
        pushlist.extend(list(self.ttldict.keys()))

    def run(self):
        """
        Start the execution of the push parser
        """
        try:
            self._log.debug("Starting running the push message bus")
            # First, create and configure the message bus
            self._message_bus = MessageBus(self._config['Push'], self._log)
            # Register the parser with the message bus
            self._message_bus.registerParsed(self.parse_push)

            # Register the sync list
            self._message_bus.registerTimer(self.syncPushList, 60)

            # Start the message bus
            self._message_bus.start()

        except Exception as e:
            tb = traceback.format_exc()
            self.error_queue.put((e, tb))
            raise e

    @property
    def exception(self):
        if not self.error_queue.empty():
            self._exception = self.error_queue.get()
        return self._exception


    def parse_push(self, parsed_object: dict):

        # First, fill out the metadata
        to_return = {}

        # If the test failed, then ignore the record
        if not parsed_object['result']['succeeded']:
            return True

        test_type = parsed_object['test']['type']
        ip_version = parsed_object['test']['spec'].get("ip-version", None)
        # Source
        source_host = Host(parsed_object['test']['spec']['source'])
        dest_host = Host(parsed_object['test']['spec']['dest'])
        # There should be a [headers][x_ps_observer]
        if 'headers' in parsed_object and 'x_ps_observer' in parsed_object['headers']:
            ma_host = Host(parsed_object['headers']['x_ps_observer'])
        else:
            self._log.error("Attribute [headers][x_ps_observer] not in pushed message")
            ma_host = Host(parsed_object['task']['href'])
        to_return['meta'] = {}
        to_return['meta']['input_source'] = source_host.hostname
        to_return['meta']['input_destination'] = dest_host.hostname

        if ip_version == 6 or (not ip_version and source_host.ipv6_address and dest_host.ipv6_address):
            to_return['meta']['source'] = source_host.ipv6_address
            to_return['meta']['destination'] = dest_host.ipv6_address
            to_return['meta']['measurement_agent'] = ma_host.ipv6_address if ma_host.ipv6_address else ma_host.hostname
        elif ip_version == 4 or (not ip_version and source_host.ipv4_address and dest_host.ipv4_address):
            to_return['meta']['source'] = source_host.ipv4_address
            to_return['meta']['destination'] = dest_host.ipv4_address
            to_return['meta']['measurement_agent'] = ma_host.ipv4_address if ma_host.ipv4_address else ma_host.hostname

        # Set the version
        to_return['version'] = 2
        to_return['meta']['push'] = True

        # Now, transform the datapoints
        # Datapoints should be in the shape of {timestamp: [datapoints]}
        # Use the start-time
        timestamp = dateutil.parser.parse(parsed_object['run']['start-time']).timestamp()

        if test_type == "throughput":
            # Create the retransmits event type
            retransmits = parsed_object['result']['summary']['summary']['retransmits']
            to_return['datapoints'] = {
                int(timestamp): int(retransmits)
            }
            self._message_bus.sendParsed("perfsonar.raw.packet-retransmits", to_return)

            # For throughput, we only need the throughput-bits in the result summary.
            throughput = parsed_object['result']['summary']['summary']['throughput-bits']
            to_return['datapoints'] = {
                int(timestamp): int(throughput)
            }

        else:
            if test_type == "latencybg":
                lost_packets = parsed_object['result']['packets-lost'] / parsed_object['result']['packets-sent']
                to_return['datapoints'] = {
                    int(timestamp): lost_packets
                }
                self._message_bus.sendParsed("perfsonar.raw.packet-loss-rate", to_return)

            # For non latency tests
            to_return['datapoints'] = {
                int(timestamp): parsed_object['result'][self.topic_map[test_type]['datapoint']]
            }

        # Update the ttl dict with the just received MA, which will also update the TTL
        if ma_host.hostname not in self.ttldict:
            self.ttldict[ma_host.hostname] = 1
        else:
            self.ttldict.set_ttl(ma_host.hostname, 60*5)

        self._message_bus.sendParsed(self.topic_map[test_type]['topic'], to_return)


        return True


