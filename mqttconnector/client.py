import paho.mqtt.client as mqtt
import time
import re
import logging


logger = logging.getLogger('root')

class MQTTClient(object):

    #TODO use TLS (port 8883)
    def __init__(self, host="localhost", port=8883, user='', password='', tls_cert='', keepalive=60):
        self._host = host
        self._port = port
        self.user = user
        self.password = password
        self.tls_cert = tls_cert
        self._keepalive = keepalive

        self._client = mqtt.Client()

        self._subscriptions = {}
        self._isconnected = False


    def connect(self, forever=False):
        """ Connect to MQTT broker

        Keyword Arguments:
            user {str} -- Username (default: {''})
            password {str} -- Password (default: {''})
            forever {bool} -- Imediately start mqtt thread if forever=False, otherwise start() has to be called explicitly (default: {False})
        """
        # pass user information if available
        if self.user and self.password:
            self._client.username_pw_set(self.user, password=self.password)

        # enable TLS if cert if provided
        if self.tls_cert:
            self._client.tls_set(self.tls_cert)
            self._client.tls_insecure_set(True)

        # set communication handles
        self._client.on_connect = self.on_connect
        self._client.on_disconnect = self.on_disconnect
        self._client.on_message = self.on_message
        self._isconnected = False
        self._client.reconnect_delay_set(min_delay=1, max_delay=5)
        # open up connection
        try:
            self._client.connect(self._host, self._port, self._keepalive)

            # if not forever:
            #     self._client.loop_start()
            # start new listening thread (non-blocking) in case forever=False

        except Exception as error:
            self._isconnected = False
            logger.error(f'MQTT-Error: {error.strerror}')
 
            # print("MQTT-Error: " +  error.strerror )
 
        #    print("MQTT-Error: " +  error.strerror +  " (" + str(error.winerror) + ")")
        # immer starten, damit im Fehler-Fall (bei not connect) ein automatisches reconnect erfolgt
        finally:
            if not forever:
                self._client.loop_start()

    def isConnected(self):
        """ isConnected
        """
        return self._isconnected

    def start(self):
        """ Start a blocking loop (only needed if connect(forever=True) is set)
        """
        self._client.loop_forever()

    def last_will(self, topic, message, retain=True):
        self._client.will_set(topic, message, retain=retain)

    def subscribe(self, topic, handler):
        """ Subscribe to particular MQTT topic

        Arguments:
            topic {str} -- topic path
        """
        res = self._client.subscribe(topic)

        if not topic in self._subscriptions:
            self._subscriptions[topic] = []

        self._subscriptions[topic].append(handler)


    def publish(self, topic, message, qos=0, retain=False):
        """ Publish message to topic

        Arguments:
            topic {str} -- topic path
            message {str} -- message to publish
        """
        self._client.publish(topic, message, qos=qos, retain=retain)

    def getConnectMessageByReturnValue(self, rc):
        if rc == 0: return "Connection successful"
        elif rc == 1: return "Connection refused - incorrect protocol version"
        elif rc == 2: return "Connection refused - invalid client identifier"
        elif rc == 3: return "Connection refused - server unavailable"
        elif rc == 4: return "Connection refused - bad username or password"
        elif rc == 5: return "Connection refused - not authorised"
        else: return "Connection refused - unknown Code: " + str (rc)

    def on_connect(self, client, userdata, flags, rc):
        """ Connection handler for MQTT client

        Arguments:
            client {[type]} -- [description]
            userdata {[type]} -- [description]
            flags {[type]} -- [description]
            rc {[type]} -- [description]
        """

        str_ = self.getConnectMessageByReturnValue(rc)
        logger.info(f'MQTT-Connected with result code: {str_}')
        # print("MQTT-Connected with result code: " + str_)

        # resubscribe to all previously subscribed topics
        for topic, _ in self._subscriptions.items():
            self._client.subscribe(topic)
        self._isconnected = True

    def on_disconnect(self, client, userdata, rc):
        """ Disconnection handler for MQTT client
     
        Arguments:
            client {[type]} -- [description]
            userdata {[type]} -- [description]
            rc {[type]} -- [description]
        """
        self._isconnected = False
        if rc != 0:
            logger.info(f'MQTT-Disconnected with result code: {str(rc)}')
            logger.error(f'MQTT-Disconnected with result code: {str(rc)}')
 
            # print("MQTT-Disconnected with result code: " + str(rc))
        else:
            logger.info(f'MQTT-Unexpected Disconnected with result code: {str(rc)}')
            logger.error(f'MQTT-Unexpected Disconnected with result code: {str(rc)}')
            # print("MQTT-Unexpected Disconnected with result code: " + str(rc))


    def on_message(self, client, userdata, msg):
        """ Message handler for MQTT client

        Arguments:
            client {[type]} -- [description]
            userdata {[type]} -- [description]
            msg {str} -- payload
        """
        # pass message content to respective handler for topic
        try:
            # use wildcard_topics if necessary
            topic = self.get_wildchard_topics(msg.topic)

            if topic in self._subscriptions:
                for handler in self._subscriptions[topic]:
                    handler(msg.payload)
                    #print(msg.topic + " " + str(msg.payload))
        except Exception as e:
            logger.error(f'MQTT-on_message error result code: {e}')
            # print(e)

    def get_wildchard_topics(self, topic):
        """ Returns wildcard topic in the subscription list that matches the given topic
        Return original topic if nothing was found

        Arguments:
            topic {str} -- MQTT topic
        """
        # check "+" wildcards
        wildcard_subscriptions = list(map(lambda x: x.replace('+', '[A-z0-9_]+'), self._subscriptions))
        for index, wildcard in enumerate(wildcard_subscriptions):
            if re.search(f"^{wildcard}$", topic):
                return list(self._subscriptions)[index]

        # check "#" wildcards
        wildcard_subscriptions = list(map(lambda x: x.replace('#', '[A-z0-9_/]+'), self._subscriptions))
        for index, wildcard in enumerate(wildcard_subscriptions):
            if re.search(f"^{wildcard}$", topic):
                return list(self._subscriptions)[index]

        return topic