import inspect
import paho.mqtt.client as mqtt
import time
import re
import logging
import ssl
import threading

# import inspect
# from functools import wraps
from inspect import Parameter, Signature

logger = logging.getLogger("root")


class MQTTClient(object):

    # TODO use TLS (port 8883)
    def __init__(
        self,
        host="localhost",
        port=8883,
        user="",
        password="",
        tls_ca="",
        tls_cert="",
        tls_key="",
        keepalive=60,
    ):
        self._host = host
        self._port = port
        self.user = user
        self.password = password
        self.tls_cert = tls_cert
        self.tls_ca = tls_ca
        self.tls_key = tls_key

        self._keepalive = keepalive

        self._client = mqtt.Client()

        self._subscriptions = {}
        self._isconnected = False
        self._tlsIsConfigured = False
        self._connectHandler = None
        self._disconnectHandler = None

    #  connectHandler in form of 'def on_connect(self, client, userdata, flags, rc):'
    #  disconnectHandler in form of 'def on_disconnect(self, client, userdata, rc):'

    def connect(self, forever=False, connectHandler=None, disconnectHandler=None):
        """Connect to MQTT broker

        Keyword Arguments:
            user {str} -- Username (default: {''})
            password {str} -- Password (default: {''})
            forever {bool} -- Imediately start mqtt thread if forever=False, otherwise start() has to be called explicitly (default: {False})
        """
        # pass user information if available
        if self.user and self.password:
            self._client.username_pw_set(self.user, password=self.password)

        # enable TLS if cert if provided: TLSv1.2: all certs file are necessary
        if self._tlsIsConfigured == False:
            if self.tls_cert and self.tls_ca and self.tls_key:
                self._client.tls_set(
                    ca_certs=self.tls_ca,
                    certfile=self.tls_cert,
                    keyfile=self.tls_key,
                    cert_reqs=ssl.CERT_REQUIRED,
                    tls_version=ssl.PROTOCOL_TLSv1_2,
                )
                self._tlsIsConfigured = True
                # self._client.tls_insecure_set(True)
            else:
                # enable TLS if cert if provided: TLSv1
                if self.tls_cert:
                    self._client.tls_set(self.tls_cert)
                    self._client.tls_insecure_set(True)
                self._tlsIsConfigured = True

        self._connectHandler = connectHandler
        self._disconnectHandler = disconnectHandler
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
            logger.error(f"MQTT-Error: {str(error)}")

            # print("MQTT-Error: " +  error.strerror )

        #    print("MQTT-Error: " +  error.strerror +  " (" + str(error.winerror) + ")")
        # immer starten, damit im Fehler-Fall (bei not connect) ein automatisches reconnect erfolgt
        finally:
            if not forever:
                self._client.loop_start()

    def isConnected(self):
        """isConnected"""
        return self._isconnected

    def start(self):
        """Start a blocking loop (only needed if connect(forever=True) is set)"""
        self._client.loop_forever()

    def last_will(self, topic, message, retain=True):
        try:
            self._client.will_set(topic, message, retain=retain)
        except Exception as e:
            logger.error(
                f"MQTT-set last will {message}  error result code: {e}")

    def getSubscriptions(self) -> dict:
        return self._subscriptions

    def unsubscribeallTopics(self):
        """unsubscribeallTopics to particular MQTT topic

        Arguments:
            topic {str} -- topic path
        """
        try:
            topics = [key for key in self._subscriptions.keys()]
            if len(topics) > 0:
                self._client.unsubscribe(topics)
            self._subscriptions = {}
        except Exception as e:
            logger.error(f"MQTT-unsubscribeallTopics  error result code: {e}")

    def unsubscribe(self, topic: str):
        """Subscribe to particular MQTT topic

        Arguments:
            topic {str} -- topic path
        """
        try:
            self._client.unsubscribe(topic)
            if topic in self._subscriptions:
                del self._subscriptions[topic]

        except Exception as e:
            logger.error(
                f"MQTT-unsubscribe topic {topic} error result code: {e}")

    def apply_topiclist(self, topics:list=[]): 
    
        qos = 0
        topictupels = [(key, qos) for key in topics]
        if len(topictupels) > 0:
            self._client.subscribe(topictupels)
            
    def configureHandler(self, handler) -> dict:
        func_sig = inspect.signature(handler)
        doWriteTopic = False
        for func_arg in (func_sig.parameters.values()):
            if func_arg.name == "topic":
                doWriteTopic = True
                break

        return {"funchandler": handler, "doUseTopic": doWriteTopic}
    def apply_topiclist(self, topics:list()=[]):

        qos = 0
        topictupels = [(key, qos) for key in topics]
        if len(topictupels) > 0:
            self._client.subscribe(topictupels)

    def subscribe(self, topic: str, handler, applySubsribe: bool = True):
        """Subscribe to particular MQTT topic

        Arguments:
            topic {str} -- topic path
            handler: in form funchandler(topic, payload) or
            # per definition name of parameter must be "topic"
            handler: in form funchandler(payload)
        """

        try:
            configurehandler = self.configureHandler(handler)
            if not topic in self._subscriptions:
                if applySubsribe:
                    res = self._client.subscribe(topic)
                self._subscriptions[topic] = []
                self._subscriptions[topic].append(configurehandler)
            else:
                subscribedhandlers = self._subscriptions[topic]
                alreadysubscribedhandler = [
                    subscribedhandler
                    for subscribedhandler in subscribedhandlers
                    if handler == subscribedhandler["funchandler"]
                ]
                # srw2ho 21.04.2023: nur zulassen, wenn gleicher handler nicht bereits subsribed ist!!!!
                if len(alreadysubscribedhandler) == 0:
                    if applySubsribe:
                        res = self._client.subscribe(topic)
                    self._subscriptions[topic].append(configurehandler)

        except Exception as e:
            logger.error(f"MQTT-subscribe topic {topic} error result code: {e}")

    def publish(self, topic, message, qos=0, retain=False):
        """Publish message to topic

        Arguments:
            topic {str} -- topic path
            message {str} -- message to publish
        """
        try:
            self._client.publish(topic, message, qos=qos, retain=retain)
        except Exception as e:
            logger.error(f"MQTT-publish message error result code: {e}")
            # print(e)

    def getConnectMessageByReturnValue(self, rc):
        if rc == 0:
            return "Connection successful"
        elif rc == 1:
            return "Connection refused - incorrect protocol version"
        elif rc == 2:
            return "Connection refused - invalid client identifier"
        elif rc == 3:
            return "Connection refused - server unavailable"
        elif rc == 4:
            return "Connection refused - bad username or password"
        elif rc == 5:
            return "Connection refused - not authorised"
        else:
            return "Connection refused - unknown Code: " + str(rc)

    def on_connect(self, client, userdata, flags, rc):
        # srw2ho 20.04.2023: Threadnames umbiegen for debugging rausnehmen -> kann auch in Thread-Handler for MQTT Request zum Teil erledigt werden
        # threading.current_thread().name = (
        #     "client.py" + "_on_connect_" + str(threading.get_ident())
        # )
        """Connection handler for MQTT client

        Arguments:
            client {[type]} -- [description]
            userdata {[type]} -- [description]
            flags {[type]} -- [description]
            rc {[type]} -- [description]
        """

        # print("MQTT-Connected with result code: " + str_)

        # resubscribe to all previously subscribed topics
        # for topic, _ in self._subscriptions.items():
        #     self._client.subscribe(topic)

        # qos default = 0
        try:
            qos = 0
            topictupels = [(key, qos)
                           for key in self._subscriptions.keys()]
            if len(topictupels) > 0:
                self._client.subscribe(topictupels)

            self._isconnected = True

            str_ = self.getConnectMessageByReturnValue(rc)
            logger.info(f"MQTT-Connected with result code: {str_}")

            if self._connectHandler != None:
                self._connectHandler(client, userdata, flags, rc)

        except Exception as e:
            logger.error(f"MQTT-on_connect error result code: {e}")

    def on_disconnect(self, client, userdata, rc):
        # srw2ho 20.04.2023: Threadnames umbiegen for debugging rausnehmen -> kann auch in Thread-Handler for MQTT Request zum Teil erledigt werden
        # threading.current_thread().name = (
        #     "client.py" + "_on_diconnect_" + str(threading.get_ident())
        # )
        """Disconnection handler for MQTT client

        Arguments:
            client {[type]} -- [description]
            userdata {[type]} -- [description]
            rc {[type]} -- [description]
        """
        self._isconnected = False
        if rc != 0:
            logger.info(f"MQTT-Disconnected with result code: {str(rc)}")
            logger.error(f"MQTT-Disconnected with result code: {str(rc)}")

            # print("MQTT-Disconnected with result code: " + str(rc))
        else:
            logger.info(
                f"MQTT-Unexpected Disconnected with result code: {str(rc)}")
            logger.error(
                f"MQTT-Unexpected Disconnected with result code: {str(rc)}")
            # print("MQTT-Unexpected Disconnected with result code: " + str(rc))
        if self._disconnectHandler != None:
            self._disconnectHandler(client, userdata, rc)

    def on_message(self, client, userdata, msg):

        '''_summary_
    
        '''
        # srw2ho 20.04.2023: Threadnames umbiegen for debugging rausnehmen -> kann auch in Thread-Handler for MQTT Request zum Teil erledigt werden
        # threading.current_thread().name = (
        #     "client.py" + "_on_message_" + str(threading.get_ident())
        # )
        """Message handler for MQTT client

        Arguments:
            client {[type]} -- [description]
            userdata {[type]} -- [description]
            msg {str} -- payload
        """
        # pass message content to respective handler for topic
        try:
               # use wildcard_topics if necessary
            doHandler = False
            if msg.topic in self._subscriptions:
                topic = msg.topic
                doHandler = True
            else:
                topic = self.get_wildchard_topics(msg.topic)
                if topic in self._subscriptions:
                    doHandler = True

            if doHandler:
                for handler in self._subscriptions[topic]:
                    funchandler = handler["funchandler"]
                    if handler["doUseTopic"]:
                        funchandler(msg.topic, msg.payload)
                    else:
                        funchandler(msg.payload)
                    # print(msg.topic + " " + str(msg.payload))
        except Exception as e:
            logger.error(f"MQTT-on_message error result code: {e}")
            # print(e)

    def get_wildchard_topics(self, topic):
        """Returns wildcard topic in the subscription list that matches the given topic
        Return original topic if nothing was found

        Arguments:
            topic {str} -- MQTT topic
        """
        # check "+" wildcards
        wildcard_subscriptions = list(
            map(lambda x: x.replace("+", "[A-z0-9_]+"), self._subscriptions)
        )
        for index, wildcard in enumerate(wildcard_subscriptions):
            if re.search(f"^{wildcard}$", topic):
                return list(self._subscriptions)[index]

        # check "#" wildcards
        wildcard_subscriptions = list(
            map(lambda x: x.replace("#", "[A-z0-9_/]+"), self._subscriptions)
        )
        for index, wildcard in enumerate(wildcard_subscriptions):
            if re.search(f"^{wildcard}$", topic):
                return list(self._subscriptions)[index]

        return topic
