

import datetime
import json
from pickle import NONE
from queue import Queue
from threading import Lock, RLock, Thread
import threading
from time import sleep
import uuid

from mqttconnector.client import MQTTClient
from ppmpmessage.v3.util import machine_message_generator
from ppmpmessage.v3.device_state import DeviceState
import logging


class MQTTServiceDeviceSendPayload(object):
    def __init__(self, topic: str = "", payload: dict = {}):

        self.m_topic = topic
        self.m_payload = payload

    def getTopic(self) -> str:
        return self.m_topic

    def getPayload(self) -> dict:
        return self.m_payload


class MQTTServiceDeviceClient(object):

    m_deviceConfig: str
    m_deviceKey: str

    m_ConsumerMQTTQUEUE: Queue
    m_ProducerMQTTQUEUE: Queue
    m_RecvChanneldata: dict()
    m_DeviceServiceMetaData: dict()
    m_Lock: RLock
    m_RecLock: RLock

    m_logger: logging.Logger
    MQTT_DEVICENAME: str
    m_MQTT_HOST: str
    m_MQTT_PORT: str
    m_MQTT_USERNAME: str
    m_MQTT_PASSWORD: str
    m_MQTT_TLS_CERT: str
    m_MQTT_DEVICENAME: str
    m_DeviceServiceNames: list
    m_LoggingLevel: int
    m_Devicestate: dict
    m_generalConfig: dict
    m_notifyInfoStateFunction: list

    def __init__(self, deviceKey: str = "", deviceConfig: dict = {}):
        self.m_deviceConfig = deviceConfig
        self.m_deviceKey = deviceKey

        self.m_ConsumerMQTTQUEUE = Queue(maxsize=0)
        self.m_ProducerMQTTQUEUE = Queue(maxsize=0)
        self.m_RecvChanneldata = dict()
        self.m_DeviceServiceMetaData = dict()
        self.m_Lock = RLock()
        self.m_RecLock = RLock()

        self.m_logger = logging.getLogger('root')
        self.MQTT_DEVICENAME = ""
        self.m_MQTT_HOST = ""
        self.m_MQTT_PORT = ""
        self.m_MQTT_USERNAME = ""
        self.m_MQTT_PASSWORD = ""
        self.m_MQTT_TLS_CERT = ""
        self.m_MQTT_DEVICENAME = ""
        self.m_DeviceServiceNames = ["PLC1"]
        self.m_LoggingLevel = 0
        self.m_Devicestate = dict()
        self.m_generalConfig = dict()

        if "mqtt.host" in self.m_deviceConfig:
            self.m_MQTT_HOST = self.m_deviceConfig["mqtt.host"]

        if "mqtt.port" in self.m_deviceConfig:
            self.m_MQTT_PORT = self.m_deviceConfig["mqtt.port"]

        if "mqtt.username" in self.m_deviceConfig:
            self.m_MQTT_USERNAME = self.m_deviceConfig["mqtt.username"]

        if "mqtt.password" in self.m_deviceConfig:
            self.m_MQTT_PASSWORD = self.m_deviceConfig["mqtt.password"]

        if "mqtt.tls_cert" in self.m_deviceConfig:
            self.m_MQTT_TLS_CERT = self.m_deviceConfig["mqtt.tls_cert"]

        if "mqtt.mqtt_devicename" in self.m_deviceConfig:
            self.m_MQTT_DEVICENAME = self.m_deviceConfig["mqtt.mqtt_devicename"]

        if "DeviceServiceNames" in self.m_deviceConfig:
            self.m_DeviceServiceNames = self.m_deviceConfig["DeviceServiceNames"]

        if "LoggingLevel" in self.m_deviceConfig:
            self.m_LoggingLevel = self.m_deviceConfig["LoggingLevel"]

        if self.m_MQTT_DEVICENAME == "":
            self.m_MQTT_DEVICENAME = None

        self.m_mqttclient = MQTTClient(host=self.m_MQTT_HOST, port=self.m_MQTT_PORT,
                                       user=self.m_MQTT_USERNAME, password=self.m_MQTT_PASSWORD, tls_cert=self.m_MQTT_TLS_CERT)

        self.m_mqttConsumerThread = None
        self.m_mqttProducerThread = None
        self.m_isInitialized = False
        self.m_writenotifytopic = {}
        self.m_notifyEvents = {}

        self.m_notifyEventsCounter = 0
        self.m_InterfaceType = ""
        self.m_InterfaceVersion = ""
        self.m_MetadataVersion = ""
        self.m_infodeviceTopic = dict()
        self.m_notifyInfoStateFunction = []

        self.initDeviceStates()

    def AddnotityInfoStateFunction(self, infostatefunct) -> None:
        self.m_Lock.acquire()
        remaining = [
            notifyInfoStateFunction for notifyInfoStateFunction in self.m_notifyInfoStateFunction if notifyInfoStateFunction == infostatefunct]

        if len(remaining) == 0:
            self.m_notifyInfoStateFunction.append(infostatefunct)
        self.m_Lock.release()

    def removeNotifyInfoStateFunction(self, infostatefunct) -> None:

        self.m_Lock.acquire()
        remaining = [
            notifyInfoStateFunction for notifyInfoStateFunction in self.m_notifyInfoStateFunction if notifyInfoStateFunction != infostatefunct]

        self.m_notifyInfoStateFunction = remaining
        self.m_Lock.release()

    def removeallNotifyInfoStateFunctions(self) -> None:

        self.m_Lock.acquire()
        self.m_notifyInfoStateFunction = []
        self.m_Lock.release()

    def getRecvChanneldata(self) -> dict:
        self.m_RecLock.acquire()
        ret = self.m_RecvChanneldata
        self.m_RecLock.release()
        return ret

    def getDeviceServiceMetaData(self) -> dict:
        return self.m_DeviceServiceMetaData

    def getMQTTClient(self) -> MQTTClient:
        return self.m_mqttclient

    def doChannelInit(self) -> bool:
        # Channel-Data neu aufsetzen
        self.m_RecLock.acquire()
        self.m_RecvChanneldata = {}
        self.m_RecLock.release()
        return True

    def on_disconnect(self, client, userdata, rc):
        self.m_logger.info(f"MQTTServiceDevice.on_disconnect")
        self.m_isInitialized = False
        
        self.initDeviceStates()

    def initDeviceStates(self):
        self.m_Lock.acquire()
        for deviceServiceName in self.m_DeviceServiceNames:
            self.m_Devicestate[deviceServiceName] = DeviceState.UNKNOWN
            self.m_infodeviceTopic[deviceServiceName] = ""

        self.m_Lock.release()


    
    def on_connect(self, client, userdata, flags, rc):

        self.initDeviceStates()
        self.unsubscribeallTopics()
        self.m_mqttclient.subscribe(
            "mh/DeviceServices/+", self.deviceServicesConsumer)
        self.m_logger.info(f"MQTTServiceDevice.on_connect")
        self.m_isInitialized = True

    def doInit(self):

        # self.m_mqttclient.subscribe(
        #     "mh/DeviceServices/+", self.deviceServicesConsumer)

        self.m_mqttclient.connect(
            forever=False, connectHandler=self.on_connect, disconnectHandler=self.on_disconnect)

        self.m_mqttConsumerThread = Thread(target=self.mqttConsumerThread)
        self.m_mqttConsumerThread.start()

        self.m_mqttProducerThread = Thread(target=self.mqttProducerThread)
        self.m_mqttProducerThread.start()

        return True

    def unsubscribeallTopics(self):
        self.m_Lock.acquire()
        self.m_mqttclient.unsubscribeallTopics()
        self.m_Lock.release()
        
    def doExit(self):

        self.m_mqttclient.unsubscribeallTopics()
        cancel = MQTTServiceDeviceSendPayload("", {'cancelThread': True})
        if self.m_mqttConsumerThread != None:

            self.m_ConsumerMQTTQUEUE.put(cancel)

            # self.m_ConsumerMQTTQUEUE.put(
            #     json.dumps({'cancelThread': True}))
            self.m_mqttConsumerThread.join()

        if self.m_mqttProducerThread != None:
            self.m_ProducerMQTTQUEUE.put(cancel)
            # self.mqttProduceMessage({"cancel": True})
            self.m_mqttProducerThread.join()

        if self.m_isInitialized:
            self.m_mqttclient._client.disconnect()
        
        self.removeallNotifyInfoStateFunctions()
        self.m_isInitialized = False
        return True

    def getMetaDataByKey(self, key: str) -> dict:
        self.m_Lock.acquire()
        if key in self.m_DeviceServiceMetaData:
            ret = self.m_DeviceServiceMetaData[key]
            self.m_Lock.release()
            return ret

        self.m_Lock.release()
        return {}

    def getCompleteMetaData(self) -> dict:
        return self.m_DeviceServiceMetaData

    def subscribeTopicByKey(self, key: str) -> bool:
        try:
            dook = False
            self.m_Lock.acquire()
            if key in self.m_DeviceServiceMetaData:
                metadata = self.m_DeviceServiceMetaData[key]
                if "readtopic" in metadata:
                    readtopic = metadata["readtopic"]
                    if not readtopic in self.m_mqttclient.getSubscriptions():
                        self.m_mqttclient.subscribe(
                            readtopic, self.mqttConsumer)
                        if self.m_LoggingLevel > 0:
                            self.m_logger.info(
                                f'MQTTServiceDevice.subscribeTopicByKey: "key = {key}"')
                    dook = True

            self.m_Lock.release()
            return dook
        except Exception as e:
            self.m_Lock.release()
            self.m_logger.error(
                f'MQTTServiceDevice.subscribeTopicByKey :{e}')
            return False

    def subscribeWriteTopicByKey(self, key: str) -> bool:
        try:
            dook = False
            self.m_Lock.acquire()
            if key in self.m_DeviceServiceMetaData:
                metadata = self.m_DeviceServiceMetaData[key]
                if "writetopic" in metadata:
                    writetopic = metadata["writetopic"]
                    if writetopic != "":
                        if not writetopic in self.m_mqttclient.getSubscriptions():
                            self.m_mqttclient.subscribe(
                                writetopic, self.mqttConsumer)
                            if self.m_LoggingLevel > 0:
                                self.m_logger.info(
                                    f'MQTTServiceDevice.subscribeWriteTopicByKey: "key = {key}"')
                        dook = True

            self.m_Lock.release()
            return dook

        except Exception as e:
            self.m_Lock.release()
            self.m_logger.error(
                f'MQTTServiceDevice.subscribeWriteTopicByKey :{e}')
            return False

    def getGeneralConfigByDevKey(self, devkey: str) -> dict:
        '''get GeneralConfig {
                            "InterfaceType":"PCComm"
                            "InterfaceVersion":"V1.0"
                           "MetadataVersion":"V1.0"
        }

        Args:
            devkey (str): _description_

        Returns:
            dict: _description_
        '''
        self.m_Lock.acquire()
        if devkey in self.m_generalConfig:
            ret = self.m_generalConfig[devkey]
            self.m_Lock.release()
            return ret

        self.m_Lock.release()
        return {}

    def getInfoDeviceTopic(self, devkey: str)-> str:
        self.m_Lock.acquire()
        if devkey in self.m_infodeviceTopic:
            infodevicetopic = self.m_infodeviceTopic[devkey]
            self.m_Lock.release()
            return infodevicetopic

        self.m_Lock.release()
    
        return ""
    
    def getdeviceStateByDevKey(self, devkey: str) -> DeviceState:
        '''Get State by DeviceKey

        Args:
            devkey (str): device Key 

        Returns:
            DeviceState: _description_
        '''
        self.m_Lock.acquire()
        if devkey in self.m_Devicestate:
            state = self.m_Devicestate[devkey]
            self.m_Lock.release()
            return state

        self.m_Lock.release()
        return DeviceState.UNKNOWN

    def getdeviceByKey(self, key: str) -> str:
        '''_summary_

        Args:
            key (str): _description_

        Returns:
            str: _description_
        '''
        try:
            splittet = key.split('.')
            if len(splittet) > 0:
                device = splittet[0]
                splitteddev = device.split('@')
                if len(splitteddev) > 1:
                    return splitteddev[1]
                else:
                    return splitteddev[0]

            return ""
        except Exception as e:
            self.m_logger.error(
                f'MQTTServiceDevice.getdeviceByKey :{e}')
            return ""

    def getdeviceStateByKey(self, key: str) -> DeviceState:
        if key in self.m_DeviceServiceMetaData:
            value = self.m_DeviceServiceMetaData[key]
            if "devicekey" in value:
                devicekey = value["devicekey"]
                return self.getdeviceStateByDevKey(devicekey)

        return DeviceState.UNKNOWN

    def getMetaValueItemByKey(self, key: str) -> dict:
        try:
            self.m_Lock.acquire()
            if key in self.m_DeviceServiceMetaData:
                value = self.m_DeviceServiceMetaData[key]
                self.m_Lock.release()
                return value
            else:
                self.m_Lock.release()
                return None
        except Exception as e:
            self.m_Lock.release()
            self.m_logger.error(
                f'MQTTServiceDevice.getMetaValueItemByKey :{e}')
            return None

    def getValueItemByKey(self, key: str = "") -> dict:
        try:
            self.m_RecLock.acquire()
            if key in self.m_RecvChanneldata:
                value = self.m_RecvChanneldata[key]
                self.m_RecLock.release()

                return value
            else:
                self.m_RecLock.release()
                return None
        except Exception as e:
            self.m_RecLock.release()
            self.m_logger.error(
                f'MQTTServiceDevice.getValueItemByKey :{e}')
            return None

    def getValueByKey(self, key: str) -> any:
        try:
            self.m_RecLock.acquire()
            if key in self.m_RecvChanneldata:
                value = self.m_RecvChanneldata[key]
                self.m_RecLock.release()
                return value["value"]
            else:
                self.m_RecLock.release()
                return None
        except Exception as e:
            self.m_RecLock.release()
            self.m_logger.error(
                f'MQTTServiceDevice.getValueItemByKey :{e}')
            return None

    def getActTime(self):
        try:
            presentDate = datetime.datetime.now(
                datetime.timezone.utc).astimezone()
            posix_timestamp = datetime.datetime.timestamp(presentDate)*1000

            return posix_timestamp

        except Exception as e:
            self.m_logger.error(
                f'MQTTServiceDevice.getActTime :{e}')
            return 0

    def getNotifyEvent(self) -> threading.Event():

        if self.m_notifyEventsCounter in self.m_notifyEvents:
            notifyEvent = self.m_notifyEvents[self.m_notifyEventsCounter]
        else:
            notifyEvent = threading.Event()
            self.m_notifyEvents[self.m_notifyEventsCounter] = notifyEvent

        if self.m_notifyEventsCounter < 20:
            self.m_notifyEventsCounter = self.m_notifyEventsCounter + 1
        else:
            self.m_notifyEventsCounter = 0

        return notifyEvent

    def getwriteValueErrorByKey(self, key: str = "") -> str:
        metadata = self.getMetaDataByKey(key)
        if len(metadata.keys()) > 0:
            return(metadata["errormsg"])

        return ("")

    def writeValueByKey(self, key: str = "", value: any = 1, withNotify: bool = False) -> bool:
        try:

            metadata = self.getMetaDataByKey(key)
            device = self.getdeviceByKey(key)

            if len(metadata.keys()) > 0:
                # metadata = self.m_DeviceServiceMetaData[key]

                if "writetopic" in metadata:

                    writetopic = metadata["writetopic"]

                    notifyEvent = None
                    if writetopic != "":
                        self.m_Lock.acquire()
                        timestamp = self.getActTime()
                        _uuid = uuid.uuid4().int
                        if withNotify:
                            payload = {
                                "metakey": key,
                                "identifier": metadata["identifier"],
                                "posix_timestamp": timestamp,
                                "value": value,
                                "notifyid": _uuid,
                                "notifytopic": self.m_writenotifytopic[device]
                            }
                            # get notify event for later setting in MQTT Consumer Procedure
                            notifyEvent = self.getNotifyEvent()
                            notifyEvent.clear()
                            metadata["notifyevent"] = notifyEvent
                            metadata["notifyid"] = _uuid
                        else:
                            payload = {
                                "metakey": key,
                                "identifier": metadata["identifier"],
                                "posix_timestamp": timestamp,
                                "value": value
                            }
                        metadata["setvalue"] = value
                        metadata["posix_timestamp"] = timestamp
                        metadata["result"] = -1
                        metadata["errormsg"] = "response-timeout"
                        self.m_Lock.release()

                        payload: MQTTServiceDeviceSendPayload = MQTTServiceDeviceSendPayload(
                            writetopic, payload)

                        self.m_ProducerMQTTQUEUE.put(payload)
                        # waiting for notify-Event, setting in MQTT Consumer Procedure

                        if notifyEvent != None:
                            flag = notifyEvent.wait(12)
                            if not flag:
                                return False
                            else:
                                return metadata["result"] != -1

                        return True

            return False
        except Exception as e:

            self.m_logger.error(
                f'MQTTServiceDevice.writeValueByKey ({key}, {value}):{e}')
            return False

# device specific

    def devicewriteNotifyConsumer(self, topic: str, payload: any):
        '''deviceServicesMetaDataConsumer

        Args:
            payload (_type_): _description_
        '''
        try:
            self.m_Lock.acquire()
            action = json.loads(payload)
            if isinstance(action, (dict)):
                if "metakey" in action and "notifyid" in action:
                    # identifier = action["identifier"]
                    metakey = action["metakey"]

                    if metakey in self.m_DeviceServiceMetaData:
                        notifyid = action["notifyid"]
                        metadata = self.m_DeviceServiceMetaData[metakey]
                        if "value" in action:
                            value = action["value"]
                            metadata["setvalue"] = value
                        if "result" in action:
                            metadata["result"] = action["result"]
                        if "errormsg" in action:
                            metadata["errormsg"] = action["errormsg"]

                        if "notifyevent" in metadata:
                            notifyidevent = metadata["notifyevent"]
                        if "notifyid" in metadata:
                            metanotifyid = metadata["notifyid"]
                            if metanotifyid == notifyid:
                                notifyidevent.set()

            self.m_Lock.release()
        except Exception as e:
            self.m_Lock.release()
            self.m_logger.error(
                f'MQTTServiceDevice.devicewriteNotifyConsumer ():{e}')

    def infoTopicConsumer(self, topic: str, payload: any):
        '''_summary_

        Args:
            payload (any): _description_
        '''
        try:
            hostname = ""
            self.m_Lock.acquire()
            action = json.loads(payload)
            state = DeviceState.UNKNOWN

            if "device" in action:
                device = action["device"]

                if "state" in device:
                    state = device["state"]

                if "additionalData" in device:
                    additionalData = device["additionalData"]
                    if "hostname" in additionalData:
                        hostname = additionalData["hostname"]
                        self.m_Devicestate[hostname] = state

                    if "generalconfig" in additionalData:

                        generalconfig = additionalData["generalconfig"]
                        if hostname != "":
                            self.m_generalConfig[hostname] = generalconfig

            for notifyInfoStateFunction in self.m_notifyInfoStateFunction:
                notifyInfoStateFunction(hostname = hostname , infostate = self.m_Devicestate[hostname])

            self.m_Lock.release()
        except Exception as e:
            self.m_Lock.release()
            self.m_logger.error(
                f'MQTTServiceDevice.infoTopicConsumer ():{e}')

    def deviceServicesMetaDataConsumer(self, topic: str,  payload: any):
        '''deviceServicesMetaDataConsumer

        Args:
            payload (_type_): _description_
        '''
        try:
            self.m_Lock.acquire()
            action = json.loads(payload)

            self.m_DeviceServiceMetaData.update(action)
            # if isinstance(action, (dict)):
            #     if "metadata" in action:
            #         metadata = action["metadata"]
            #         self.m_DeviceServiceMetaData.update(metadata)

            self.m_Lock.release()
        except Exception as e:
            self.m_Lock.release()
            self.m_logger.error(
                f'MQTTServiceDevice.deviceServicesMetaDataConsumer ():{e}')

    def isKeyinDeviceNames(self, key: str) -> bool:
        '''_summary_

        Args:
            key (str): _description_

        Returns:
            bool: _description_
        '''
        self.m_Lock.acquire()
        for item in self.m_DeviceServiceNames:
            if item == key:
                self.m_Lock.release()
                return True

        self.m_Lock.release()
        return False

    def deviceServicesConsumer(self, topic: str, payload: any):
        '''deviceServicesConsumer

        Args:
            payload (_type_): _description_
        '''
        try:
            if payload==b'':
                return
            
            self.m_Lock.acquire()
            action = json.loads(payload)

            if isinstance(action, (dict)):
                for key, devices in action.items():
                    if self.isKeyinDeviceNames(key):
                        # more than one device
                        if "topic" in devices:
                            # self.m_DeviceServiceMetaData[key] = {}

                            devicetopic = f'{devices["topic"]}'
                            metadatatopic = f'{devicetopic}/metadata'
                            self.m_writenotifytopic[key] = f"{devicetopic}/notify"
                            infotopic = f'{devicetopic}/info'
                            self.m_infodeviceTopic[key] =infotopic
                            self.m_mqttclient.subscribe(
                                metadatatopic, self.deviceServicesMetaDataConsumer)
                            self.m_mqttclient.subscribe(
                                self.m_writenotifytopic[key], self.devicewriteNotifyConsumer)
                            self.m_mqttclient.subscribe(
                                infotopic, self.infoTopicConsumer)
                        if self.m_LoggingLevel > 0:
                            self.m_logger.info(
                                f'MQTTServiceDevice.deviceServicesConsumer: "devicekey: {key} -> topic = {devicetopic} "')

            self.m_Lock.release()
        except Exception as e:
            self.m_Lock.release()
            self.m_logger.error(
                f'MQTTServiceDevice.deviceServicesMetaDataConsumer error for ({topic}):{e}')

    def doProcessMQTTPayload(self, topic: str, jsonpayload: any):
        '''doProcessMQTTPayload

        Args:
            jsonpayload (_type_): Processing incoming MQTT Payload
        '''
        try:
            qsize = self.m_ConsumerMQTTQUEUE.qsize()
            self.m_RecLock.acquire()
            if 'metakey' in jsonpayload:
                metakey = jsonpayload['metakey']
                self.m_RecvChanneldata[metakey] = jsonpayload

                if self.m_LoggingLevel > 0:
                    self.m_logger.info(
                        f'recieve: "key = {metakey} : value ({jsonpayload["posix_timestamp"]}): {jsonpayload["value"]} -> qsize({qsize})"')
            self.m_RecLock.release()
        except Exception as e:
            self.m_RecLock.release()
            self.m_logger.error(
                f'MQTTServiceDevice.doProcessMQTTPayload ():{e}')

    def mqttConsumerThread(self):

        while True:

            try:
                data: MQTTServiceDeviceSendPayload = self.m_ConsumerMQTTQUEUE.get()

                if 'cancelThread' in data.getPayload():
                    break
                else:
                    # action = json.loads(data.getPayload())
                    self.doProcessMQTTPayload(
                        data.getTopic(), data.getPayload())

            except Exception as e:
                self.m_logger.error(
                    f'MQTTServiceDevice.mqttConsumerThread :{e}')

    def mqttProducerThread(self):
        """Sends the MQTT payload accumalated in the queue
        """
        while True:

            try:
                data: MQTTServiceDeviceSendPayload = self.m_ProducerMQTTQUEUE.get()

                if ("cancelThread" in data.getPayload()):
                    break
                else:
                    # print(payload)
                    payloadstr: str = json.dumps(data.getPayload())
                    # payload = json.dumps(data)
                    self.m_mqttclient.publish(
                        data.getTopic(), payloadstr, retain=False)
                    if self.m_LoggingLevel > 0:
                        self.m_logger.info(
                            f'publish: "topic = {data.getTopic()} :  {data.getPayload()["value"]}"')

            except (Exception) as error:
                self.m_logger.info(
                    f'MQTTServiceDevice.mqttProducerThread!{error}')
                self.m_logger.error(
                    f'MQTTServiceDevice.mqttProducerThread!{error}')

    def mqttConsumer(self, topic: str, payload: any):
        """ Subscribe to MQTT changes and write payload into queue

        Arguments:
            payload {obj} -- JSON payload in PPMP format
        """
        try:
            action = json.loads(payload)

            payload_: MQTTServiceDeviceSendPayload = MQTTServiceDeviceSendPayload(
                topic, action)
            self.m_ConsumerMQTTQUEUE.put(payload_)
        # logger.info(f'MQTT Queue size: {QUEUE_MQTT.qsize()}')
        except (Exception) as error:
            self.m_logger.info(
                f'MQTTServiceDevice.mqttConsumer!{error}')
            self.m_logger.error(
                f'MQTTServiceDevice.mqttConsumer!{error}')

    def mqttProduceMessage(self, payloads: list):
        """ Subscribe to MQTT changes and write payload into queue

        Arguments:
            payload {obj} -- JSON payload in PPMP format
        """
        for payload in payloads:
            self.m_ProducerMQTTQUEUE.put(payload)
        # logger.info(f'MQTT Queue size: {QUEUE_MQTT.qsize()}')
