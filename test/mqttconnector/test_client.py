import json
import unittest
from mqttconnector.client import MQTTClient
from unittest.mock import Mock


class Test(unittest.TestCase):

    def setUp(self):
        self.mqtt_client = MQTTClient(host='localhost', port=8883, user='test1', password='test2', tls_cert="")

    def test_get_wildchard_topics_single_plus(self):
        callback = Mock()

        WILDCARD1 = 'mh/+/ppmp'

        self.mqtt_client.subscribe(WILDCARD1, callback)

        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test/ppmp'), WILDCARD1)
        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test_2/ppmp'), WILDCARD1)

        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/ppmp'), WILDCARD1)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('xy/test/test2/ppmp'), WILDCARD1)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/ppmp2'), WILDCARD1)

    def test_get_wildchard_topics_single_hash(self):
        callback = Mock()

        WILDCARD1 = '#/ppmp'

        self.mqtt_client.subscribe(WILDCARD1, callback)

        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test/ppmp'), WILDCARD1)
        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test_2/ppmp'), WILDCARD1)
        self.assertEqual(self.mqtt_client.get_wildchard_topics('xy/test_2/ppmp'), WILDCARD1)

        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/ppmp2'), WILDCARD1)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('xy/test/test2/xy'), WILDCARD1)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/ppmp_'), WILDCARD1)


    def test_get_wildchard_topics_multiple_plus(self):
        callback = Mock()

        WILDCARD1 = 'mh/+/ppmp'
        WILDCARD2 = 'mh/+/cmd'
        WILDCARD3 = 'mh/+/cmd/res'

        self.mqtt_client.subscribe(WILDCARD1, callback)
        self.mqtt_client.subscribe(WILDCARD2, callback)
        self.mqtt_client.subscribe(WILDCARD3, callback)

        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test/ppmp'), WILDCARD1)
        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test_2/ppmp'), WILDCARD1)

        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test/cmd'), WILDCARD2)
        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test_2/cmd'), WILDCARD2)

        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test/cmd/res'), WILDCARD3)
        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test_2/cmd/res'), WILDCARD3)

        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/ppmp'), WILDCARD1)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('xy/test/test2/ppmp'), WILDCARD1)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/ppmp2'), WILDCARD1)

        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/cmd'), WILDCARD2)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('xy/test/test2/cmd'), WILDCARD2)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/cmd2'), WILDCARD2)

        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/cmd/res'), WILDCARD3)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('xy/test/test2/cmd/res'), WILDCARD3)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/cmd/res2'), WILDCARD3)

    def test_get_wildchard_topics_multiple_hash(self):
        callback = Mock()

        WILDCARD1 = 'mh/#/ppmp'
        WILDCARD2 = '#/cmd'
        WILDCARD3 = 'mh/#/cmd/res'

        self.mqtt_client.subscribe(WILDCARD1, callback)
        self.mqtt_client.subscribe(WILDCARD2, callback)
        self.mqtt_client.subscribe(WILDCARD3, callback)

        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test/ppmp'), WILDCARD1)
        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test_2/ppmp'), WILDCARD1)
        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test_2/test3/ppmp'), WILDCARD1)

        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test/cmd'), WILDCARD2)
        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test_2/cmd'), WILDCARD2)
        self.assertEqual(self.mqtt_client.get_wildchard_topics('xy/test_2/test3/cmd'), WILDCARD2)

        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test/cmd/res'), WILDCARD3)
        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test_2/cmd/res'), WILDCARD3)
        self.assertEqual(self.mqtt_client.get_wildchard_topics('mh/test_2/test3/cmd/res'), WILDCARD3)

        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/ppm'), WILDCARD1)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('xy/test/test2/ppmp'), WILDCARD1)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/ppmp2'), WILDCARD1)

        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/cmd/test'), WILDCARD2)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('xy/test/test_2/cm'), WILDCARD2)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/cmd2'), WILDCARD2)

        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/cmd/res/test'), WILDCARD3)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('xy/test/test2/cmd/res'), WILDCARD3)
        self.assertNotEqual(self.mqtt_client.get_wildchard_topics('mh/test/test2/cmd/res2'), WILDCARD3)


if __name__ == '__main__':
    unittest.main()
