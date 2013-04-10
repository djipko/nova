# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2011 Isaku Yamahata
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Tests for Block Device utility functions.
"""

from nova import block_device
from nova import exception
from nova import test


class BlockDeviceTestCase(test.TestCase):
    def test_properties(self):
        root_device0 = '/dev/sda'
        root_device1 = '/dev/sdb'
        mappings = [{'virtual': 'root',
                     'device': root_device0}]

        properties0 = {'mappings': mappings}
        properties1 = {'mappings': mappings,
                       'root_device_name': root_device1}

        self.assertEqual(block_device.properties_root_device_name({}), None)
        self.assertEqual(
            block_device.properties_root_device_name(properties0),
            root_device0)
        self.assertEqual(
            block_device.properties_root_device_name(properties1),
            root_device1)

    def test_ephemeral(self):
        self.assertFalse(block_device.is_ephemeral('ephemeral'))
        self.assertTrue(block_device.is_ephemeral('ephemeral0'))
        self.assertTrue(block_device.is_ephemeral('ephemeral1'))
        self.assertTrue(block_device.is_ephemeral('ephemeral11'))
        self.assertFalse(block_device.is_ephemeral('root'))
        self.assertFalse(block_device.is_ephemeral('swap'))
        self.assertFalse(block_device.is_ephemeral('/dev/sda1'))

        self.assertEqual(block_device.ephemeral_num('ephemeral0'), 0)
        self.assertEqual(block_device.ephemeral_num('ephemeral1'), 1)
        self.assertEqual(block_device.ephemeral_num('ephemeral11'), 11)

        self.assertFalse(block_device.is_swap_or_ephemeral('ephemeral'))
        self.assertTrue(block_device.is_swap_or_ephemeral('ephemeral0'))
        self.assertTrue(block_device.is_swap_or_ephemeral('ephemeral1'))
        self.assertTrue(block_device.is_swap_or_ephemeral('swap'))
        self.assertFalse(block_device.is_swap_or_ephemeral('root'))
        self.assertFalse(block_device.is_swap_or_ephemeral('/dev/sda1'))

    def test_mappings_prepend_dev(self):
        mapping = [
            {'virtual': 'ami', 'device': '/dev/sda'},
            {'virtual': 'root', 'device': 'sda'},
            {'virtual': 'ephemeral0', 'device': 'sdb'},
            {'virtual': 'swap', 'device': 'sdc'},
            {'virtual': 'ephemeral1', 'device': 'sdd'},
            {'virtual': 'ephemeral2', 'device': 'sde'}]

        expected = [
            {'virtual': 'ami', 'device': '/dev/sda'},
            {'virtual': 'root', 'device': 'sda'},
            {'virtual': 'ephemeral0', 'device': '/dev/sdb'},
            {'virtual': 'swap', 'device': '/dev/sdc'},
            {'virtual': 'ephemeral1', 'device': '/dev/sdd'},
            {'virtual': 'ephemeral2', 'device': '/dev/sde'}]

        prepended = block_device.mappings_prepend_dev(mapping)
        self.assertEqual(prepended.sort(), expected.sort())

    def test_strip_dev(self):
        self.assertEqual(block_device.strip_dev('/dev/sda'), 'sda')
        self.assertEqual(block_device.strip_dev('sda'), 'sda')

    def test_strip_prefix(self):
        self.assertEqual(block_device.strip_prefix('/dev/sda'), 'a')
        self.assertEqual(block_device.strip_prefix('a'), 'a')
        self.assertEqual(block_device.strip_prefix('xvda'), 'a')
        self.assertEqual(block_device.strip_prefix('vda'), 'a')

    def test_volume_in_mapping(self):
        swap = {'device_name': '/dev/sdb',
                'swap_size': 1}
        ephemerals = [{'num': 0,
                       'virtual_name': 'ephemeral0',
                       'device_name': '/dev/sdc1',
                       'size': 1},
                      {'num': 2,
                       'virtual_name': 'ephemeral2',
                       'device_name': '/dev/sdd',
                       'size': 1}]
        block_device_mapping = [{'mount_device': '/dev/sde',
                                 'device_path': 'fake_device'},
                                {'mount_device': '/dev/sdf',
                                 'device_path': 'fake_device'}]
        block_device_info = {
                'root_device_name': '/dev/sda',
                'swap': swap,
                'ephemerals': ephemerals,
                'block_device_mapping': block_device_mapping}

        def _assert_volume_in_mapping(device_name, true_or_false):
            in_mapping = block_device.volume_in_mapping(
                    device_name, block_device_info)
            self.assertEquals(in_mapping, true_or_false)

        _assert_volume_in_mapping('sda', False)
        _assert_volume_in_mapping('sdb', True)
        _assert_volume_in_mapping('sdc1', True)
        _assert_volume_in_mapping('sdd', True)
        _assert_volume_in_mapping('sde', True)
        _assert_volume_in_mapping('sdf', True)
        _assert_volume_in_mapping('sdg', False)
        _assert_volume_in_mapping('sdh1', False)


class BlockDeviceDictTestCase(test.TestCase):
    def test_basic_behaviour(self):
        fake_handlers = {"one": block_device.NoOpHandler("one"),
                         "two": block_device.NoOpHandler("two")}

        self.stubs.Set(block_device, "v1_field_handlers",
                       fake_handlers)

        test_dict = block_device.BlockDeviceDict(
            {"one": 1, "two": 2, "three": 3})

        self.assertIn("one", test_dict)
        self.assertEquals(test_dict["one"], 1)
        self.assertEquals(test_dict.get("two"), 2)
        self.assertRaises(KeyError, lambda: test_dict["three"])

        del test_dict["one"]
        self.assertNotIn("one", test_dict)
        self.assertRaises(KeyError, lambda: test_dict["one"])

        test_dict["one"] = 5
        self.assertEquals(test_dict["one"], 5)

        self.assertEquals(len(test_dict), 2)
        itemlist = list(test_dict.iteritems())
        self.assertIn(("one", 5), itemlist)
        self.assertIn(("two", 2), itemlist)

        def assign(key, val):
            test_dict[key] = val

        self.assertRaises(exception.InvalidBDMField, assign, "three", 6)
        self.assertRaises(exception.InvalidBDMField, assign, "lame", "fake")

        test_dict.update({"one": 1, "two": 5})
        self.assertEquals(test_dict["one"], 1)
        self.assertEquals(test_dict["two"], 5)

    def test_handler_delegation(self):
        class AddHandler(block_device.ItemHandlerBase):
            def get(self, d):
                return d["one"] + d["two"]

            def set(self, d, val):
                d["one"] = d["two"] = val / 2
                super(AddHandler, self).set(d, val)

        fake_handlers = {"three": AddHandler("three")}
        self.stubs.Set(block_device, "v1_field_handlers",
                       fake_handlers)

        test_dict = block_device.BlockDeviceDict({"one": 1, "two": 2})

        self.assertEquals(test_dict["three"], 3)

        test_dict["three"] = 6
        self.assertEquals(test_dict["three"], 6)
        self.assertEquals(test_dict._real_dict["one"], 3)
        self.assertEquals(test_dict._real_dict["two"], 3)

        del test_dict["three"]
        self.assertFalse(test_dict)

        test_dict["three"] = 4
        self.assertEquals(test_dict.get("three"), 4)
        self.assertEquals(test_dict._real_dict["one"], 2)
        self.assertEquals(test_dict._real_dict["two"], 2)

        # Test the situation when we don't have all that is
        # needed to compute the property - we should not see it
        test_dict = block_device.BlockDeviceDict({"one": 1})
        self.assertFalse(test_dict)
