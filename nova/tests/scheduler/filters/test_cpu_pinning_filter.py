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

import mock
from oslo.serialization import jsonutils
import six

from nova import objects
from nova.objects import base as obj_base
from nova.scheduler.filters import cpu_pinning_filter
from nova import test
from nova.tests import fake_instance
from nova.tests.scheduler import fakes
from nova.virt import hardware


class TestNUMATopologyFilter(test.NoDBTestCase):

    def setUp(self):
        super(TestNUMATopologyFilter, self).setUp()
        self.filt_cls = cpu_pinning_filter.CPUPinningFilter()
        self.host_pin = hardware.VirtHostCPUPinning(
                cells=[hardware.VirtHostCPUPinningCell(0, set([0, 1, 2]))]
                ).to_json()

    def test_cpu_pinning_filter_pass(self):
        inst_pin = hardware.VirtInstanceCPUPinning(
                cells=[hardware.VirtInstanceCPUPinningCell(set([0, 1, 2]))])

        instance = fake_instance.fake_instance_obj(mock.sentinel.ctx)
        instance.cpu_pinning = objects.InstanceCPUPinning.obj_from_topology(
                inst_pin)
        filter_properties = {
            'request_spec': {
                'instance_properties': jsonutils.to_primitive(
                    obj_base.obj_to_primitive(instance))}}
        host = fakes.FakeHostState('host1', 'node1',
                                   {'cpu_pinning': self.host_pin})
        self.assertTrue(self.filt_cls.host_passes(host, filter_properties))
        self.assertIsInstance(
                filter_properties
                    ['request_spec']['instance_properties']['cpu_pinning'],
                six.string_types)

    def test_cpu_pinning_filter_fail(self):
        inst_pin = hardware.VirtInstanceCPUPinning(
                cells=[hardware.VirtInstanceCPUPinningCell(set([0, 1, 2, 4]))])

        instance = fake_instance.fake_instance_obj(mock.sentinel.ctx)
        instance.cpu_pinning = objects.InstanceCPUPinning.obj_from_topology(
                inst_pin)
        filter_properties = {
            'request_spec': {
                'instance_properties': jsonutils.to_primitive(
                    obj_base.obj_to_primitive(instance))}}
        host = fakes.FakeHostState('host1', 'node1',
                                   {'cpu_pinning': self.host_pin})
        self.assertFalse(self.filt_cls.host_passes(host, filter_properties))

    def test_cpu_pinning_filter_no_pinning_pass(self):
        instance = fake_instance.fake_instance_obj(mock.sentinel.ctx)
        instance.cpu_pinning = None
        filter_properties = {
            'request_spec': {
                'instance_properties': jsonutils.to_primitive(
                    obj_base.obj_to_primitive(instance))}}
        host = fakes.FakeHostState('host1', 'node1',
                                   {'cpu_pinning': self.host_pin})
        self.assertTrue(self.filt_cls.host_passes(host, filter_properties))
