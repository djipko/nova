
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

import uuid

import mock

from nova import exception
from nova import objects
from nova.tests import matchers
from nova.tests.objects import test_objects
from nova.virt import hardware

fake_cpu_pinning = hardware.VirtInstanceCPUPinning(
                cells=[hardware.VirtInstanceCPUPinningCell(
                           set([0, 1]), id=0, pinning={0: 4, 1: 1},
                           topology=hardware.VirtCPUTopology(1, 1, 2)),
                       hardware.VirtInstanceCPUPinningCell(
                           set([2, 3]), id=1, pinning={2: 14, 3: 10},
                           topology=hardware.VirtCPUTopology(1, 1, 2))])

fake_db_extra = {
    'created_at': None,
    'updated_at': None,
    'deleted_at': None,
    'deleted': 0,
    'id': 1,
    'instance_uuid': str(uuid.uuid4()),
    'cpu_pinning': fake_cpu_pinning.to_json()
    }


class _TestInstanceCPUPinning(object):
    @mock.patch('nova.db.instance_extra_update_by_uuid')
    def test_save(self, mock_update):
        topo_obj = objects.InstanceCPUPinning.obj_from_topology(
               fake_cpu_pinning)
        topo_obj.instance_uuid = fake_db_extra['instance_uuid']
        topo_obj.save(self.context)
        mock_update.assert_called_once_with(
                self.context, topo_obj.instance_uuid,
                {'cpu_pinning': fake_db_extra['cpu_pinning']})

    @mock.patch('nova.db.instance_extra_get_by_instance_uuid')
    def test_get_by_instance_uuid(self, mock_get):
        mock_get.return_value = fake_db_extra
        cpu_pinning = objects.InstanceCPUPinning.get_by_instance_uuid(
            self.context, 'fake_uuid')
        self.assertEqual(fake_db_extra['instance_uuid'],
                         cpu_pinning.instance_uuid)
        for obj_cell, topo_cell, hydrated_obj_cell in zip(
                cpu_pinning.cells, fake_cpu_pinning.cells,
                cpu_pinning.topology_from_obj().cells):
            self.assertIsInstance(obj_cell, objects.InstanceCPUPinningCell)
            self.assertEqual(topo_cell.id, obj_cell.id)
            for attr in ('sockets', 'cores', 'threads'):
                self.assertEqual(getattr(topo_cell.topology, attr),
                                 getattr(obj_cell.topology, attr))
            self.assertThat(topo_cell.pinning, matchers.DictMatches(
                                hydrated_obj_cell.pinning))

    @mock.patch('nova.db.instance_extra_get_by_instance_uuid')
    def test_get_by_instance_uuid_missing(self, mock_get):
        mock_get.return_value = None
        self.assertRaises(
            exception.InstanceCPUPinningNotFound,
            objects.InstanceCPUPinning.get_by_instance_uuid,
            self.context, 'fake_uuid')

    @mock.patch('nova.db.instance_extra_get_by_instance_uuid')
    def test_get_by_instance_uuid_missing_data(self, mock_get):
        mock_get.return_value = fake_db_extra.copy()
        mock_get.return_value['cpu_pinning'] = None
        self.assertRaises(
            exception.InstanceCPUPinningNotFound,
            objects.InstanceCPUPinning.get_by_instance_uuid,
            self.context, 'fake_uuid')


class TestInstanceCPUPinning(test_objects._LocalTest,
                               _TestInstanceCPUPinning):
    pass


class TestInstanceCPUPinningRemote(test_objects._RemoteTest,
                                     _TestInstanceCPUPinning):
    pass
