#    Copyright 2014 Red Hat Inc.
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

import six

from nova import db
from nova import exception
from nova.objects import base
from nova.objects import fields
from nova.virt import hardware


class InstanceCPUTopology(base.NovaObject):
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'sockets': fields.IntegerField(),
        'cores': fields.IntegerField(),
        'threads': fields.IntegerField(),
        }

    def topology_from_obj(self):
        return hardware.VirtCPUTopology(self.sockets or 1, self.cores or 1,
                                        self.threads or 1)

    @classmethod
    def obj_from_topology(cls, topology):
        if not isinstance(topology, hardware.VirtCPUTopology):
            raise exception.ObjectActionError(action='obj_from_topology',
                                              reason='invalid topology class')
        return cls(sockets=topology.sockets, cores=topology.cores,
                   threads=topology.threads)


class InstanceCPUPinningCell(base.NovaObject):
    # Version 1.0: Initial version
    VERSION = '1.0'
    fields = {
        'id': fields.IntegerField(nullable=True),
        'topology': fields.ObjectField('InstanceCPUTopology', nullable=True),
        'pinning': fields.DictOfStringsField(),
    }


class InstanceCPUPinning(base.NovaObject):
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'instance_uuid': fields.UUIDField(),
        'cells': fields.ListOfObjectsField('InstanceCPUPinningCell'),
        }

    @classmethod
    def obj_from_topology(cls, cpu_pinning):
        if not isinstance(cpu_pinning, hardware.VirtInstanceCPUPinning):
            raise exception.ObjectActionError(action='obj_from_topology',
                                              reason='invalid pinning class')
        if cpu_pinning:
            cells = []
            for cell in cpu_pinning.cells:
                if cell.topology:
                    cpu_topology = InstanceCPUTopology.obj_from_topology(
                            cell.topology)
                else:
                    cpu_topology = None
                # NOTE(ndipanov): We need to make sure pinning is a dict of
                # strings because of the DictOfStringsField field type
                cell = InstanceCPUPinningCell(
                        id=cell.id, cpuset=cell.cpuset, topology=cpu_topology,
                        pinning=dict(map(six.text_type, t)
                                      for t in six.iteritems(cell.pinning)))
                cells.append(cell)
            return cls(cells=cells)

    def topology_from_obj(self):
        cells = []
        for cell in self.cells:
            pinning = None
            cpu_topology = None
            cpuset = set()
            if cell.pinning:
                pinning = {}
                for cpu, pin in six.iteritems(cell.pinning):
                    # NOTE(ndipanov): Keys are always ints, values can be an
                    # int or 'None'
                    try:
                        pinning[int(cpu)] = int(pin)
                    except ValueError:
                        pinning[int(cpu)] = None
                cpuset = set(pinning.keys())
            if cell.topology:
                cpu_topology = cell.topology.topology_from_obj()

            cell = hardware.VirtInstanceCPUPinningCell(
                    cpuset, id=cell.id, pinning=pinning,
                    topology=cpu_topology)
            cells.append(cell)

        return hardware.VirtInstanceCPUPinning(cells=cells)

    @base.remotable
    def save(self, context):
        topology = self.topology_from_obj()
        if not topology:
            return
        values = {'cpu_pinning': topology.to_json()}
        db.instance_extra_update_by_uuid(context, self.instance_uuid,
                                         values)
        self.obj_reset_changes()

    @base.remotable_classmethod
    def get_by_instance_uuid(cls, context, instance_uuid):
        db_extra = db.instance_extra_get_by_instance_uuid(
                context, instance_uuid, columns=['cpu_pinning'])
        if not db_extra or not db_extra['cpu_pinning']:
            raise exception.InstanceCPUPinningNotFound(
                    instance_uuid=instance_uuid)

        topo = hardware.VirtInstanceCPUPinning.from_json(
                db_extra['cpu_pinning'])
        obj_topology = cls.obj_from_topology(topo)
        obj_topology.instance_uuid = db_extra['instance_uuid']
        # NOTE (ndipanov) not really needed as we never save, but left for
        # consistency
        obj_topology.obj_reset_changes()
        return obj_topology
