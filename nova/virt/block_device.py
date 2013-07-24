# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import functools
import operator

from nova import block_device
from nova.openstack.common import jsonutils
from nova.openstack.common import log as logging

LOG = logging.getLogger(__name__)

class _NotTransformable(Exception):
    pass


class _InvalidType(_NotTransformable):
    pass


class _NoLegacy(Exception):
    pass


class DriverBlockDevice(dict):
    def __init__(self, bdm):
        if 'no_device' in bdm:
            raise _NotTransformable()

        self.update(dict((field, None)
                    for field in self._fields))
        self._transform(block_device)

    def _transform(self, bdm):
        """Transform bdm to the format that is passed to drivers."""
        raise NotImplementedError()

    def legacy(self):
        """Basic legacy transformation.

        Basic method will just drop the fields that are not in
        _legacy_fields set. Override this in subclass if needed.
        """
        legacy_bdm = dict((k, None) for k in _legacy_fields)
        return legacy_bdm.update(
            dict((k, v) for k, v in self.iteritems()
                    if k in self._legacy_fields)
        )

    def attach(self, **kwargs):
        """Override in subclass with the connecting logic for the type of device."""
        raise NotImplementedError()


class DriverSwapBlockDevice(DriverBlockDevice):
    _fields = set(['device_name', 'swap_size', 'disk_bus'])
    _legacy_fields = _fields - set(['disk_bus'])

    def _transform(self, bdm):
        if not block_device.new_format_is_swap(bdm):
            raise _InvalidType
        self.update({
            'device_name': bdm.get('device_name'),
            'swap_size': bdm.get('volume_size', 0),
            'disk_bus': bdm.get('disk_bus')
        })


class DriverEphemeralBlockDevice(DriverBlockDevice):
    _fields = set(['device_name', 'size', 'disk_bus'])
    _legacy_fields = (_fields -
                      set(['disk_bus']) |
                      set(['num', 'virtual_name']))

    def _transform(self, bdm):
        if not block_device.new_format_is_swap(bdm):
            raise _InvalidType
        self.update({
            'device_name': bdm.get('device_name'),
            'size': bdm.get('volume_size', 0),
            'disk_bus': bdm.get('disk_bus')
        })

    def legacy(self, num=0):
        legacy_bdm = super(DriverEphemeralBlockDevice, self).legacy()
        legacy_bdm['num'] = num
        legacy_bdm['virtual_name'] = 'ephemeral' + str(num)
        return legacy_bdm


class DriverVolumeBlockDevice(DriverBlockDevice):
    _legacy_fields = set(['connection_info', 'mount_device',
                          'delete_on_termination'])
    _new_fields = set(['guest_format', 'device_type',
                       'disk_bus', 'boot_index'])
    _fields = _legacy_fields | _new_fields

    def _transform(self, bdm):
        if not bdm.get('volume_id'):
            raise _InvalidType

        # NOTE (ndipanov): Save it as an attribute as we will
        #                  need it for attach()
        self.volume_size = bdm.get('volume_size')
        self.volume_id = bdm.get('volume_id')
        self.id = bdm.get('id')

        self.update(
            dict((k, v) for k, v in bdm.iteritems()
                 if k in self._fields - set(['mount_device']))
        )
        self['mount_device'] = bdm.get('device_name')

    def attach(self, context, instance, volume_api, virt_driver, dp_api=None):
        volume = self.volume_api.get(context, self.volume_id)
        volume_api.check_attach(context, volume, instance=instance)

        # Attach a volume to an instance at boot time. So actual attach
        # is done by instance creation.
        instance_id = instance['id']
        instance_uuid = instance['uuid']
        volume_id = volume['id']
        context = context.elevated()

        LOG.audit(_('Booting with volume %(volume_id)s at %(mountpoint)s'),
                  {'volume_id': volume_id,
                   'mountpoint': self['mount_device']},
                  context=context, instance=instance)

        connector = virt_driver.get_volume_connector(instance)
        connection_info = volume_api.initialize_connection(context,
                                                           volume_id,
                                                           connector)
        volume_api.attach(context, volume_id,
                          instance_uuid, self['mount_device'])

        if 'serial' not in connection_info:
            connection_info['serial'] = bdm['volume_id']
        self['connection_info'] = connection_info
        if db_api:
            db_api.block_device_mapping_update(
                context, self.id,
                {'connection_info': jsonutils.dumps(connection_info)})


class DriverSnapshotBlockDevice(DriverVolumeBlockDevice):
    def _transform(self, bdm):
        if not bdm.get('snapshot_id'):
            raise _InvalidType

        # NOTE (ndipanov): Save it as an attribute as we will
        #                  need it for attach()
        self.volume_size = bdm.get('volume_size')
        self.snapshot_id = bdm.get('snapshot_id')
        self.volume_id = bdm.get('volume_id')
        self.id = bdm.get('id')

        self.update(
            dict((k, v) for k, v in bdm.iteritems()
                 if k in self._fields - set(['mount_device']))
        )
        self['mount_device'] = bdm.get('device_name')

    def attach(self, context, instance, volume_api, virt_driver,
               db_api=None, wait_func=None):
        snapshot = volume_api.get_snapshot(context,
                                           self.snapshot_id)
        vol = volume_api.create(context, self.volume_size, '', '', snapshot)
        if wait_func:
            wait_func(context, vol['id'])
        if db_api:
            db_api.block_device_mapping_update(context, self.id,
                                               {'volume_id': vol['id']})
        self.volume_id = vol['id']

        # Call the volume attach now
        super(DriverSnapshotBlockDevice, self).attach(context, instance,
                                                      volume_api, virt_driver,
                                                      dp_api)


def _convert_block_devices(device_type, block_device_mapping):
    def _is_transformable(bdm):
        try:
            device_type(bdm)
        except _NotTransformable:
            return False
        return True

    return [device_type(bdm)
            for bdm in block_device_mapping
            if _is_transformable(bdm)]


convert_swap = functools.partial(_convert_block_devices,
                                 DriverSwapBlockDevice)


convert_ephemerals = functools.partial(_convert_block_devices,
                                      DriverSwapBlockDevice)


convert_volumes = functools.partial(_convert_block_devices,
                                   DriverVolumeBlockDevice)


convert_snapshots = functools.partial(_convert_block_devices,
                                     DriverSnapshotBlockDevice)


def attach_block_devices(block_device_mapping, *attach_args, **attach_kwargs):
    return map(operator.methodcaller('attach', *attach_args, **attach_kwargs),
        block_device_mapping)


def legacy_block_devices(block_device_mapping):
    def _has_legacy(bdm):
        try:
            bdm.legacy()
        except _NoLegacy:
            return False
        return True

    bdms = [bdm.legacy()
            for bdm in block_device_mapping
            if _has_legacy(bdm)]
    # Re-enumerate ephemeral devices
    if all(isinstance(bdm, DriverEphemeralBlockDevice)
           for bdm in bdms):
        for i, dev in enumerate(bdms):
            dev['virtual_name'] = dev['virtual_name'][:-1] + str(i)

    return bdms
