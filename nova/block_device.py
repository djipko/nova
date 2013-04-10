# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2011 Isaku Yamahata <yamahata@valinux co jp>
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

import collections
import copy
import re


from nova import exception
from nova.openstack.common import log as logging
from nova.virt import driver

LOG = logging.getLogger(__name__)

DEFAULT_ROOT_DEV_NAME = '/dev/sda1'
_DEFAULT_MAPPINGS = {'ami': 'sda1',
                     'ephemeral0': 'sda2',
                     'root': DEFAULT_ROOT_DEV_NAME,
                     'swap': 'sda3'}


def properties_root_device_name(properties):
    """get root device name from image meta data.
    If it isn't specified, return None.
    """
    root_device_name = None

    # NOTE(yamahata): see image_service.s3.s3create()
    for bdm in properties.get('mappings', []):
        if bdm['virtual'] == 'root':
            root_device_name = bdm['device']

    # NOTE(yamahata): register_image's command line can override
    #                 <machine>.manifest.xml
    if 'root_device_name' in properties:
        root_device_name = properties['root_device_name']

    return root_device_name


_ephemeral = re.compile('^ephemeral(\d|[1-9]\d+)$')


def is_ephemeral(device_name):
    return _ephemeral.match(device_name) is not None


def ephemeral_num(ephemeral_name):
    assert is_ephemeral(ephemeral_name)
    return int(_ephemeral.sub('\\1', ephemeral_name))


def is_swap_or_ephemeral(device_name):
    return device_name == 'swap' or is_ephemeral(device_name)


def mappings_prepend_dev(mappings):
    """Prepend '/dev/' to 'device' entry of swap/ephemeral virtual type."""
    for m in mappings:
        virtual = m['virtual']
        if (is_swap_or_ephemeral(virtual) and
            (not m['device'].startswith('/'))):
            m['device'] = '/dev/' + m['device']
    return mappings


_dev = re.compile('^/dev/')


def strip_dev(device_name):
    """remove leading '/dev/'."""
    return _dev.sub('', device_name) if device_name else device_name


_pref = re.compile('^((x?v|s)d)')


def strip_prefix(device_name):
    """remove both leading /dev/ and xvd or sd or vd."""
    device_name = strip_dev(device_name)
    return _pref.sub('', device_name)


def instance_block_mapping(instance, bdms):
    root_device_name = instance['root_device_name']
    # NOTE(clayg): remove this when xenapi is setting default_root_device
    if root_device_name is None:
        if driver.compute_driver_matches('xenapi.XenAPIDriver'):
            root_device_name = '/dev/xvda'
        else:
            return _DEFAULT_MAPPINGS

    mappings = {}
    mappings['ami'] = strip_dev(root_device_name)
    mappings['root'] = root_device_name
    default_ephemeral_device = instance.get('default_ephemeral_device')
    if default_ephemeral_device:
        mappings['ephemeral0'] = default_ephemeral_device
    default_swap_device = instance.get('default_swap_device')
    if default_swap_device:
        mappings['swap'] = default_swap_device
    ebs_devices = []

    # 'ephemeralN', 'swap' and ebs
    for bdm in bdms:
        if bdm['no_device']:
            continue

        # ebs volume case
        if (bdm['volume_id'] or bdm['snapshot_id']):
            ebs_devices.append(bdm['device_name'])
            continue

        virtual_name = bdm['virtual_name']
        if not virtual_name:
            continue

        if is_swap_or_ephemeral(virtual_name):
            mappings[virtual_name] = bdm['device_name']

    # NOTE(yamahata): I'm not sure how ebs device should be numbered.
    #                 Right now sort by device name for deterministic
    #                 result.
    if ebs_devices:
        nebs = 0
        ebs_devices.sort()
        for ebs in ebs_devices:
            mappings['ebs%d' % nebs] = ebs
            nebs += 1

    return mappings


def match_device(device):
    """Matches device name and returns prefix, suffix."""
    match = re.match("(^/dev/x{0,1}[a-z]{0,1}d{0,1})([a-z]+)[0-9]*$", device)
    if not match:
        return None
    return match.groups()


def volume_in_mapping(mount_device, block_device_info):
    block_device_list = [strip_dev(vol['mount_device'])
                         for vol in
                         driver.block_device_info_get_mapping(
                         block_device_info)]

    swap = driver.block_device_info_get_swap(block_device_info)
    if driver.swap_is_usable(swap):
        block_device_list.append(strip_dev(swap['device_name']))

    block_device_list += [strip_dev(ephemeral['device_name'])
                          for ephemeral in
                          driver.block_device_info_get_ephemerals(
                          block_device_info)]

    LOG.debug(_("block_device_list %s"), block_device_list)
    return strip_dev(mount_device) in block_device_list


bdm_v1_field_names = ["id", "instance_uuid", "device_name",
                      "delete_on_termination", "virtual_name",
                      "snapshot_id", "volume_id", "volume_size",
                      "no_device", "connection_info"]


class ItemHandlerBase(object):
    def __init__(self, key):
        self._key = key
        self.deleted = False

    def get(self, d):
        return d[self._key]

    def set(self, d, val):
        self.deleted = False

    def delete(self, d):
        self.deleted = True


class NoOpHandler(ItemHandlerBase):
    def set(self, d, val):
        d[self._key] = val
        super(NoOpHandler, self).set(d, val)


v1_field_handlers = dict((field, NoOpHandler(field))
                        for field in bdm_v1_field_names)


class BlockDeviceDict(collections.MutableMapping):
    """Dict like class that will allow us to hide the new block device
    database structure from the code that is not ready for it.
    It will always act like it has the same keys as v1_field_handlers, that
    is - the old database structure.

    Once initialized with the v2 fields - it will delegate all attempts
    to work with v1 fields to the handler assigned to the field in
    _v1_field_handlers dictionary, which will calculate it based on v2 fields
    the class was initialised with.

    Handlers are supposed to implement three methods get, set and delete
    which all take the original dict as a parameter. These methods should
    raise a KeyError should something go wrong. Handlers should also
    expose a 'deleted' attribute for consistent behaviour.
    """

    def __init__(self, *args, **kwargs):
        self._real_dict = dict(*args, **kwargs)
        self._fields = copy.deepcopy(v1_field_handlers)
        for _, fld in self._fields.iteritems():
            try:
                fld.get(self._real_dict)
            except KeyError:
                fld.deleted = True

    def __getitem__(self, key):
        handler = self._fields.get(key)
        if handler:
            if handler.deleted:
                raise KeyError(key)
            # Attempt to compute the field
            return handler.get(self._real_dict)
        else:
            raise KeyError(key)

    def __setitem__(self, key, val):
        if key in self._fields:
            self._fields[key].set(self._real_dict, val)
        else:
            # NOTE (ndipanov): This is not a valid situation
            # as it means an attempt to add an invalid field
            # to a bdm dict. Code that is "in the know" should
            # manipulate _real_dict. This is a nice to have
            # precaution
            raise exception.InvalidBDMField(key=key)

    def __delitem__(self, key):
        handler = self._fields.get(key)
        if handler:
            if handler.deleted:
                raise KeyError(key)
            handler.delete(self._real_dict)
        else:
            del self._real_dict[key]

    def __len__(self):
        # NOTE(ndipanov): len will act as the original dict, and we'll assume
        # new code is "in the know" and will access _real_dict for the
        # time being.
        return len([fld for fld in self._fields.values() if not fld.deleted])

    def __iter__(self):
        return (key for key, fld in self._fields.iteritems()
                if not fld.deleted)


def bdm_get_values(bdm_dict):
    try:
        return bdm_dict._real_dict
    except AttributeError:
        return bdm_dict
