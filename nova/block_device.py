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

import itertools
import re

from nova.openstack.common import log as logging
from nova.virt import driver

LOG = logging.getLogger(__name__)

DEFAULT_ROOT_DEV_NAME = '/dev/sda1'
_DEFAULT_MAPPINGS = {'ami': 'sda1',
                     'ephemeral0': 'sda2',
                     'root': DEFAULT_ROOT_DEV_NAME,
                     'swap': 'sda3'}


bdm_v1_attrs = set(["volume_id", "snapshot_id", "device_name",
                  "virtual_name", "volume_size"])


bdm_v2_attrs = set(['source_type', 'destination_type', 'uuid', 'guest_format',
                     'disk_bus', 'device_type', 'boot_index', 'volume_size',
                     'delete_on_termination', 'device_name'])


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
    return _ephemeral.match(device_name)


def ephemeral_num(ephemeral_name):
    assert is_ephemeral(ephemeral_name)
    return int(_ephemeral.sub('\\1', ephemeral_name))


def is_swap_or_ephemeral(device_name):
    return (device_name and
            (device_name == 'swap' or is_ephemeral(device_name)))


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
    blanks = []

    # 'ephemeralN', 'swap' and ebs
    for bdm in bdms:
        # ebs volume case
        if bdm['source_type'] in ('volume', 'snapshot'):
            ebs_devices.append(bdm['device_name'])
            continue

        if bdm['source_type'] == 'blank':
            blanks.append(bdm)

    # NOTE(yamahata): I'm not sure how ebs device should be numbered.
    #                 Right now sort by device name for deterministic
    #                 result.
    if ebs_devices:
        ebs_devices.sort()
        for nebs, ebs in enumerate(ebs_devices):
            mappings['ebs%d' % nebs] = ebs

    swap = [bdm for bdm in blanks if bdm['guest_format'] == 'swap']
    if swap:
        mappings['swap'] = swap.pop()['device_name']

    ephemerals = [bdm for bdm in blanks if bdm['guest_format'] != 'swap']
    if ephemerals:
        for num, eph in enumerate(ephemerals):
            mappings['ephemeral%d' % num] = eph['device_name']

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


def _blank_api_bdm():
    """Create a blank block device mapping dict that compute
    API methods work with - the db structure is different
    """
    blank = dict(zip(bdm_v2_attrs, itertools.repeat(None)))
    blank['boot_index'] = -1
    return blank


def create_image_bdm(image_ref, boot_index=0):
    """Create a block device dict based on the image_ref
    This is usefull in the API layer to keep the compatibility
    with having an image_ref as a field in the instance requests
    """
    image_bdm = _blank_api_bdm()
    image_bdm['source_type'] = 'image'
    image_bdm['uuid'] = image_ref
    image_bdm['delete_on_termination'] = True
    image_bdm['boot_index'] = boot_index
    image_bdm['device_type'] = 'disk'
    image_bdm['destination_type'] = 'local'
    return image_bdm


def bdm_v1_to_v2(bdms_v1, image_uuid, assign_boot_index=True):
    """Transform the old bdms to the new v2 format.
    Default some fields as necessary.
    """
    bdms_v2 = []
    for bdm in bdms_v1:

        bdm_v2 = _blank_api_bdm()

        virt_name = bdm.get('virt_name')
        volume_size = bdm.get('volume_size')

        if is_swap_or_ephemeral(virt_name):
            bdm_v2['source_type'] = 'blank'
            bdm_v2['volume_size'] = bdm.get('volume_size', 0)
            bdm_v2['delete_on_termination'] = True

            if virt_name == 'swap':
                bdm_v2['guest_format'] = 'swap'

            bdms_v2.append(bdm_v2)

        elif bdm.get('snapshot_id'):
            bdm_v2['source_type'] = 'snapshot'
            bdm_v2['destination_type'] = 'volume'
            bdm_v2['uuid'] = bdm['snapshot_id']
            if volume_size:
                bdm_v2['volume_size'] = volume_size
            bdm_v2['device_name'] = bdm.get('device_name')
            bdm_v2['delete_on_termination'] = bdm.get(
                'delete_on_termination', True)

            bdms_v2.append(bdm_v2)

        elif bdm.get('volume_id'):
            bdm_v2['source_type'] = 'volume'
            bdm_v2['destination_type'] = 'volume'
            bdm_v2['uuid'] = bdm['volume_id']
            if volume_size:
                bdm_v2['volum_size'] = volume_size
            bdm_v2['device_name'] = bdm.get('device_name')
            bdm_v2['delete_on_termination'] = bdm.get(
                'delete_on_termination', True)

            bdms_v2.append(bdm_v2)
        else:  # Log a warning that the bdm is not as expected
            LOG.warn(_("Got an unexpected block device "
                       "that cannot be converted to v2 format"))

    if image_uuid:
        image_bdm = create_image_bdm(image_uuid)
        bdms_v2 = [image_bdm] + bdms_v2

    # Decide boot sequences:
    if assign_boot_index:
        non_boot = [bdm for bdm in bdms_v2 if bdm['source_type'] == 'blank']
        bootable = [bdm for bdm in bdms_v2 if bdm not in non_boot]

        for index, bdm in enumerate(bootable):
            bdm['boot_index'] = index

        return bootable + non_boot
    else:
        return bdms_v2


def bdm_api_to_db_format(block_device_mapping, drop_bootable_image=False):
    preped_bdm = []

    for bdm in block_device_mapping:

        values = bdm.copy()

        source_type = values.get('source_type')
        uuid = values.get('uuid')

        if source_type not in ('volume', 'snapshot', 'image'):
            preped_bdm.append(values)
            continue

        if source_type == 'volume':
            values['volume_id'] = uuid
        elif source_type == 'snapshot':
            values['snapshot_id'] = uuid
        elif source_type == 'image':
            # NOTE (ndipanov): Drop the bootable image if
            # there is a bootable vol in bdm supplied in the image.
            # This can happen only if bdm _v1 is used for starting
            # an instance created from a snapshot.
            if (drop_bootable_image and
                values.get('boot_index') == 0):
                continue
            values['image_id'] = uuid

        del values['uuid']

        preped_bdm.append(values)

    return preped_bdm
