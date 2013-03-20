# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
#    Copyright 2013 OpenStack LLC
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


def create_fake_image_bdm(image_ref):
    """Create a block device mapping for the image."""
    return {
        'source_type': 'image',
        'destination_type': 'local',
        'uuid': image_ref,
        'disk_bus': None,
        'device_type': 'disk',
        'boot_index': 0,
        'volume_size': None,
        'delete_on_termination': True,
        'device_name': None,
    }


def fake_validate_bdm(*args, **kwargs):
    pass


def stub_out_validate_bdm(stubs, compute_api_object):
    stubs.Set(compute_api_object, '_validate_bdm',
              fake_validate_bdm)
