# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 OpenStack LLC.
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

import re

from sqlalchemy import Boolean, Column, MetaData, String, Table
from sqlalchemy.sql.expression import select, update


def upgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    
    # Create the new column for device type
    block_device_mapping = Table('block_device_mapping', meta, autoload=True)
    
    user_label = Column('user_label', String(255))
    is_root = Column('is_root', Boolean, default=False)
    device_type = Column('device_type', String(255))
    
    user_label.create(block_device_mapping)
    is_root.create(block_device_mapping)
    device_type.create(block_device_mapping)
    
    # Migrate the necessary values
    _update_bdms(meta, block_device_mapping)
    
    # Drop the virtual_name column as we won't need it anymore
    virtual_name = block_device_mapping.columns.virtual_name
    virtual_name.drop()


def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    
    # Create the old virtual_name column
    block_device_mapping = Table('block_device_mapping', meta, autoload=True)
    virtual_name = Column('virtual_name', String(255), nullable=True)
    virtual_name.create(block_device_mapping)
    
    _revert_bdms(meta, block_device_mapping)
    
    device_type = block_device_mapping.columns.device_type
    is_root = block_device_mapping.columns.is_root
    user_label = block_device_mapping.columns.user_label
    device_type.drop()
    is_root.drop()
    user_label.drop()


_ephemeral = re.compile('^ephemeral(\d|[1-9]\d+)$')
_dev = re.compile('^/dev/')


def _is_ephemeral(virt_name):
    return virt_name and _ephemeral.match(virt_name)


def _is_swap(virt_name):
    return virt_name == 'swap'


def _strip_dev(device_name):
    """remove leading '/dev/'"""
    return _dev.sub('', device_name) if device_name else device_name


def _update_bdms(meta, bdm):
    
    instance = Table('instances', meta, autoload=True)
    
    instance_q = select([instance.c.uuid, instance.c.root_device_name])
    root_devs = dict(instance_q.execute().fetchall())
    
    bdm_q = select(
        [bdm.c.id, bdm.c.device_name, bdm.c.virtual_name,
         bdm.c.snapshot_id, bdm.c.volume_id, bdm.c.instance_uuid]
    )
    
    for (bdm_id, device_name, virt_name, snapshot_id,
         volume_id, instance_uuid ) in bdm_q.execute():
        if _is_ephemeral(virt_name):
            # For ephemerals keep their virtual_name as a user_label
            update(bdm).where(bdm.c.id == bdm_id).values(
                device_type='ephemeral',
                user_label=virt_name).execute()
        
        if _is_swap(virt_name):
            update(bdm).where(bdm.c.id == bdm_id).values(
                device_type='swap').execute()
        
        if virt_name == 'NoDevice':
            update(bdm).where(bdm.c.id == bdm_id).values(
                device_type='NoDevice').execute()
            
        # NOTE (ndipanov):  We need to check if this volume is root.
        #                   Libvirt will record the root device in the db
        #                   instance table.
        if volume_id or snapshot_id:
            # This will be correct with libvirt
            # TODO: why!
            is_root = device_name == _strip_dev(
                root_devs.get(instance_uuid, "")) 
            if is_root:
                update(bdm).where(bdm.c.id == bdm_id).values(
                    is_root=True).execute()
            if snapshot_id:
                update(bdm).where(bdm.c.id == bdm_id).values(
                    device_type='snapshot').execute()
            else:
                update(bdm).where(bdm.c.id == bdm_id).values(
                    device_type='volume').execute()

        # copy the device_name to the user_label column
        update(bdm).where(bdm.c.id == bdm_id).values(
            user_label=device_name).execute()


def _revert_bdms(meta, bdm):
    # NOTE (ndipanov): This will only restore virtual names that
    #                   are either swap, ephemeral, or NoDevice.
    #                   but these are the only values that are
    #                   considered by nova code anyway.

    bdm_q = select([bdm.c.id, bdm.c.device_type, bdm.c.user_label])
    
    for bdm_id, device_type, user_label in bdm_q.execute(): 
        if device_type == 'ephemeral':
            update(bdm).where(bdm.c.id == bdm_id).values(
                virtual_name=user_label).execute()
        
        if device_type == 'swap':
            update(bdm).where(bdm.c.id == bdm_id).values(
                virtual_name='swap').execute()
        
        if device_type == 'NoDevice':
            update(bdm).where(bdm.c.id == bdm_id).values(
                virtual_name ='NoDevice').execute()
