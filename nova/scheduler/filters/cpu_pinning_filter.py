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

from nova.scheduler import filters
from nova.virt import hardware


class CPUPinningFilter(filters.BaseHostFilter):
    """Filter for CPU pinning."""

    def host_passes(self, host_state, filter_properties):
        request_spec = filter_properties.get('request_spec', {})

        instance = request_spec.get('instance_properties', {})
        instance_pinning = hardware.instance_cpu_pinning_from_instance(
                instance)
        host_pinning, _fmt = (
                hardware.host_cpu_pinning_and_format_from_host(host_state))

        proposed_pinning = (
            hardware.VirtHostCPUPinning.get_pinning_for_instance(
                host_pinning, instance_pinning))
        if proposed_pinning and instance_pinning:
            instance['cpu_pinning'] = proposed_pinning.to_json()
            return True
        elif instance_pinning:
            return False
        else:
            return True
