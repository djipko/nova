# Copyright 2014 Red Hat, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import collections
import copy
import itertools

from oslo.config import cfg
from oslo.serialization import jsonutils
import six

from nova import context
from nova import exception
from nova.i18n import _
from nova import objects
from nova.openstack.common import log as logging

virt_cpu_opts = [
    cfg.StrOpt('vcpu_pin_set',
                help='Defines which pcpus that instance vcpus can use. '
               'For example, "4-12,^8,15"'),
]

CONF = cfg.CONF
CONF.register_opts(virt_cpu_opts)

LOG = logging.getLogger(__name__)


def get_vcpu_pin_set():
    """Parsing vcpu_pin_set config.

    Returns a set of pcpu ids can be used by instances.
    """
    if not CONF.vcpu_pin_set:
        return None

    cpuset_ids = parse_cpu_spec(CONF.vcpu_pin_set)
    if not cpuset_ids:
        raise exception.Invalid(_("No CPUs available after parsing %r") %
                                CONF.vcpu_pin_set)
    return cpuset_ids


def parse_cpu_spec(spec):
    """Parse a CPU set specification.

    :param spec: cpu set string eg "1-4,^3,6"

    Each element in the list is either a single
    CPU number, a range of CPU numbers, or a
    caret followed by a CPU number to be excluded
    from a previous range.

    :returns: a set of CPU indexes
    """

    cpuset_ids = set()
    cpuset_reject_ids = set()
    for rule in spec.split(','):
        rule = rule.strip()
        # Handle multi ','
        if len(rule) < 1:
            continue
        # Note the count limit in the .split() call
        range_parts = rule.split('-', 1)
        if len(range_parts) > 1:
            # So, this was a range; start by converting the parts to ints
            try:
                start, end = [int(p.strip()) for p in range_parts]
            except ValueError:
                raise exception.Invalid(_("Invalid range expression %r")
                                        % rule)
            # Make sure it's a valid range
            if start > end:
                raise exception.Invalid(_("Invalid range expression %r")
                                        % rule)
            # Add available CPU ids to set
            cpuset_ids |= set(range(start, end + 1))
        elif rule[0] == '^':
            # Not a range, the rule is an exclusion rule; convert to int
            try:
                cpuset_reject_ids.add(int(rule[1:].strip()))
            except ValueError:
                raise exception.Invalid(_("Invalid exclusion "
                                          "expression %r") % rule)
        else:
            # OK, a single CPU to include; convert to int
            try:
                cpuset_ids.add(int(rule))
            except ValueError:
                raise exception.Invalid(_("Invalid inclusion "
                                          "expression %r") % rule)

    # Use sets to handle the exclusion rules for us
    cpuset_ids -= cpuset_reject_ids

    return cpuset_ids


def format_cpu_spec(cpuset, allow_ranges=True):
    """Format a libvirt CPU range specification.

    :param cpuset: set (or list) of CPU indexes

    Format a set/list of CPU indexes as a libvirt CPU
    range specification. It allow_ranges is true, it
    will try to detect continuous ranges of CPUs,
    otherwise it will just list each CPU index explicitly.

    :returns: a formatted CPU range string
    """

    # We attempt to detect ranges, but don't bother with
    # trying to do range negations to minimize the overall
    # spec string length
    if allow_ranges:
        ranges = []
        previndex = None
        for cpuindex in sorted(cpuset):
            if previndex is None or previndex != (cpuindex - 1):
                ranges.append([])
            ranges[-1].append(cpuindex)
            previndex = cpuindex

        parts = []
        for entry in ranges:
            if len(entry) == 1:
                parts.append(str(entry[0]))
            else:
                parts.append("%d-%d" % (entry[0], entry[len(entry) - 1]))
        return ",".join(parts)
    else:
        return ",".join(str(id) for id in sorted(cpuset))


def get_number_of_serial_ports(flavor, image_meta):
    """Get the number of serial consoles from the flavor or image

    :param flavor: Flavor object to read extra specs from
    :param image_meta: Image object to read image metadata from

    If flavor extra specs is not set, then any image meta value is permitted.
    If flavour extra specs *is* set, then this provides the default serial
    port count. The image meta is permitted to override the extra specs, but
    *only* with a lower value. ie

    - flavor hw:serial_port_count=4
      VM gets 4 serial ports
    - flavor hw:serial_port_count=4 and image hw_serial_port_count=2
      VM gets 2 serial ports
    - image hw_serial_port_count=6
      VM gets 6 serial ports
    - flavor hw:serial_port_count=4 and image hw_serial_port_count=6
      Abort guest boot - forbidden to exceed flavor value

    :returns: number of serial ports
    """

    def get_number(obj, property):
        num_ports = obj.get(property)
        if num_ports is not None:
            try:
                num_ports = int(num_ports)
            except ValueError:
                raise exception.ImageSerialPortNumberInvalid(
                    num_ports=num_ports, property=property)
        return num_ports

    image_meta_prop = (image_meta or {}).get('properties', {})

    flavor_num_ports = get_number(flavor.extra_specs, "hw:serial_port_count")
    image_num_ports = get_number(image_meta_prop, "hw_serial_port_count")

    if (flavor_num_ports and image_num_ports) is not None:
        if image_num_ports > flavor_num_ports:
            raise exception.ImageSerialPortNumberExceedFlavorValue()
        return image_num_ports

    return flavor_num_ports or image_num_ports or 1


class VirtCPUTopology(object):

    def __init__(self, sockets, cores, threads):
        """Create a new CPU topology object

        :param sockets: number of sockets, at least 1
        :param cores: number of cores, at least 1
        :param threads: number of threads, at least 1

        Create a new CPU topology object representing the
        number of sockets, cores and threads to use for
        the virtual instance.
        """

        self.sockets = sockets
        self.cores = cores
        self.threads = threads

    def score(self, wanttopology):
        """Calculate score for the topology against a desired configuration

        :param wanttopology: VirtCPUTopology instance for preferred topology

        Calculate a score indicating how well this topology
        matches against a preferred topology. A score of 3
        indicates an exact match for sockets, cores and threads.
        A score of 2 indicates a match of sockets & cores or
        sockets & threads or cores and threads. A score of 1
        indicates a match of sockets or cores or threads. A
        score of 0 indicates no match

        :returns: score in range 0 (worst) to 3 (best)
        """

        score = 0
        if (wanttopology.sockets != -1 and
            self.sockets == wanttopology.sockets):
            score = score + 1
        if (wanttopology.cores != -1 and
            self.cores == wanttopology.cores):
            score = score + 1
        if (wanttopology.threads != -1 and
            self.threads == wanttopology.threads):
            score = score + 1
        return score

    @staticmethod
    def get_topology_constraints(flavor, image_meta):
        """Get the topology constraints declared in flavor or image

        :param flavor: Flavor object to read extra specs from
        :param image_meta: Image object to read image metadata from

        Gets the topology constraints from the configuration defined
        in the flavor extra specs or the image metadata. In the flavor
        this will look for

         hw:cpu_sockets - preferred socket count
         hw:cpu_cores - preferred core count
         hw:cpu_threads - preferred thread count
         hw:cpu_maxsockets - maximum socket count
         hw:cpu_maxcores - maximum core count
         hw:cpu_maxthreads - maximum thread count

        In the image metadata this will look at

         hw_cpu_sockets - preferred socket count
         hw_cpu_cores - preferred core count
         hw_cpu_threads - preferred thread count
         hw_cpu_maxsockets - maximum socket count
         hw_cpu_maxcores - maximum core count
         hw_cpu_maxthreads - maximum thread count

        The image metadata must be strictly lower than any values
        set in the flavor. All values are, however, optional.

        This will return a pair of VirtCPUTopology instances,
        the first giving the preferred socket/core/thread counts,
        and the second giving the upper limits on socket/core/
        thread counts.

        exception.ImageVCPULimitsRangeExceeded will be raised
        if the maximum counts set against the image exceed
        the maximum counts set against the flavor

        exception.ImageVCPUTopologyRangeExceeded will be raised
        if the preferred counts set against the image exceed
        the maximum counts set against the image or flavor

        :returns: (preferred topology, maximum topology)
        """

        # Obtain the absolute limits from the flavor
        flvmaxsockets = int(flavor.extra_specs.get(
            "hw:cpu_max_sockets", 65536))
        flvmaxcores = int(flavor.extra_specs.get(
            "hw:cpu_max_cores", 65536))
        flvmaxthreads = int(flavor.extra_specs.get(
            "hw:cpu_max_threads", 65536))

        LOG.debug("Flavor limits %(sockets)d:%(cores)d:%(threads)d",
                  {"sockets": flvmaxsockets,
                   "cores": flvmaxcores,
                   "threads": flvmaxthreads})

        # Get any customized limits from the image
        maxsockets = int(image_meta.get("properties", {})
                         .get("hw_cpu_max_sockets", flvmaxsockets))
        maxcores = int(image_meta.get("properties", {})
                       .get("hw_cpu_max_cores", flvmaxcores))
        maxthreads = int(image_meta.get("properties", {})
                         .get("hw_cpu_max_threads", flvmaxthreads))

        LOG.debug("Image limits %(sockets)d:%(cores)d:%(threads)d",
                  {"sockets": maxsockets,
                   "cores": maxcores,
                   "threads": maxthreads})

        # Image limits are not permitted to exceed the flavor
        # limits. ie they can only lower what the flavor defines
        if ((maxsockets > flvmaxsockets) or
            (maxcores > flvmaxcores) or
            (maxthreads > flvmaxthreads)):
            raise exception.ImageVCPULimitsRangeExceeded(
                sockets=maxsockets,
                cores=maxcores,
                threads=maxthreads,
                maxsockets=flvmaxsockets,
                maxcores=flvmaxcores,
                maxthreads=flvmaxthreads)

        # Get any default preferred topology from the flavor
        flvsockets = int(flavor.extra_specs.get("hw:cpu_sockets", -1))
        flvcores = int(flavor.extra_specs.get("hw:cpu_cores", -1))
        flvthreads = int(flavor.extra_specs.get("hw:cpu_threads", -1))

        LOG.debug("Flavor pref %(sockets)d:%(cores)d:%(threads)d",
                  {"sockets": flvsockets,
                   "cores": flvcores,
                   "threads": flvthreads})

        # If the image limits have reduced the flavor limits
        # we might need to discard the preferred topology
        # from the flavor
        if ((flvsockets > maxsockets) or
            (flvcores > maxcores) or
            (flvthreads > maxthreads)):
            flvsockets = flvcores = flvthreads = -1

        # Finally see if the image has provided a preferred
        # topology to use
        sockets = int(image_meta.get("properties", {})
                      .get("hw_cpu_sockets", -1))
        cores = int(image_meta.get("properties", {})
                    .get("hw_cpu_cores", -1))
        threads = int(image_meta.get("properties", {})
                      .get("hw_cpu_threads", -1))

        LOG.debug("Image pref %(sockets)d:%(cores)d:%(threads)d",
                  {"sockets": sockets,
                   "cores": cores,
                   "threads": threads})

        # Image topology is not permitted to exceed image/flavor
        # limits
        if ((sockets > maxsockets) or
            (cores > maxcores) or
            (threads > maxthreads)):
            raise exception.ImageVCPUTopologyRangeExceeded(
                sockets=sockets,
                cores=cores,
                threads=threads,
                maxsockets=maxsockets,
                maxcores=maxcores,
                maxthreads=maxthreads)

        # If no preferred topology was set against the image
        # then use the preferred topology from the flavor
        # We use 'and' not 'or', since if any value is set
        # against the image this invalidates the entire set
        # of values from the flavor
        if sockets == -1 and cores == -1 and threads == -1:
            sockets = flvsockets
            cores = flvcores
            threads = flvthreads

        LOG.debug("Chosen %(sockets)d:%(cores)d:%(threads)d limits "
                  "%(maxsockets)d:%(maxcores)d:%(maxthreads)d",
                  {"sockets": sockets, "cores": cores,
                   "threads": threads, "maxsockets": maxsockets,
                   "maxcores": maxcores, "maxthreads": maxthreads})

        return (VirtCPUTopology(sockets, cores, threads),
                VirtCPUTopology(maxsockets, maxcores, maxthreads))

    @staticmethod
    def get_possible_topologies(vcpus, maxtopology, allow_threads):
        """Get a list of possible topologies for a vCPU count
        :param vcpus: total number of CPUs for guest instance
        :param maxtopology: VirtCPUTopology for upper limits
        :param allow_threads: if the hypervisor supports CPU threads

        Given a total desired vCPU count and constraints on the
        maximum number of sockets, cores and threads, return a
        list of VirtCPUTopology instances that represent every
        possible topology that satisfies the constraints.

        exception.ImageVCPULimitsRangeImpossible is raised if
        it is impossible to achieve the total vcpu count given
        the maximum limits on sockets, cores & threads.

        :returns: list of VirtCPUTopology instances
        """

        # Clamp limits to number of vcpus to prevent
        # iterating over insanely large list
        maxsockets = min(vcpus, maxtopology.sockets)
        maxcores = min(vcpus, maxtopology.cores)
        maxthreads = min(vcpus, maxtopology.threads)

        if not allow_threads:
            maxthreads = 1

        LOG.debug("Build topologies for %(vcpus)d vcpu(s) "
                  "%(maxsockets)d:%(maxcores)d:%(maxthreads)d",
                  {"vcpus": vcpus, "maxsockets": maxsockets,
                   "maxcores": maxcores, "maxthreads": maxthreads})

        # Figure out all possible topologies that match
        # the required vcpus count and satisfy the declared
        # limits. If the total vCPU count were very high
        # it might be more efficient to factorize the vcpu
        # count and then only iterate over its factors, but
        # that's overkill right now
        possible = []
        for s in range(1, maxsockets + 1):
            for c in range(1, maxcores + 1):
                for t in range(1, maxthreads + 1):
                    if t * c * s == vcpus:
                        possible.append(VirtCPUTopology(s, c, t))

        # We want to
        #  - Minimize threads (ie larger sockets * cores is best)
        #  - Prefer sockets over cores
        possible = sorted(possible, reverse=True,
                          key=lambda x: (x.sockets * x.cores,
                                         x.sockets,
                                         x.threads))

        LOG.debug("Got %d possible topologies", len(possible))
        if len(possible) == 0:
            raise exception.ImageVCPULimitsRangeImpossible(vcpus=vcpus,
                                                           sockets=maxsockets,
                                                           cores=maxcores,
                                                           threads=maxthreads)

        return possible

    @staticmethod
    def sort_possible_topologies(possible, wanttopology):
        """Sort the topologies in order of preference
        :param possible: list of VirtCPUTopology instances
        :param wanttopology: VirtCPUTopology for preferred topology

        This takes the list of possible topologies and resorts
        it such that those configurations which most closely
        match the preferred topology are first.

        :returns: sorted list of VirtCPUTopology instances
        """

        # Look at possible topologies and score them according
        # to how well they match the preferred topologies
        # We don't use python's sort(), since we want to
        # preserve the sorting done when populating the
        # 'possible' list originally
        scores = collections.defaultdict(list)
        for topology in possible:
            score = topology.score(wanttopology)
            scores[score].append(topology)

        # Build list of all possible topologies sorted
        # by the match score, best match first
        desired = []
        desired.extend(scores[3])
        desired.extend(scores[2])
        desired.extend(scores[1])
        desired.extend(scores[0])

        return desired

    @staticmethod
    def get_desirable_configs(flavor, image_meta, allow_threads=True):
        """Get desired CPU topologies according to settings

        :param flavor: Flavor object to query extra specs from
        :param image_meta: ImageMeta object to query properties from
        :param allow_threads: if the hypervisor supports CPU threads

        Look at the properties set in the flavor extra specs and
        the image metadata and build up a list of all possible
        valid CPU topologies that can be used in the guest. Then
        return this list sorted in order of preference.

        :returns: sorted list of VirtCPUTopology instances
        """

        LOG.debug("Getting desirable topologies for flavor %(flavor)s "
                  "and image_meta %(image_meta)s",
                  {"flavor": flavor, "image_meta": image_meta})

        preferred, maximum = (
            VirtCPUTopology.get_topology_constraints(flavor,
                                                     image_meta))

        possible = VirtCPUTopology.get_possible_topologies(
            flavor.vcpus, maximum, allow_threads)
        desired = VirtCPUTopology.sort_possible_topologies(
            possible, preferred)

        return desired

    @staticmethod
    def get_best_config(flavor, image_meta, allow_threads=True):
        """Get bst CPU topology according to settings

        :param flavor: Flavor object to query extra specs from
        :param image_meta: ImageMeta object to query properties from
        :param allow_threads: if the hypervisor supports CPU threads

        Look at the properties set in the flavor extra specs and
        the image metadata and build up a list of all possible
        valid CPU topologies that can be used in the guest. Then
        return the best topology to use

        :returns: a VirtCPUTopology instance for best topology
        """

        return VirtCPUTopology.get_desirable_configs(flavor,
                                                     image_meta,
                                                     allow_threads)[0]


class VirtNUMATopologyCell(object):
    """Class for reporting NUMA resources in a cell

    The VirtNUMATopologyCell class represents the
    hardware resources present in a NUMA cell.
    """

    def __init__(self, id, cpuset, memory):
        """Create a new NUMA Cell

        :param id: integer identifier of cell
        :param cpuset: set containing list of CPU indexes
        :param memory: RAM measured in KiB

        Creates a new NUMA cell object to record the hardware
        resources.

        :returns: a new NUMA cell object
        """

        super(VirtNUMATopologyCell, self).__init__()

        self.id = id
        self.cpuset = cpuset
        self.memory = memory

    def _to_dict(self):
        return {'cpus': format_cpu_spec(self.cpuset, allow_ranges=False),
                'mem': {'total': self.memory},
                'id': self.id}

    @classmethod
    def _from_dict(cls, data_dict):
        cpuset = parse_cpu_spec(data_dict.get('cpus', ''))
        memory = data_dict.get('mem', {}).get('total', 0)
        cell_id = data_dict.get('id')
        return cls(cell_id, cpuset, memory)


class VirtNUMATopologyCellLimit(VirtNUMATopologyCell):
    def __init__(self, id, cpuset, memory, cpu_limit, memory_limit):
        """Create a new NUMA Cell with usage

        :param id: integer identifier of cell
        :param cpuset: set containing list of CPU indexes
        :param memory: RAM measured in KiB
        :param cpu_limit: maximum number of  CPUs allocated
        :param memory_usage: maxumum RAM allocated in KiB

        Creates a new NUMA cell object to represent the max hardware
        resources and utilization. The number of CPUs specified
        by the @cpu_usage parameter may be larger than the number
        of bits set in @cpuset if CPU overcommit is used. Likewise
        the amount of RAM specified by the @memory_usage parameter
        may be larger than the available RAM in @memory if RAM
        overcommit is used.

        :returns: a new NUMA cell object
        """

        super(VirtNUMATopologyCellLimit, self).__init__(
            id, cpuset, memory)

        self.cpu_limit = cpu_limit
        self.memory_limit = memory_limit

    def _to_dict(self):
        data_dict = super(VirtNUMATopologyCellLimit, self)._to_dict()
        data_dict['mem']['limit'] = self.memory_limit
        data_dict['cpu_limit'] = self.cpu_limit
        return data_dict

    @classmethod
    def _from_dict(cls, data_dict):
        cpuset = parse_cpu_spec(data_dict.get('cpus', ''))
        memory = data_dict.get('mem', {}).get('total', 0)
        cpu_limit = data_dict.get('cpu_limit', len(cpuset))
        memory_limit = data_dict.get('mem', {}).get('limit', memory)
        cell_id = data_dict.get('id')
        return cls(cell_id, cpuset, memory, cpu_limit, memory_limit)


class VirtNUMATopologyCellUsage(VirtNUMATopologyCell):
    """Class for reporting NUMA resources and usage in a cell

    The VirtNUMATopologyCellUsage class specializes
    VirtNUMATopologyCell to include information about the
    utilization of hardware resources in a NUMA cell.
    """

    def __init__(self, id, cpuset, memory, cpu_usage=0, memory_usage=0):
        """Create a new NUMA Cell with usage

        :param id: integer identifier of cell
        :param cpuset: set containing list of CPU indexes
        :param memory: RAM measured in KiB
        :param cpu_usage: number of  CPUs allocated
        :param memory_usage: RAM allocated in KiB

        Creates a new NUMA cell object to record the hardware
        resources and utilization. The number of CPUs specified
        by the @cpu_usage parameter may be larger than the number
        of bits set in @cpuset if CPU overcommit is used. Likewise
        the amount of RAM specified by the @memory_usage parameter
        may be larger than the available RAM in @memory if RAM
        overcommit is used.

        :returns: a new NUMA cell object
        """

        super(VirtNUMATopologyCellUsage, self).__init__(
            id, cpuset, memory)

        self.cpu_usage = cpu_usage
        self.memory_usage = memory_usage

    def _to_dict(self):
        data_dict = super(VirtNUMATopologyCellUsage, self)._to_dict()
        data_dict['mem']['used'] = self.memory_usage
        data_dict['cpu_usage'] = self.cpu_usage
        return data_dict

    @classmethod
    def _from_dict(cls, data_dict):
        cpuset = parse_cpu_spec(data_dict.get('cpus', ''))
        cpu_usage = data_dict.get('cpu_usage', 0)
        memory = data_dict.get('mem', {}).get('total', 0)
        memory_usage = data_dict.get('mem', {}).get('used', 0)
        cell_id = data_dict.get('id')
        return cls(cell_id, cpuset, memory, cpu_usage, memory_usage)


class VirtNUMATopology(object):
    """Base class for tracking NUMA topology information

    The VirtNUMATopology class represents the NUMA hardware
    topology for memory and CPUs in any machine. It is
    later specialized for handling either guest instance
    or compute host NUMA topology.
    """

    def __init__(self, cells=None):
        """Create a new NUMA topology object

        :param cells: list of VirtNUMATopologyCell instances

        """

        super(VirtNUMATopology, self).__init__()

        self.cells = cells or []

    def __len__(self):
        """Defined so that boolean testing works the same as for lists."""
        return len(self.cells)

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, str(self._to_dict()))

    def _to_dict(self):
        return {'cells': [cell._to_dict() for cell in self.cells]}

    @classmethod
    def _from_dict(cls, data_dict):
        return cls(cells=[cls.cell_class._from_dict(cell_dict)
                          for cell_dict in data_dict.get('cells', [])])

    def to_json(self):
        return jsonutils.dumps(self._to_dict())

    @classmethod
    def from_json(cls, json_string):
        return cls._from_dict(jsonutils.loads(json_string))


class VirtNUMAInstanceTopology(VirtNUMATopology):
    """Class to represent the topology configured for a guest
    instance. It provides helper APIs to determine configuration
    from the metadata specified against the flavour and or
    disk image
    """

    cell_class = VirtNUMATopologyCell

    @staticmethod
    def _get_flavor_or_image_prop(flavor, image_meta, propname):
        flavor_val = flavor.get('extra_specs', {}).get("hw:" + propname)
        image_val = image_meta.get("hw_" + propname)

        if flavor_val is not None:
            if image_val is not None:
                raise exception.ImageNUMATopologyForbidden(
                    name='hw_' + propname)

            return flavor_val
        else:
            return image_val

    @classmethod
    def _get_constraints_manual(cls, nodes, flavor, image_meta):
        cells = []
        totalmem = 0

        availcpus = set(range(flavor['vcpus']))

        for node in range(nodes):
            cpus = cls._get_flavor_or_image_prop(
                flavor, image_meta, "numa_cpus.%d" % node)
            mem = cls._get_flavor_or_image_prop(
                flavor, image_meta, "numa_mem.%d" % node)

            # We're expecting both properties set, so
            # raise an error if either is missing
            if cpus is None or mem is None:
                raise exception.ImageNUMATopologyIncomplete()

            mem = int(mem)
            cpuset = parse_cpu_spec(cpus)

            for cpu in cpuset:
                if cpu > (flavor['vcpus'] - 1):
                    raise exception.ImageNUMATopologyCPUOutOfRange(
                        cpunum=cpu, cpumax=(flavor['vcpus'] - 1))

                if cpu not in availcpus:
                    raise exception.ImageNUMATopologyCPUDuplicates(
                        cpunum=cpu)

                availcpus.remove(cpu)

            cells.append(VirtNUMATopologyCell(node, cpuset, mem))
            totalmem = totalmem + mem

        if availcpus:
            raise exception.ImageNUMATopologyCPUsUnassigned(
                cpuset=str(availcpus))

        if totalmem != flavor['memory_mb']:
            raise exception.ImageNUMATopologyMemoryOutOfRange(
                memsize=totalmem,
                memtotal=flavor['memory_mb'])

        return cls(cells)

    @classmethod
    def _get_constraints_auto(cls, nodes, flavor, image_meta):
        if ((flavor['vcpus'] % nodes) > 0 or
            (flavor['memory_mb'] % nodes) > 0):
            raise exception.ImageNUMATopologyAsymmetric()

        cells = []
        for node in range(nodes):
            cpus = cls._get_flavor_or_image_prop(
                flavor, image_meta, "numa_cpus.%d" % node)
            mem = cls._get_flavor_or_image_prop(
                flavor, image_meta, "numa_mem.%d" % node)

            # We're not expecting any properties set, so
            # raise an error if there are any
            if cpus is not None or mem is not None:
                raise exception.ImageNUMATopologyIncomplete()

            ncpus = int(flavor['vcpus'] / nodes)
            mem = int(flavor['memory_mb'] / nodes)
            start = node * ncpus
            cpuset = set(range(start, start + ncpus))

            cells.append(VirtNUMATopologyCell(node, cpuset, mem))

        return cls(cells)

    @classmethod
    def get_constraints(cls, flavor, image_meta):
        nodes = cls._get_flavor_or_image_prop(
            flavor, image_meta, "numa_nodes")

        if nodes is None:
            return None

        nodes = int(nodes)

        # We'll pick what path to go down based on whether
        # anything is set for the first node. Both paths
        # have logic to cope with inconsistent property usage
        auto = cls._get_flavor_or_image_prop(
            flavor, image_meta, "numa_cpus.0") is None

        if auto:
            return cls._get_constraints_auto(
                nodes, flavor, image_meta)
        else:
            return cls._get_constraints_manual(
                nodes, flavor, image_meta)


class VirtNUMALimitTopology(VirtNUMATopology):
    """Class to represent the max resources of a compute node used
    for checking oversubscription limits.
    """

    cell_class = VirtNUMATopologyCellLimit


class VirtNUMAHostTopology(VirtNUMATopology):

    """Class represents the NUMA configuration and utilization
    of a compute node. As well as exposing the overall topology
    it tracks the utilization of the resources by guest instances
    """

    cell_class = VirtNUMATopologyCellUsage

    @staticmethod
    def can_fit_instances(host, instances):
        """Test if the instance topology can fit into the host

        Returns True if all the cells of the all the instance topologies in
        'instances' exist in the given 'host' topology. False otherwise.
        """
        if not host:
            return True

        host_cells = set(cell.id for cell in host.cells)
        instances_cells = [set(cell.id for cell in instance.cells)
                            for instance in instances]
        return all(instance_cells <= host_cells
                    for instance_cells in instances_cells)

    @classmethod
    def usage_from_instances(cls, host, instances, free=False):
        """Get host topology usage

        :param host: VirtNUMAHostTopology with usage information
        :param instances: list of VirtNUMAInstanceTopology
        :param free: If True usage of the host will be decreased

        Sum the usage from all @instances to report the overall
        host topology usage

        :returns: VirtNUMAHostTopology including usage information
        """

        if host is None:
            return

        instances = instances or []
        cells = []
        sign = -1 if free else 1
        for hostcell in host.cells:
            memory_usage = hostcell.memory_usage
            cpu_usage = hostcell.cpu_usage
            for instance in instances:
                for instancecell in instance.cells:
                    if instancecell.id == hostcell.id:
                        memory_usage = (
                                memory_usage + sign * instancecell.memory)
                        cpu_usage = cpu_usage + sign * len(instancecell.cpuset)

            cell = cls.cell_class(
                hostcell.id, hostcell.cpuset, hostcell.memory,
                max(0, cpu_usage), max(0, memory_usage))

            cells.append(cell)

        return cls(cells)

    @classmethod
    def claim_test(cls, host, instances, limits=None):
        """Test if we can claim an instance on the host with given limits.

        :param host: VirtNUMAHostTopology with usage information
        :param instances: list of VirtNUMAInstanceTopology
        :param limits: VirtNUMALimitTopology with max values set. Should
                       match the host topology otherwise

        :returns: None if the claim succeeds or text explaining the error.
        """
        if not (host and instances):
            return

        if not cls.can_fit_instances(host, instances):
            return (_("Requested instance NUMA topology cannot fit "
                      "the given host NUMA topology."))

        if not limits:
            return

        claimed_host = cls.usage_from_instances(host, instances)

        for claimed_cell, limit_cell in zip(claimed_host.cells, limits.cells):
            if (claimed_cell.memory_usage > limit_cell.memory_limit or
                    claimed_cell.cpu_usage > limit_cell.cpu_limit):
                return (_("Requested instance NUMA topology is too large for "
                          "the given host NUMA topology limits."))


class VirtInstanceCPUPinningCell(object):
    def __init__(self, cpuset, id=None, pinning=None, topology=None):
        """Create an object containing instance NUMA cell CPU pinning data

        :param cpuset: A set of vCPU ids of the instance
        :param id: Cell id when pinned to a host, or None if instance has
                   not yet been pinned
        :param pinning: A dictionary mapping instance vCPU id to the host pCPU
                        it is pinned to
        :param topology: A VirtCPUTopology instance

        We will use topology to figure out siblings
        """
        self.id = id
        pinning = pinning or {}
        self.topology = topology or None

        num_threads = 1
        if self.topology:
            num_threads = self.topology.threads
        if num_threads != 0 and len(cpuset) % num_threads != 0:
            raise exception.CPUPinningIllegalTopology(threads=num_threads,
                                                      cpuset=cpuset)

        self.pinning = dict(zip(cpuset, itertools.repeat(None)))
        self.pinning.update(dict((cpu, pin) for cpu, pin in pinning.items()
                                 if cpu in cpuset))

    def __len__(self):
        return len(self.pinning)

    def _to_dict(self):
        topo = None
        if self.topology:
            topo = {'sock': self.topology.sockets,
                    'core': self.topology.cores,
                    'th': self.topology.threads}
        return {'id': self.id, 'pin': self.pinning, 'topo': topo}

    @classmethod
    def _from_dict(cls, data):
        topology = data.get('topo') or None
        pinning = data.get('pin') or {}
        if topology:
            topology = VirtCPUTopology(sockets=topology['sock'],
                                       cores=topology['core'],
                                       threads=topology['th'])
        return cls(set(pinning.keys()), id=data.get('id'),
                   pinning=pinning, topology=topology)

    @property
    def cpuset(self):
        return set(self.pinning.keys())

    @property
    def siblings(self):
        cpu_list = sorted(list(self.cpuset))

        threads = 0
        if self.topology:
            threads = self.topology.threads
        if threads == 1:
            threads = 0

        return map(set, zip(*[iter(cpu_list)] * threads))


class VirtInstanceCPUPinning(VirtNUMATopology):

    cell_class = VirtInstanceCPUPinningCell


class VirtHostCPUPinningCell(object):
    def __init__(self, id, cpuset, pinning=None, siblings=None):
        """Create an object containing host NUMA cell CPU pinning data

        :param id: NUMA cell id
        :param cpuset: A set of pCPU ids of the host
        :param pinning: A set of "taken" pCPU ids that have instance
                        vCPUs pinned to them
        :param siblings: A list of sets of pCPU ids each representing a set
                         of sibling cores
        """
        self.id = id
        pinning = pinning or set()

        self.pinning = dict(zip(cpuset, itertools.repeat(False)))
        self.pinning.update(dict((cpu, True) for cpu in pinning
                            if cpu in cpuset))
        self.siblings = siblings or []

    def _to_dict(self):
        return {'id': self.id,
                'cpuset': format_cpu_spec(self.cpuset, allow_ranges=False),
                'sib': [format_cpu_spec(sib, allow_ranges=False)
                        for sib in self.siblings or []] or None,
                'pin': format_cpu_spec(self.pinned_cpus, allow_ranges=False)}

    @classmethod
    def _from_dict(cls, data):
        pinning = data.get('pin') or ''
        siblings = data.get('sib') or None
        if siblings:
            siblings = [parse_cpu_spec(sib) for sib in siblings]
        return cls(data['id'], parse_cpu_spec(data.get('cpuset', '')),
                   pinning=parse_cpu_spec(pinning) or None,
                   siblings=siblings)

    @property
    def cpuset(self):
        return set(self.pinning.keys())

    @property
    def pinned_cpus(self):
        return set(cpu_id for cpu_id, pinned in self.pinning.items() if pinned)

    @property
    def free_cpus(self):
        return set(cpu_id for cpu_id, pinned in self.pinning.items()
                   if not pinned)

    @staticmethod
    def _can_pack_instance(instance_pinning, threads_per_core, cores_list):
        if threads_per_core * len(cores_list) < len(instance_pinning):
            return False
        if instance_pinning.siblings:
            return instance_pinning.topology.threads <= threads_per_core
        else:
            return len(instance_pinning) % threads_per_core == 0

    @staticmethod
    def _pack_instance_onto_cores(available_siblings, instance_pinning,
                                  cell_id):
        """Pack an instance onto a set of siblings

        :param available_siblings: list of sets of CPU id's - available
                                   siblings per core
        :param instance_pinning: An instance of VirtInstanceCPUPinning
                                 describing the pinning requirements of the
                                 instance

        :returns: An instance of VirtInstanceCPUPinning containing the pinning
                  information, and potentially a new topology to be exposed to
                  the instance. None if there is no valid way to satisfy the
                  sibling requirements for the instance.

        This method will calculate the pinning for the given instance and it's
        topology, making sure that hyperthreads of the instance match up with
        those of the host when the pinning takes effect.
        """

        # We build up a data structure 'can_pack' that answers the question:
        # 'Given the number of threads I want to pack, give me a list of all
        # the available sibling sets that can accomodate it'
        can_pack = collections.defaultdict(list)
        for sib in available_siblings:
            for threads_no in range(1, len(sib) + 1):
                can_pack[threads_no].append(sib)

        # We iterate over the can_pack dict in descending order of cores that
        # can be packed - an attempt to get even distribution over time
        for cores_per_sib, sib_list in sorted(
                (t for t in can_pack.items()), reverse=True):
            if VirtHostCPUPinningCell._can_pack_instance(
                    instance_pinning, cores_per_sib, sib_list):
                sliced_sibs = map(lambda s: list(s)[:cores_per_sib], sib_list)
                if instance_pinning.siblings:
                    pinning = dict(
                            zip(itertools.chain(*instance_pinning.siblings),
                                itertools.chain(*sliced_sibs)))
                else:
                    pinning = dict(zip(sorted(instance_pinning.cpuset),
                                       itertools.chain(*sliced_sibs)))

                topology = (instance_pinning.topology or
                    VirtCPUTopology(sockets=1,
                                    cores=len(sliced_sibs),
                                    threads=cores_per_sib))
                return VirtInstanceCPUPinningCell(
                    instance_pinning.cpuset, id=cell_id, pinning=pinning,
                    topology=topology)

    @classmethod
    def get_pinning_for_cell(cls, host_pinning, instance_pinning):
        """Figure out if cells can be pinned to a host cell and return details

        :param host_pinning: VirtHostCPUPinning instance - the host info that
                             the isntance should be pinned to
        :param instance_pinning: VirtInstanceCPUPinning instance - instance
                                 info, without any pinning information

        :returns: VirtInstanceCPUPinning instance with pinning information, or
                  None if instance cannot be pinned to the given host
        """
        # If we do not have enough CPUs available - bail early
        if len(host_pinning.free_cpus) < len(instance_pinning):
            return

        # There is hyperthreading enabled on the host so we want to make sure
        # we expose that to the guest
        if host_pinning.siblings:
            available_siblings = [sibling_set & host_pinning.free_cpus
                                  for sibling_set in host_pinning.siblings]
            # Instance requires hyperthreading in it's topology - so we need to
            # pack it
            if instance_pinning.topology and instance_pinning.siblings:
                return cls._pack_instance_onto_cores(
                        available_siblings, instance_pinning, host_pinning.id)
            # If it does not and the host has hyperthreading - we have to
            # expose it
            else:
                largest_free_sibling_set = sorted(
                        available_siblings, key=len)[-1]
                # We can pack the instance onto a single core
                if (len(instance_pinning.cpuset) <=
                        len(largest_free_sibling_set)):
                    topology = instance_pinning.topology or VirtCPUTopology(
                            sockets=1, cores=1, threads=len(instance_pinning))
                    return VirtInstanceCPUPinningCell(
                        instance_pinning.cpuset.copy(),
                        id=host_pinning.id,
                        pinning=dict(
                            zip(sorted(instance_pinning.cpuset),
                                largest_free_sibling_set)),
                        topology=topology)
                # We can't so we need to pack it anyway and update the topology
                else:
                    return VirtHostCPUPinningCell._pack_instance_onto_cores(
                            available_siblings, instance_pinning,
                            host_pinning.id)
        else:
            # Straightforward to pin to available cpus when there is no
            # hyperthreading on the host
            to_pin = sorted(host_pinning.free_cpus)[:len(instance_pinning)]
            return VirtInstanceCPUPinningCell(
                        instance_pinning.cpuset,
                        id=host_pinning.id,
                        pinning=dict(
                            zip(sorted(instance_pinning.cpuset), to_pin)),
                        topology=instance_pinning.topology)

    @classmethod
    def usage_from_instance_cell(cls, host_cell, instance_cell, free=False):
        """Get the pinning info of 'host_cell' after pinning 'instance_cell' to
        it's pCPUs.

        :param host: VirtHostCPUPinning instance - host pinning info prior to
                     attemtping to pin instance vCPUs to it's pCPUs
        :param instance: VirtInstanceCPUPinning instance with pinning info,
                         most likely generated by get_pinning_for_instance

        :returns: A new instance of VirtHostCPUPinning with pinning info
                  updated with pinning from the instance or
        :raises: CPUPinningInvalidInstanceUsage if the instance cannot be
                 pinned to the host due to lack of free CPUs or topology
                 mismatch
        """
        host_pinning = host_cell.pinning.copy()
        marker = not free
        for _cpu, pin in instance_cell.pinning.items():
            if host_pinning[pin] == marker:
                raise exception.CPUPinningInvalidInstanceUsage()
            host_pinning[pin] = marker
        return cls(host_cell.id, host_cell.cpuset,
                   pinning=set(cpu for cpu, pinned in host_pinning.items()
                               if pinned),
                   siblings=host_cell.siblings)


class VirtHostCPUPinning(VirtNUMATopology):

    cell_class = VirtHostCPUPinningCell

    @classmethod
    def get_pinning_for_instance(cls, host, instance_pinning):
        if (not (host and instance_pinning) or
                len(host) < len(instance_pinning)):
            return
        else:
            for host_cell_perm in itertools.permutations(
                    host.cells, len(instance_pinning)):
                cells = []
                for host_cell, instance_cell in zip(
                        host_cell_perm, instance_pinning.cells):
                    got_pinning = cls.cell_class.get_pinning_for_cell(
                                host_cell, instance_cell)
                    if got_pinning is None:
                        break
                    cells.append(got_pinning)
                if len(cells) == len(host_cell_perm):
                    return VirtInstanceCPUPinning(cells=cells)

    @classmethod
    def usage_from_instances(cls, host, instances_pinning, free=False):
        if not host:
            return
        instances_pinning = instances_pinning or []
        cells = []
        for host_cell in host.cells:
            used_host_cell = copy.deepcopy(host_cell)
            for instance_pinning in instances_pinning:
                for instance_cell in instance_pinning.cells:
                    if instance_cell.id == host_cell.id:
                        got_cell = cls.cell_class.usage_from_instance_cell(
                                used_host_cell, instance_cell, free=free)
                        used_host_cell = got_cell
            cells.append(used_host_cell)
        return cls(cells=cells)

    @classmethod
    def claim_test(cls, host, instances_pinning, limits=None):
        fail_msg = _("Instances cannot be pinned to this host.")
        if not host:
            return fail_msg
        elif not instances_pinning:
            return
        else:
            for instance_pinning in instances_pinning:
                got_pinning = cls.get_pinning_for_instance(
                        host, instance_pinning)
                if got_pinning is None:
                    return fail_msg
                host = cls.usage_from_instances(host, [got_pinning])


# TODO(ndipanov): Remove when all code paths are using objects
def instance_topology_from_instance(instance):
    """Convenience method for getting the numa_topology out of instances

    Since we may get an Instance as either a dict, a db object, or an actual
    Instance object, this makes sure we get beck either None, or an instance
    of objects.InstanceNUMATopology class.
    """
    if isinstance(instance, objects.Instance):
        # NOTE (ndipanov): This may cause a lazy-load of the attribute
        instance_numa_topology = instance.numa_topology
    else:
        if 'numa_topology' in instance:
            instance_numa_topology = instance['numa_topology']
        elif 'uuid' in instance:
            try:
                instance_numa_topology = (
                    objects.InstanceNUMATopology.get_by_instance_uuid(
                            context.get_admin_context(), instance['uuid'])
                    )
            except exception.NumaTopologyNotFound:
                instance_numa_topology = None
        else:
            instance_numa_topology = None

    if instance_numa_topology:
        if isinstance(instance_numa_topology, six.string_types):
            instance_numa_topology = VirtNUMAInstanceTopology.from_json(
                            instance_numa_topology)
        elif isinstance(instance_numa_topology, dict):
            # NOTE (ndipanov): A horrible hack so that we can use this in the
            # scheduler, since the InstanceNUMATopology object is serialized
            # raw using the obj_base.obj_to_primitive, (which is buggy and will
            # give us a dict with a list of InstanceNUMACell objects), and then
            # passed to jsonutils.to_primitive, which will make a dict out of
            # those objects. All of this is done by
            # scheduler.utils.build_request_spec called in the conductor.
            #
            # Remove when request_spec is a proper object itself!
            dict_cells = instance_numa_topology.get('cells')
            if dict_cells:
                cells = [objects.InstanceNUMACell(id=cell['id'],
                                                  cpuset=set(cell['cpuset']),
                                                  memory=cell['memory'])
                         for cell in dict_cells]
                instance_numa_topology = (
                        objects.InstanceNUMATopology(cells=cells))

    return instance_numa_topology


# TODO(ndipanov): Remove when all code paths are using objects
def host_topology_and_format_from_host(host):
    """Convenience method for getting the numa_topology out of hosts

    Since we may get a host as either a dict, a db object, or an actual
    ComputeNode object, or an instance of HostState class, this makes sure we
    get beck either None, or an instance of VirtNUMAHostTopology class.

    :returns: A two-tuple, first element is the topology itself or None, second
              is a boolean set to True if topology was in json format.
    """
    was_json = False
    try:
        host_numa_topology = host.get('numa_topology')
    except AttributeError:
        host_numa_topology = host.numa_topology

    if host_numa_topology is not None and isinstance(
            host_numa_topology, six.string_types):
        was_json = True
        host_numa_topology = VirtNUMAHostTopology.from_json(host_numa_topology)

    return host_numa_topology, was_json


# TODO(ndipanov): Remove when all code paths are using objects
def get_host_numa_usage_from_instance(host, instance, free=False,
                                     never_serialize_result=False):
    """Calculate new 'numa_usage' of 'host' from 'instance' NUMA usage

    This is a convenience method to help us handle the fact that we use several
    different types throughout the code (ComputeNode and Instance objects,
    dicts, scheduler HostState) which may have both json and deserialized
    versions of VirtNUMATopology classes.

    Handles all the complexity without polluting the class method with it.

    :param host: nova.objects.ComputeNode instance, or a db object or dict
    :param instance: nova.objects.Instance instance, or a db object or dict
    :param free: if True the the returned topology will have it's usage
                 decreased instead.
    :param never_serialize_result: if True result will always be an instance of
                                   VirtNUMAHostTopology class.

    :returns: numa_usage in the format it was on the host or
              VirtNUMAHostTopology instance if never_serialize_result was True
    """
    instance_numa_topology = instance_topology_from_instance(instance)
    if instance_numa_topology:
        instance_numa_topology = [instance_numa_topology]

    host_numa_topology, jsonify_result = host_topology_and_format_from_host(
            host)

    updated_numa_topology = (
        VirtNUMAHostTopology.usage_from_instances(
                host_numa_topology, instance_numa_topology, free=free))

    if updated_numa_topology is not None:
        if jsonify_result and not never_serialize_result:
            updated_numa_topology = updated_numa_topology.to_json()

    return updated_numa_topology
