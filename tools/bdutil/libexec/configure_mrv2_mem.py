#!/usr/bin/env python

"""Calculator utility to calculate the memory for YARN and Hadoop MapReduce v2.

Fills a text file with the bash invocation to export the following environment
variables:
  YARN_MIN_MEM_MB- The minimum container size for the node in MB.
  YARN_MAX_MEM_MB - The maximum container size for the node in MB.
  NODEMANAGER_MEM_MB - The total ammount of memory for containers on the node in
      MB.
  For container types MAP, REDUCE, APP_MASTER:
    <TYPE>_MEM_MB - The default size of the <TYPE> container in MB.
    <TYPE>_JAVA_OPTS - The java options to be used with JVMs for <TYPE>.
    CORES_PER_<TYPE>_ROUNDED - The integral value of a number of virtual cores
        for <TYPE> containers.
"""

import argparse
import math
import sys


def _round_down(rough_value, increment, minimum=None, maximum=None):
  """Utility method for rounding a value down to an increment.

  Args:
    rough_value: A float. The initial value to be rounded.
    increment: The increment to round down to.
    minimum: Optional minimum value, default is increment.
    maximum: Optional maximum value, default is the max int.

  Returns:
    An integer formed by rounding rough_value down to the nearest positive
    number of increments while staying between minimum and maximum.
  """
  if not minimum:
    minimum = increment
  if not maximum:
    maximum = sys.maxint
  rounded_value = rough_value - (rough_value % increment)
  return int(min(maximum, max(minimum, rounded_value)))


def _get_export_line(env_var, value):
  """Create a line of text to set an environment variable in bash.

  Args:
    env_var: The name of the environment variable to set
    value: The value to set env_var to.

  Returns:
    A string which sets env_var to value in bash.
  """
  line = "export %s='%s'" % (env_var, value)
  return line


class MapReduceV2ResourceCalculator(object):
  """Utility class for calculating Hadoop MapReduce V2 and YARN memory."""

  # The fraction of YARN container memory to give to the container JVM for heap.
  HEAP_RATIO = 0.8

  def __init__(
      self,
      total_memory,
      available_memory_ratio,
      total_cores,
      cores_per_map,
      cores_per_reduce,
      cores_per_app_master):
    """Constructor which calculates resource requirements for all containers.

    Args:
      total_memory: The total memory on the node in MB.
      available_memory_ratio: The ratio of memory YARN is allowed to use.
      total_cores: The number of virtual cores on the node.
      cores_per_map: A float corresponding to the approximate number of virtual
          cores to use per map container.
      cores_per_reduce: A float corresponding to the approximate number of
          virtual cores to use per reduce container.
      cores_per_app_master: A float corresponding to the approximate number of
          virtual cores to use per MapReduce AppMaster container.

    Raises:
      ValueError: If machine has insufficent memory for one container.
    """
    available_memory = int(total_memory * available_memory_ratio)
    # Set minimum container size to a power of 2, which is approximately 1/8th
    # of the available memory between 256MB and 2048MB
    self.minimum_memory = 2 ** _round_down(
        int(math.log(available_memory / 8, 2)),
        1,
        minimum=8,
        maximum=11)
    if available_memory < self.minimum_memory:
      raise ValueError('Insufficient memory on machine.')
    self.maximum_memory = _round_down(available_memory, self.minimum_memory)
    self.total_cores = total_cores
    self.container_properties = {
        'map': self.calculate_container_properties(cores_per_map),
        'reduce': self.calculate_container_properties(cores_per_reduce),
        'app_master': self.calculate_container_properties(cores_per_app_master)
    }

    # Avoid bug where YARN cannot preempt reducers, because there is enough
    # memory across all nodes to schedule a mapper, but not on any single node.
    excess_memory = self.maximum_memory % self.container_properties[
        'reduce']['memory']
    excess_memory %= self.container_properties['map']['memory']
    self.maximum_memory -= excess_memory

  def calculate_container_properties(self, rough_cores):
    """Calculates resource requirements for a MRV2 YARN container.

    Args:
      rough_cores: A float corresponding to the approximate number of cores
          that should be used for the container.

    Returns:
      A dict containing the calculated proerties.
    """
    container_memory = self.convert_cores_to_memory(rough_cores)
    container_cores = _round_down(rough_cores, 1, maximum=self.total_cores)
    container_heap_size = int(
        MapReduceV2ResourceCalculator.HEAP_RATIO * container_memory)
    container_properties = {
        'memory': container_memory,
        'cores': container_cores,
        'heap_size': container_heap_size
    }
    return container_properties

  def convert_cores_to_memory(self, rough_cores):
    """Calculate the amount of memory for a container given a number of cores.

    Args:
      rough_cores: A float corresponding to the approximate number of cores that
          should be used for the container.

    Returns:
      The ammount of memory in MB that the container should be allocated.
    """
    rough_memory = (self.maximum_memory * rough_cores) / self.total_cores
    rounded_memory = _round_down(rough_memory,
                                 self.minimum_memory,
                                 maximum=self.maximum_memory)
    return rounded_memory

  def build_environment_variable_file(self):
    """Builds a bash script setting the calculated environment variables.

    Returns:
      A string holding the bash script.
    """
    lines = []
    lines.append(_get_export_line('YARN_MIN_MEM_MB', self.minimum_memory))
    lines.append(_get_export_line('YARN_MAX_MEM_MB', self.maximum_memory))
    lines.append(_get_export_line('NODEMANAGER_MEM_MB', self.maximum_memory))
    for name in sorted(self.container_properties):
      properties = self.container_properties[name]
      name = name.upper()
      lines.append(_get_export_line(name + '_MEM_MB', properties['memory']))
      lines.append(
          _get_export_line('CORES_PER_%s_ROUNDED' % name, properties['cores']))
      lines.append(
          _get_export_line(name + '_JAVA_OPTS',
                           '-Xmx%dm' % properties['heap_size']))
    return '\n'.join(lines)


def parse_args():
  """Parse inputs to script."""
  parser = argparse.ArgumentParser()
  parser.description = __doc__
  parser.add_argument('--output_file', required=True)
  parser.add_argument('--total_memory', type=int, required=True)
  parser.add_argument('--available_memory_ratio', type=float, required=True)
  parser.add_argument('--total_cores', type=int, required=True)
  parser.add_argument('--cores_per_map', type=float, required=True)
  parser.add_argument('--cores_per_reduce', type=float, required=True)
  parser.add_argument('--cores_per_app_master', type=float, required=True)
  return parser.parse_args()


def main():
  args = parse_args()
  calc = MapReduceV2ResourceCalculator(
      args.total_memory,
      args.available_memory_ratio,
      args.total_cores,
      args.cores_per_map,
      args.cores_per_reduce,
      args.cores_per_app_master)
  with open(args.output_file, 'w') as env_file:
    env_file.write(calc.build_environment_variable_file())


if __name__ == '__main__':
  main()
