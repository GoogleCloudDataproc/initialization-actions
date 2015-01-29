#!/usr/bin/python
"""Utility for converting Ambari recommendations into blueprints.

Will be replaced by Ambari functionality when added.
"""

import argparse
import json


DEFAULT_PASSWORD = 'admin'


def _merge_configurations(recommended_configurations, custom_configurations):
  """Integrate Ambari's recommendation with user specified configuration."""
  for name, configuration in recommended_configurations.iteritems():
    properties = configuration['properties']
    properties.update(custom_configurations.get(name, {}))
    custom_configurations[name] = properties
  blueprint_format = [{k: v} for k, v in custom_configurations.iteritems()]
  return blueprint_format


def _fix_components(blueprint):
  """Remove ZKFC move MYSQL_SERVER by HIVE_METASTORE."""
  # TODO(user): Determine why this is an issue
  problem_components = ['ZKFC', 'MYSQL_SERVER']
  for host_group in blueprint['host_groups']:
    components = host_group['components']
    for problem_component in problem_components:
      entry = {'name': problem_component}
      if entry in components:
        components.remove(entry)
    if {'name': 'HIVE_METASTORE'} in components:
      components.append({'name': 'MYSQL_SERVER'})


def create_blueprint(
    configuration_recommendation,
    host_group_recommendation,
    custom_configurations):
  """Create Ambari blueprint."""
  blueprint = {}
  recommended_configurations = configuration_recommendation['resources'][0][
      'recommendations']['blueprint']['configurations']
  recommended_host_groups = host_group_recommendation['resources'][0][
      'recommendations']['blueprint']['host_groups']
  blueprint['configurations'] = _merge_configurations(
      recommended_configurations, custom_configurations['configurations'])
  blueprint['host_groups'] = recommended_host_groups
  blueprint['Blueprints'] = configuration_recommendation['resources'][0][
      'Versions']
  _fix_components(blueprint)
  return blueprint


def create_cluster_template(host_group_recommendation, blueprint_name):
  """Create Ambari cluster template."""
  cluster_template = {}
  cluster_template['blueprint'] = blueprint_name
  cluster_template['default_password'] = DEFAULT_PASSWORD
  recommended_template = host_group_recommendation['resources'][0][
      'recommendations']['blueprint_cluster_binding']
  cluster_template.update(recommended_template)
  return cluster_template


def create_blueprints(
    conf_recommendation_file,
    host_recommendation_file,
    blueprint_file_name,
    cluster_template_file_name,
    blueprint_name,
    custom_configuraton_file=None):
  """Create and dump Ambari blueprint cluster template."""
  configuration_recommendation = json.load(conf_recommendation_file)
  host_group_recommendation = json.load(host_recommendation_file)
  if custom_configuraton_file:
    custom_configurations = json.load(custom_configuraton_file)
  else:
    custom_configurations = {'configurations': {}}

  blueprint = create_blueprint(
      configuration_recommendation,
      host_group_recommendation,
      custom_configurations)

  cluster_template = create_cluster_template(
      host_group_recommendation, blueprint_name)

  with open(blueprint_file_name, 'w') as blueprint_file:
    json.dump(blueprint, blueprint_file, indent=2)
  with open(cluster_template_file_name, 'w') as cluster_template_file:
    json.dump(cluster_template, cluster_template_file, indent=2)


def parse_args():
  """Parse inputs to script."""
  parser = argparse.ArgumentParser()
  parser.description = __doc__
  parser.add_argument('--conf_recommendation', type=file, required=True)
  parser.add_argument('--host_recommendation', type=file, required=True)
  parser.add_argument('--blueprint', required=True)
  parser.add_argument('--cluster_template', required=True)
  parser.add_argument('--blueprint_name', required=True)
  parser.add_argument('--custom_configuraton', type=file, required=False)
  return parser.parse_args()


def main():
  args = parse_args()
  create_blueprints(
      args.conf_recommendation,
      args.host_recommendation,
      args.blueprint,
      args.cluster_template,
      args.blueprint_name,
      args.custom_configuraton)


if __name__ == '__main__':
  main()
