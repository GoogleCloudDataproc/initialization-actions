"""verify_solr.py: this script runs tests on node running Solr

Test logic:
Solr performs indexing on data in collections(cores in SolrStandalone mode). This test:
1. creates collection with sharding and replication factor set to 2
2. uses Solr API to update collection schema
3. uses Solr API once again to create catch all rule
4. posts movie data(json file) downloaded from Solr examples to Solr server
5. runs basic query on collection and validates output.
"""

import json
import subprocess

SOLR_DIR = '/usr/lib/solr'
SOLR_URL = 'http://localhost:8983/solr'
SOLR_EXAMPLE_DOC = 'https://raw.githubusercontent.com/apache/lucene-solr/master/solr/example/films/films.json'
SOLR_COLLECTION_NAME = 'films'


def run_command(command):
    p = subprocess.Popen(
        command,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = p.communicate()
    stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
    return p.returncode, stdout, stderr


def create_core_collection():
    cmd = "sudo runuser -l solr -s /bin/bash -c " \
          "'{}/bin/solr create -c {} -s 2 -rf 2'".format(SOLR_DIR,
                                                         SOLR_COLLECTION_NAME)
    ret_code, stdout, stderr = run_command(cmd)
    assert ret_code == 0, "Failed to create core or collection." \
                          " Error: {}".format(stderr)


def use_api_to_update_schema():
    in_json = '{"add-field": {"name": "name", "type": "text_general", ' \
        '"multiValued": false, "stored": true}}'
    ret_code, stdout, stderr = run_command(
        "curl -X POST -H 'Content-type:application/json' --data-binary '{}' "
        "{}/{}/schema".format(in_json, SOLR_URL, SOLR_COLLECTION_NAME))
    assert ret_code == 0, "Failed to update schema using API." \
                          " Error: {}".format(stderr)


def use_api_to_create_catch_all_rule():
    in_json = '{"add-copy-field": {"source": "*", "dest": "_text_"}}'
    ret_code, stdout, stderr = run_command(
        "curl -X POST -H 'Content-type:application/json' --data-binary '{}' "
        "{}/{}/schema".format(in_json, SOLR_URL, SOLR_COLLECTION_NAME))
    assert ret_code == 0, "Failed to create rule using API." \
                          " Error: {}".format(stderr)


def post_test_data():
    ret_code, stdout, stderr = run_command(
        "wget -nv --timeout=30 --tries=5 --retry-connrefused {} -O /tmp/films.json"
        .format(SOLR_EXAMPLE_DOC))
    assert ret_code == 0, "Failed to get test data. Error: {}".format(stderr)

    ret_code, stdout, stderr = run_command(
        "sudo runuser -l solr -s /bin/bash -c "
        "'{}/bin/post -c {} /tmp/films.json'".format(SOLR_DIR,
                                                     SOLR_COLLECTION_NAME))
    assert ret_code == 0, "Failed to post data. Error: {}".format(stderr)


def run_test_query():
    ret_code, stdout, stderr = run_command(
        "curl --silent -H 'Accept:application/json' "
        "'{}/{}/query?q=Comedy&rows=0'".format(SOLR_URL, SOLR_COLLECTION_NAME))
    assert ret_code == 0, "Failed to query solr using API." \
                          " Error: {}".format(stderr)
    out_json = json.loads(stdout)
    assert out_json['response']['numFound'] == 417, \
        "Failed to get right number of matches. Got: {}".format(stdout)


def main():
    create_core_collection()
    use_api_to_update_schema()
    use_api_to_create_catch_all_rule()
    post_test_data()
    run_test_query()


if __name__ == '__main__':
    main()
