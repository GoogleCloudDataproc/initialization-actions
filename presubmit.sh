#!/bin/bash
set -e

git init


# Stage files to track their history
git add .
git reset -- .

git remote add origin "https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git"
git fetch origin

# Infer the files that changed
CHANGED_FILES=$(git diff origin/master --name-only)
echo $CHANGED_FILES

# Determines whether a given string is a substring of any changed file name
contains() {
	for file in $CHANGED_FILES
	do
	    if [[ $file =~ $1 ]]; then
	    	return 0
		fi
	done
	return 1
}

# Determines whether a given string is the prefix of any changed file name
contains_prefix() {
	prefix="${1}*"
	for file in $CHANGED_FILES
	do
	    if [[ $file == $prefix ]]; then
	    	return 0
		fi
	done
	return 1
}

# Run the tests of the init actions that were modified
if contains "bigtable"; then
	python bigtable/test_bigtable.py || true
fi
if contains "drill"; then
	python drill/test_drill.py || true
fi
if contains "flink"; then
	python flink/test_flink.py || true
fi
if contains "ganglia"; then
	python ganglia/test_ganglia.py || true
fi
if contains "hbase"; then
	python hbase/test_hbase.py || true
fi
if contains "hive"; then
	python hive-hcatalog/test_hive.py || true
fi
if contains "hue"; then
	python hue/test_hue.py || true
fi
if contains "kafka"; then
	python kafka/test_kafka.py || true
fi
if contains "livy"; then
	python livy/test_livy.py || true
fi
if contains "oozie"; then
	python oozie/test_oozie.py || true
fi
if contains_prefix "presto"; then
	python presto/test_presto.py || true
fi
if contains "ranger"; then
	python ranger/test_ranger.py || true
fi
if contains "solr"; then
	python solr/test_solr.py || true
fi
if contains "starburst-presto"; then
	python starburst-presto/test_presto.py || true
fi
if contains "tez"; then
	python tez/test_tez.py || true
fi
if contains "tony"; then
	python tony/test_tony.py || true
fi
