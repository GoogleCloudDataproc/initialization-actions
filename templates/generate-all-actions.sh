#!/bin/bash

for tt in $(find templates -name '*.sh.in') ; do
  genfile=`perl -e "print( q{${tt}} =~ m:templates/(.*?.sh).in: )"`
  mkdir -p /tmp/init/$(dirname $genfile)
  perl templates/generate-action.pl "${genfile}" | tee "${genfile}" > "/tmp/init/${genfile}"
done
