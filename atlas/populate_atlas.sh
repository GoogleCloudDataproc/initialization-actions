#!/usr/bin/expect

set -euxo pipefail

readonly USERNAME=$0
readonly PASSWORD=$1

spawn /usr/lib/atlas/apache-atlas-1.2.0/quick_start.py
expect -exact "Enter USERNAME for atlas :-"
send -- "${USERNAME}\n"
expect -exact "Enter PASSWORD for atlas :-"
send -- "${PASSWORD}\n"
wait
