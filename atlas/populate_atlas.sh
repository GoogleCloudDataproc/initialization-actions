#!/usr/bin/expect

set -euxo pipefail
set timeout 180
set USERNAME [lindex $argv 0]
set PASSWORD [lindex $argv 1]
log_user 1

if {[llength $argv] == 0} {
  send_user "Usage: populate_atlas.sh username \'password\' \n"
  exit 1
}

spawn python2 /usr/lib/atlas/bin/quick_start.py
expect -exact "Enter username for atlas :-"
send -- "$USERNAME\n"
expect -exact "Enter password for atlas :-"
send -- "$PASSWORD\n"
expect {
  -exact "No sample data added to Apache Atlas Server." {
    send_user "Failed\n"
    exit 1
  }
  -exact "Sample data added to Apache Atlas Server." {
    send_user "Success\n"
    exit 0
  }
  timeout {
    send_user "\nFailed to populate Atlas\n"
    exit 1
  }
}