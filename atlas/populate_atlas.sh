#!/usr/bin/expect

set username [lindex $argv 0];
set password [lindex $argv 1];

spawn /etc/atlas/bin/quick_start.py
expect -exact "Enter username for atlas :-"
send -- "${username}\n"
expect -exact "Enter password for atlas :-"
send -- "${password}\n"
wait
