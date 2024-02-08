#!/bin/bash

# This bash script allows to bypass the error MySQL ERROR: Access denied for user 'root'@'localhost'.
# It changes the password for root and sets the authentication method to mysql_native_password which is the
# traditional method for authentication.

# Run MySQL commands with sudo
sudo mysql <<EOF
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'root-password';
quit
EOF
