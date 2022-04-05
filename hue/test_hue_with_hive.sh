#!/bin/bash
curl -s localhost:8888/hue/accounts/login?next=/ -c /tmp/login.cookies --output /dev/null
csrftoken=$(sed -n -e "/.*csrftoken.*/s/.*csrftoken\t//p" /tmp/login.cookies)
curl -s localhost:8888/hue/accounts/login -H "Content-Type: application/x-www-form-urlencoded" -H "Referer: http://localhost:8888/accounts/login?next=/"   -d "username=admin&password=admin&csrfmiddlewaretoken=${csrftoken}&next=/"  -b /tmp/login.cookies -c /tmp/auth.cookies -o /dev/null
curl -s localhost:8888/desktop/api2/get_config/ -b /tmp/auth.cookies | grep -q '"dialect": "hive"'
if (( $? == 0 )); then
  exit 0
fi
exit 1
