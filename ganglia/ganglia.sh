#!/bin/bash
#    Copyright 2015 Google, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

set -x -e

apt-get update && apt-get install -y ganglia-monitor

MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

sed -e "/name = \"unspecified\" /s/unspecified/$MASTER/" -i /etc/ganglia/gmond.conf
sed -e '/mcast_join /s/^  /  #/' -i /etc/ganglia/gmond.conf
sed -e '/bind /s/^  /  #/' -i /etc/ganglia/gmond.conf

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then
    # Only run on the master node
    # Install dependencies needed for ganglia
    DEBIAN_FRONTEND=noninteractive apt install -y rrdtool gmetad ganglia-webfrontend
    cp /etc/ganglia-webfrontend/apache.conf /etc/apache2/sites-enabled/ganglia.conf

    sed -i "s/my cluster/$MASTER/" /etc/ganglia/gmetad.conf
    sed -e '/udp_send_channel {/a\  host = localhost' -i /etc/ganglia/gmond.conf

    service ganglia-monitor restart && service gmetad restart && service apache2 restart

else

    sed -e "/udp_send_channel {/a\  host = $MASTER" -i /etc/ganglia/gmond.conf
    sed -i '/udp_recv_channel {/,/}/d' /etc/ganglia/gmond.conf
    service ganglia-monitor restart

fi
