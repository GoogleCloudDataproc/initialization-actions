#!/usr/bin/env bash
#
# disable transparent huge pages for Hadoop
#

thp_disable=true # disable transparent huge pages

if [ "${thp_disable}" = true ]; then
    if test -f /sys/kernel/mm/redhat_transparent_hugepage/enabled; then
        echo never > /sys/kernel/mm/redhat_transparent_hugepage/enabled
    fi
    if test -f /sys/kernel/mm/redhat_transparent_hugepage/defrag; then
        echo never > /sys/kernel/mm/redhat_transparent_hugepage/defrag
    fi
    if test -f /sys/kernel/mm/transparent_hugepage/enabled; then
        echo never > /sys/kernel/mm/transparent_hugepage/enabled
    fi
    if test -f /sys/kernel/mm/transparent_hugepage/defrag; then
        echo never > /sys/kernel/mm/transparent_hugepage/defrag
    fi
fi

exit 0
