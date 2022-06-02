set -xe

# Blackhole the VM metadata server except for root.
iptables -A OUTPUT -d 169.254.169.254 -p tcp -m owner --uid-owner root -j ACCEPT
iptables -A OUTPUT -d 169.254.169.254 -p tcp -j REJECT

# Save current iptable setting to preserve it after reboot.
mkdir -p /etc/iptables
iptables-save >/etc/iptables/rules.v4

