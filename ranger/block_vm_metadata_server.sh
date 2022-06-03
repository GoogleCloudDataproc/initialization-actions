set -xe

# Block tcp connection to the VM metadata server except for root so that normal users can't get the VM service account's credential.
iptables -A OUTPUT -d 169.254.169.254 -p tcp -m owner --uid-owner root -j ACCEPT
iptables -A OUTPUT -d 169.254.169.254 -p tcp -j REJECT

# Install iptables-persistent to preserve the setting after reboot.
# This section only works on Debian and Ubuntu (Debian based systems).
echo iptables-persistent iptables-persistent/autosave_v4 boolean true | debconf-set-selections
echo iptables-persistent iptables-persistent/autosave_v6 boolean true | debconf-set-selections
apt-get update
apt-get -y install iptables-persistent

# Save current iptable setting to preserve it after reboot.
mkdir -p /etc/iptables
iptables-save >/etc/iptables/rules.v4

