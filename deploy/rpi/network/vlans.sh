#!/usr/bin/env bash
# Configure 802.1Q VLANs on a Raspberry Pi edge node.
#
#   VLAN 10  mgmt     10.10.0.0/24  SSH + Ansible from the ops laptop only
#   VLAN 20  cameras  10.20.0.0/24  ESP32-CAMs, Pi CV agents, control-plane edge gateway
#   VLAN 30  uplink   10.30.0.0/24  route to the cloud (EKS ingress, Kafka bootstrap)
#
# The managed switch port facing the Pi is a trunk carrying 10/20/30 tagged.
# Idempotent: safe to re-run. Persisted by systemd-networkd files below.
set -euo pipefail

PARENT=${PARENT:-eth0}
NODE_OCTET=${NODE_OCTET:-11}   # .11, .12, ... per Pi

declare -A VLANS=( [10]=mgmt [20]=cameras [30]=uplink )

modprobe 8021q
for id in "${!VLANS[@]}"; do
  ifname="${PARENT}.${id}"
  if ! ip link show "$ifname" &>/dev/null; then
    ip link add link "$PARENT" name "$ifname" type vlan id "$id"
  fi
  ip addr replace "10.${id}.0.${NODE_OCTET}/24" dev "$ifname"
  ip link set "$ifname" up
  echo "configured $ifname (${VLANS[$id]}) 10.${id}.0.${NODE_OCTET}/24"
done

# Only the uplink VLAN gets a default route.
ip route replace default via 10.30.0.1 dev "${PARENT}.30"

# Persist with systemd-networkd.
if [[ "${PERSIST:-1}" == "1" ]]; then
  mkdir -p /etc/systemd/network
  cat > "/etc/systemd/network/10-${PARENT}.network" <<NET
[Match]
Name=${PARENT}

[Network]
VLAN=${PARENT}.10
VLAN=${PARENT}.20
VLAN=${PARENT}.30
LinkLocalAddressing=no
NET
  for id in "${!VLANS[@]}"; do
    cat > "/etc/systemd/network/20-${PARENT}.${id}.netdev" <<NET
[NetDev]
Name=${PARENT}.${id}
Kind=vlan

[VLAN]
Id=${id}
NET
    gw=""
    [[ "$id" == "30" ]] && gw=$'\nGateway=10.30.0.1'
    cat > "/etc/systemd/network/30-${PARENT}.${id}.network" <<NET
[Match]
Name=${PARENT}.${id}

[Network]
Address=10.${id}.0.${NODE_OCTET}/24${gw}
NET
  done
  systemctl enable --now systemd-networkd >/dev/null
fi
