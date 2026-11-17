#!/usr/bin/env bash
# iptables policy for an EdgeFleet Pi node. Default-deny in both directions;
# cameras can only talk to the local agent / gateway, never to the internet.
set -euo pipefail

P=${PARENT:-eth0}
MGMT=${P}.10 CAMS=${P}.20 UPLINK=${P}.30
CONTROL_PLANE=${CONTROL_PLANE:-edgefleet.example.com}
KAFKA_PORT=${KAFKA_PORT:-9094}   # TLS listener exposed by the EKS NLB

ipt() { iptables -w "$@"; }

ipt -F; ipt -X; ipt -t nat -F
ipt -P INPUT DROP
ipt -P FORWARD DROP
ipt -P OUTPUT DROP

# Loopback + established flows
ipt -A INPUT  -i lo -j ACCEPT
ipt -A OUTPUT -o lo -j ACCEPT
ipt -A INPUT  -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
ipt -A OUTPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
ipt -A FORWARD -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
ipt -A INPUT -m conntrack --ctstate INVALID -j DROP

# mgmt VLAN: SSH only, rate-limited
ipt -A INPUT -i "$MGMT" -p tcp --dport 22 -m conntrack --ctstate NEW -m limit --limit 6/min --limit-burst 6 -j ACCEPT

# camera VLAN -> this node: DHCP, NTP, heartbeat relay (8080), Kafka REST proxy (8082)
ipt -A INPUT -i "$CAMS" -p udp --dport 67 -j ACCEPT
ipt -A INPUT -i "$CAMS" -p udp --dport 123 -j ACCEPT
ipt -A INPUT -i "$CAMS" -p tcp -m multiport --dports 8080,8082 -m conntrack --ctstate NEW -j ACCEPT
ipt -A OUTPUT -o "$CAMS" -p udp --sport 67 -j ACCEPT

# camera VLAN must never reach mgmt or the internet directly
ipt -A FORWARD -i "$CAMS" -j LOG --log-prefix "ef-cam-fwd-drop: " --log-level 4 -m limit --limit 5/min
ipt -A FORWARD -i "$CAMS" -j DROP

# uplink: DNS, NTP, HTTPS to control plane + firmware bucket, Kafka TLS
ipt -A OUTPUT -o "$UPLINK" -p udp --dport 53 -j ACCEPT
ipt -A OUTPUT -o "$UPLINK" -p tcp --dport 53 -j ACCEPT
ipt -A OUTPUT -o "$UPLINK" -p udp --dport 123 -j ACCEPT
ipt -A OUTPUT -o "$UPLINK" -p tcp --dport 443 -j ACCEPT
ipt -A OUTPUT -o "$UPLINK" -p tcp --dport "$KAFKA_PORT" -j ACCEPT

# Log and drop everything else
ipt -A INPUT  -j LOG --log-prefix "ef-in-drop: "  --log-level 4 -m limit --limit 5/min
ipt -A OUTPUT -j LOG --log-prefix "ef-out-drop: " --log-level 4 -m limit --limit 5/min

# Same default-deny for IPv6
ip6tables -w -P INPUT DROP; ip6tables -w -P FORWARD DROP; ip6tables -w -P OUTPUT DROP
ip6tables -w -A INPUT -i lo -j ACCEPT; ip6tables -w -A OUTPUT -o lo -j ACCEPT

if command -v netfilter-persistent >/dev/null; then netfilter-persistent save; fi
echo "firewall applied (control plane: $CONTROL_PLANE)"
