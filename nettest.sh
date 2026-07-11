#!/usr/bin/env bash
# Thanks gemini 3.1 Pro

# netctrl - Manage internet state and speed
# Usage: sudo ./netctrl.sh {on|off|throttle <mbps>}

# Ensure the script is run as root
if [ "$EUID" -ne 0 ]; then
  echo "Error: Please run as root (use sudo)"
  exit 1
fi

# Auto-detect the active network interface (e.g., eth0, wlan0, enp3s0)
IFACE=$(ip route | awk '/default/ {print $5}' | head -n1)

clear_throttle() {
    # Suppress errors if no rules exist yet
    tc qdisc del dev "$IFACE" root 2>/dev/null
    tc qdisc del dev "$IFACE" ingress 2>/dev/null
}

case "$1" in
    on)
        echo "Enabling networking system-wide..."
        nmcli networking on
        
        # Give NetworkManager a second to establish the connection 
        # so we can detect the interface and clear its throttle
        sleep 2
        IFACE=$(ip route | awk '/default/ {print $5}' | head -n1)
        
        if [ -n "$IFACE" ]; then
            clear_throttle
            echo "Internet is ON. Throttling reset for $IFACE."
        else
            echo "Internet is ON (No active interface detected to reset throttle)."
        fi
        ;;
        
    off)
        echo "Disabling networking system-wide..."
        nmcli networking off
        
        # We clear the throttle here too, just to leave things clean
        if [ -n "$IFACE" ]; then
            clear_throttle
        fi
        echo "Internet is OFF."
        ;;
        
    throttle)
        SPEED="$2"
        
        # Validate that the second argument is a positive integer
        if ! [[ "$SPEED" =~ ^[0-9]+$ ]]; then
            echo "Error: Please provide a valid speed in Mbps."
            echo "Example: $0 throttle 5"
            exit 1
        fi
        
        if [ -z "$IFACE" ]; then
            echo "Error: No active network connection detected to throttle."
            exit 1
        fi
        
        echo "Throttling $IFACE to ${SPEED} Mbps..."
        
        # Clear any existing limits first to prevent "File exists" errors
        clear_throttle
        
        # 1. Egress (Upload) limit using Token Bucket Filter (TBF)
        tc qdisc add dev "$IFACE" root tbf rate "${SPEED}mbit" burst 32kbit latency 400ms
        
        # 2. Ingress (Download) limit using policing
        tc qdisc add dev "$IFACE" handle ffff: ingress
        tc filter add dev "$IFACE" parent ffff: protocol ip prio 50 u32 match ip src 0.0.0.0/0 police rate "${SPEED}mbit" burst 32kbit drop flowid :1
        
        echo "Throttle applied."
        ;;
        
    *)
        echo "Usage: $0 {on|off|throttle <mbps>}"
        exit 1
        ;;
esac