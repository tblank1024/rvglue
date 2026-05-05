# raspap – 5G Modem Routing Diagnostic Tool

`fix_5g_routing.py` diagnoses (and can automatically repair) routing-table
problems that prevent a 5G USB modem from providing internet access on a
**Raspberry Pi 5** running Raspberry Pi OS.

## Assumptions

* The 5G modem is **powered and receiving a signal** – the problem is purely
  in the Linux routing table, not in the modem hardware or SIM.
* The modem appears as one of:
  * `wwan0` / `wwp…` – MBIM/QMI modems managed by ModemManager
  * `usb0` / `ppp0`  – RNDIS / PPP modems
  * `enx…`           – CDC-Ethernet modems
* Python 3.7+ is available (standard library only, no extra packages needed).

## Usage

```bash
# 1. Diagnose only (no changes made, no root needed for read-only checks)
python3 fix_5g_routing.py

# 2. Preview what would be fixed without making any changes
sudo python3 fix_5g_routing.py --dry-run

# 3. Diagnose and apply fixes
sudo python3 fix_5g_routing.py --fix

# 4. Extra detail
sudo python3 fix_5g_routing.py --fix --verbose
```

## What the script checks

| Step | Check |
|------|-------|
| 1 | All network interfaces – is the modem interface UP and does it have an IP? |
| 2 | Routing table – is there a default (0.0.0.0) route via the modem? Is its metric lower than competing routes? |
| 3 | Modem subnet route – is the directly-connected subnet routable? |
| 4 | Connectivity – can the Pi ping `8.8.8.8` through the modem interface? |
| 5 | DNS – does `nslookup google.com` succeed? |

## What the script can fix

| Issue | Fix applied |
|-------|-------------|
| Modem interface is DOWN | `ip link set <iface> up` |
| Modem interface has no IP | `dhclient -v <iface>` |
| No default route at all | `ip route add default via <gw> dev <iface> metric 600` |
| Default route not via modem | Adds modem default route with lower metric |
| Modem default has too-high metric | `ip route change default … metric <lower>` |
| Missing subnet route | `ip route add <subnet> dev <iface>` |
| DNS broken | Appends `nameserver 8.8.8.8` to `/etc/resolv.conf` |

> **Note:** Changes made with `--fix` are not persistent across reboots.
> To make them permanent, edit `/etc/dhcpcd.conf` or create a
> `systemd-networkd` configuration file.

## Exit codes

| Code | Meaning |
|------|---------|
| 0    | No issues detected |
| 1    | Script error (e.g. not run as root when --fix requested) |
| 2    | One or more issues detected |
