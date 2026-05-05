#!/usr/bin/env python3
"""
fix_5g_routing.py – Diagnose and repair routing-table issues that prevent a
5G USB modem from providing internet access on a Raspberry Pi 5.

Usage:
    sudo python3 fix_5g_routing.py            # diagnose only
    sudo python3 fix_5g_routing.py --fix      # diagnose and apply fixes
    sudo python3 fix_5g_routing.py --verbose  # extra detail

Requirements: Python 3.7+, standard library only (subprocess, re, …).
"""

import argparse
import ipaddress
import re
import subprocess
import sys
from dataclasses import dataclass, field
from typing import List, Optional


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def run(cmd: List[str], check: bool = False) -> subprocess.CompletedProcess:
    """Run a command and return CompletedProcess.

    When *check* is False (the default) a non-zero exit code is silently
    ignored.  When *check* is True, CalledProcessError is raised on failure.
    """
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=check,
    )


def header(msg: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print('='*60)


def ok(msg: str) -> None:
    print(f"  [OK]  {msg}")


def warn(msg: str) -> None:
    print(f"  [!!]  {msg}")


def info(msg: str) -> None:
    print(f"  [--]  {msg}")


# ──────────────────────────────────────────────────────────────────────────────
# Data structures
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class RouteEntry:
    destination: str   # e.g. "0.0.0.0" or "192.168.8.0"
    gateway: str       # e.g. "192.168.8.1" or "0.0.0.0"
    netmask: str       # e.g. "0.0.0.0" or "255.255.255.0"
    flags: str
    iface: str
    metric: int = 0


@dataclass
class InterfaceInfo:
    name: str
    up: bool = False
    has_ip: bool = False
    ipv4: Optional[str] = None
    prefix_len: int = 24       # actual prefix length from 'ip addr show'
    link_type: str = ""    # "wwan", "usb", "eth", "wlan", "other"


@dataclass
class DiagResult:
    modem_ifaces: List[InterfaceInfo] = field(default_factory=list)
    routes: List[RouteEntry] = field(default_factory=list)
    issues: List[str] = field(default_factory=list)
    fixes_applied: List[str] = field(default_factory=list)


# ──────────────────────────────────────────────────────────────────────────────
# Network interface discovery
# ──────────────────────────────────────────────────────────────────────────────

# Common interface name patterns for USB / 5G modems
MODEM_PATTERNS = re.compile(
    r'^(wwan\d+|usb\d+|wwp\w+|enx[0-9a-f]+|eth\d+|ppp\d+)$'
)

EXCLUDE_IFACES = {"lo"}


def get_interfaces(verbose: bool = False) -> List[InterfaceInfo]:
    """Return all non-loopback interfaces with basic state information."""
    result = run(["ip", "-o", "link", "show"])
    if result.returncode != 0:
        warn("Could not run 'ip link show'")
        return []

    ifaces: List[InterfaceInfo] = []
    # Format: <index>: <name>: <flags> …
    for line in result.stdout.splitlines():
        m = re.match(r'^\d+:\s+(\S+?)[@:]?\s+.*?<([^>]*)>', line)
        if not m:
            continue
        name = m.group(1).split('@')[0]  # strip @ifb0 etc.
        flags_str = m.group(2)
        if name in EXCLUDE_IFACES:
            continue

        up = "UP" in flags_str.split(',')

        # Determine link type
        if re.match(r'^wwan\d+|^wwp', name):
            link_type = "wwan"
        elif re.match(r'^usb\d+', name):
            link_type = "usb"
        elif re.match(r'^ppp\d+', name):
            link_type = "ppp"
        elif re.match(r'^eth\d+|^enx|^enp', name):
            link_type = "eth"
        elif re.match(r'^wlan\d+', name):
            link_type = "wlan"
        else:
            link_type = "other"

        iface = InterfaceInfo(name=name, up=up, link_type=link_type)

        # Get IPv4 address and prefix length
        addr_result = run(["ip", "-4", "addr", "show", name])
        addr_match = re.search(r'inet\s+(\d+\.\d+\.\d+\.\d+)/(\d+)', addr_result.stdout)
        if addr_match:
            iface.has_ip = True
            iface.ipv4 = addr_match.group(1)
            iface.prefix_len = int(addr_match.group(2))

        if verbose:
            info(f"Found interface: {name} up={up} ip={iface.ipv4} type={link_type}")

        ifaces.append(iface)

    return ifaces


def identify_modem_ifaces(ifaces: List[InterfaceInfo]) -> List[InterfaceInfo]:
    """Return interfaces that look like they belong to a 5G/USB modem."""
    candidates = []
    for iface in ifaces:
        if iface.link_type in ("wwan", "usb", "ppp"):
            candidates.append(iface)
        elif iface.link_type == "eth" and MODEM_PATTERNS.match(iface.name):
            # Some modems present as ethernet (e.g. enx… CDC-Ethernet)
            candidates.append(iface)
    return candidates


# ──────────────────────────────────────────────────────────────────────────────
# Routing table inspection
# ──────────────────────────────────────────────────────────────────────────────

def get_routes(verbose: bool = False) -> List[RouteEntry]:
    """Parse the kernel routing table via 'route -n'."""
    result = run(["route", "-n"])
    if result.returncode != 0:
        # Fall back to 'ip route'
        return get_routes_ip(verbose)

    routes: List[RouteEntry] = []
    for line in result.stdout.splitlines():
        parts = line.split()
        # Columns: Destination Gateway Netmask Flags Metric Ref Use Iface
        if len(parts) < 8 or not re.match(r'^\d', parts[0]):
            continue
        try:
            entry = RouteEntry(
                destination=parts[0],
                gateway=parts[1],
                netmask=parts[2],
                flags=parts[3],
                metric=int(parts[4]),
                iface=parts[7],
            )
            routes.append(entry)
            if verbose:
                info(f"Route: {entry.destination}/{entry.netmask} gw={entry.gateway} "
                     f"iface={entry.iface} flags={entry.flags} metric={entry.metric}")
        except (ValueError, IndexError):
            continue
    return routes


def get_routes_ip(verbose: bool = False) -> List[RouteEntry]:
    """Parse routing table via 'ip route show' (fallback)."""
    result = run(["ip", "route", "show"])
    routes: List[RouteEntry] = []
    for line in result.stdout.splitlines():
        # e.g. "default via 192.168.8.1 dev wwan0 proto dhcp metric 700"
        # or   "192.168.8.0/24 dev wwan0 proto kernel scope link src 192.168.8.100"
        dest = "0.0.0.0"
        gateway = "0.0.0.0"
        netmask = "0.0.0.0"
        iface = ""
        metric = 0
        flags = "U"

        if line.startswith("default"):
            dest = "0.0.0.0"
            netmask = "0.0.0.0"
            flags = "UG"
        else:
            prefix_m = re.match(r'^(\d+\.\d+\.\d+\.\d+)(?:/(\d+))?', line)
            if prefix_m:
                dest = prefix_m.group(1)
                prefix_len = int(prefix_m.group(2) or 32)
                netmask = str(ipaddress.IPv4Network(f"0.0.0.0/{prefix_len}").netmask)

        gw_m = re.search(r'via\s+(\d+\.\d+\.\d+\.\d+)', line)
        if gw_m:
            gateway = gw_m.group(1)
            flags = "UG"

        dev_m = re.search(r'dev\s+(\S+)', line)
        if dev_m:
            iface = dev_m.group(1)

        metric_m = re.search(r'metric\s+(\d+)', line)
        if metric_m:
            metric = int(metric_m.group(1))

        if iface:
            entry = RouteEntry(
                destination=dest,
                gateway=gateway,
                netmask=netmask,
                flags=flags,
                iface=iface,
                metric=metric,
            )
            routes.append(entry)
            if verbose:
                info(f"Route (ip): {dest}/{netmask} gw={gateway} "
                     f"iface={iface} flags={flags} metric={metric}")
    return routes


# ──────────────────────────────────────────────────────────────────────────────
# Diagnostics
# ──────────────────────────────────────────────────────────────────────────────

FALLBACK_DNS = "8.8.8.8"   # used for ping connectivity test

# Candidate paths where dhcpcd stores its lease/config files
_DHCPCD_LEASE_DIRS = ["/var/lib/dhcpcd", "/run/dhcpcd", "/var/lib/dhcp"]


def _gateway_from_dhcp_lease(iface_name: str) -> Optional[str]:
    """Try to read the gateway from a dhcpcd/dhclient lease file."""
    import glob as _glob
    patterns = [
        f"{d}/{iface_name}.lease*"   for d in _DHCPCD_LEASE_DIRS
    ] + [
        f"{d}/dhclient-{iface_name}.conf" for d in _DHCPCD_LEASE_DIRS
    ] + [
        "/var/lib/dhcp/dhclient.leases",
    ]
    for pattern in patterns:
        for path in _glob.glob(pattern):
            try:
                with open(path) as fh:
                    content = fh.read()
                m = re.search(r'routers?\s+(\d+\.\d+\.\d+\.\d+)', content)
                if m:
                    return m.group(1)
            except OSError:
                continue
    return None


def _gateway_from_ip_route(iface_name: str) -> Optional[str]:
    """Look for any gateway reachable via iface_name in 'ip route show'."""
    result = run(["ip", "route", "show", "dev", iface_name])
    for line in result.stdout.splitlines():
        m = re.search(r'via\s+(\d+\.\d+\.\d+\.\d+)', line)
        if m:
            return m.group(1)
    return None


def resolve_gateway(iface_obj: "InterfaceInfo") -> Optional[str]:
    """Return the best-guess gateway for *iface_obj*, or None if unknown.

    Priority:
    1. DHCP lease file (most accurate)
    2. Existing route via that interface that carries a 'via' gateway
    """
    gw = _gateway_from_dhcp_lease(iface_obj.name)
    if gw:
        return gw
    gw = _gateway_from_ip_route(iface_obj.name)
    if gw:
        return gw
    return None



def check_dns() -> bool:
    """Return True if DNS resolution appears to work.

    Uses Python's socket module to avoid locale-dependent tool output parsing.
    """
    import socket
    try:
        socket.setdefaulttimeout(5)
        socket.getaddrinfo("google.com", 80)
        return True
    except (socket.gaierror, OSError):
        return False


def ping_host(host: str, iface: Optional[str] = None, count: int = 3) -> bool:
    """Ping a host; optionally bind to a specific interface."""
    cmd = ["ping", "-c", str(count), "-W", "2"]
    if iface:
        cmd += ["-I", iface]
    cmd.append(host)
    result = run(cmd)
    return result.returncode == 0


def diagnose(verbose: bool = False) -> DiagResult:
    diag = DiagResult()

    # ── 1. Interfaces ──────────────────────────────────────────────────────
    header("1. Network Interfaces")
    all_ifaces = get_interfaces(verbose)
    modem_ifaces = identify_modem_ifaces(all_ifaces)
    diag.modem_ifaces = modem_ifaces

    if not modem_ifaces:
        warn("No modem-type interfaces (wwan*, usb*, ppp*) detected.")
        warn("If the modem is connected via USB, check 'lsusb' and 'dmesg | tail -30'.")
        diag.issues.append("no_modem_iface")
    else:
        for iface in modem_ifaces:
            state = "UP" if iface.up else "DOWN"
            ip_info = iface.ipv4 if iface.has_ip else "no IP"
            if iface.up and iface.has_ip:
                ok(f"{iface.name} is {state}, IP={ip_info}")
            elif iface.up and not iface.has_ip:
                warn(f"{iface.name} is {state} but has NO IP address")
                diag.issues.append(f"no_ip:{iface.name}")
            else:
                warn(f"{iface.name} is {state}, IP={ip_info}")
                diag.issues.append(f"iface_down:{iface.name}")

    # ── 2. Routing table ──────────────────────────────────────────────────
    header("2. Routing Table")
    routes = get_routes(verbose)
    diag.routes = routes

    default_routes = [r for r in routes if r.destination == "0.0.0.0"]
    modem_defaults = [
        r for r in default_routes
        if any(r.iface == iface.name for iface in modem_ifaces)
    ]

    if not default_routes:
        warn("No default (0.0.0.0) route found – internet traffic has nowhere to go!")
        diag.issues.append("no_default_route")
    elif not modem_defaults:
        others = [(r.iface, r.metric) for r in default_routes]
        warn(f"Default route exists but points to other interface(s): {others}")
        warn("Traffic is NOT routed through the 5G modem.")
        diag.issues.append("default_not_via_modem")
        for r in default_routes:
            info(f"  existing default: gw={r.gateway} dev={r.iface} metric={r.metric}")
    else:
        # Sort by metric; lowest metric wins
        best = sorted(modem_defaults, key=lambda r: r.metric)[0]
        all_defaults = sorted(default_routes, key=lambda r: r.metric)
        if all_defaults[0].iface != best.iface:
            warn(
                f"Modem default route (metric={best.metric}) is overridden by "
                f"{all_defaults[0].iface} (metric={all_defaults[0].metric})."
            )
            diag.issues.append("modem_route_metric_too_high")
            for r in all_defaults:
                info(f"  default: gw={r.gateway} dev={r.iface} metric={r.metric}")
        else:
            ok(f"Default route via modem interface {best.iface} (metric={best.metric}, gw={best.gateway})")

    # ── 3. Modem-specific subnet routes ───────────────────────────────────
    header("3. Modem Subnet Routes")
    for iface in modem_ifaces:
        subnet_routes = [r for r in routes if r.iface == iface.name and r.destination != "0.0.0.0"]
        if subnet_routes:
            for r in subnet_routes:
                ok(f"  subnet {r.destination}/{r.netmask} dev {r.iface}")
        else:
            if iface.has_ip:
                warn(f"No subnet route for {iface.name} (IP={iface.ipv4}) – may be missing.")
                diag.issues.append(f"no_subnet_route:{iface.name}")
            else:
                info(f"  {iface.name}: no IP, so no subnet route expected yet.")

    # ── 4. Connectivity test ──────────────────────────────────────────────
    header("4. Connectivity Tests")
    for iface in modem_ifaces:
        if iface.has_ip:
            if ping_host(FALLBACK_DNS, iface=iface.name):
                ok(f"Ping {FALLBACK_DNS} via {iface.name} succeeded")
            else:
                warn(f"Ping {FALLBACK_DNS} via {iface.name} FAILED")
                diag.issues.append(f"ping_failed:{iface.name}")

    if not modem_ifaces:
        info("Skipping connectivity test – no modem interface found.")
    else:
        # General (default-route) connectivity
        if ping_host(FALLBACK_DNS):
            ok(f"General ping {FALLBACK_DNS} (via default route) succeeded")
        else:
            warn(f"General ping {FALLBACK_DNS} FAILED – no internet connectivity")
            diag.issues.append("no_internet")

    # ── 5. DNS ────────────────────────────────────────────────────────────
    header("5. DNS Resolution")
    if check_dns():
        ok("DNS resolution working (google.com resolved)")
    else:
        warn("DNS resolution FAILED")
        diag.issues.append("dns_broken")

    return diag


# ──────────────────────────────────────────────────────────────────────────────
# Fixes
# ──────────────────────────────────────────────────────────────────────────────

def apply_fixes(diag: DiagResult, dry_run: bool = False) -> None:
    header("Applying Fixes")

    def exec_fix(description: str, cmd: List[str]) -> bool:
        print(f"  >> {' '.join(cmd)}")
        if dry_run:
            info(f"  (dry-run) would run: {' '.join(cmd)}")
            diag.fixes_applied.append(f"[DRY-RUN] {description}")
            return True
        result = run(cmd)
        if result.returncode == 0:
            ok(description)
            diag.fixes_applied.append(description)
            return True
        else:
            warn(f"Fix FAILED: {description}")
            warn(f"  stderr: {result.stderr.strip()}")
            return False

    modem_iface_names = [i.name for i in diag.modem_ifaces]

    for issue in diag.issues:

        # ── Interface is DOWN ──
        if issue.startswith("iface_down:"):
            iface = issue.split(":", 1)[1]
            exec_fix(f"Bring up interface {iface}", ["ip", "link", "set", iface, "up"])

        # ── Interface has no IP → try DHCP ──
        elif issue.startswith("no_ip:"):
            iface = issue.split(":", 1)[1]
            warn(f"Interface {iface} has no IP. Attempting DHCP (dhclient)…")
            exec_fix(
                f"Request DHCP address on {iface}",
                ["dhclient", "-v", iface],
            )

        # ── No default route at all ──
        elif issue == "no_default_route":
            for iface_name in modem_iface_names:
                iface_obj = next((i for i in diag.modem_ifaces if i.name == iface_name), None)
                if iface_obj and iface_obj.has_ip:
                    gateway = resolve_gateway(iface_obj)
                    if not gateway:
                        warn(
                            f"Cannot determine gateway for {iface_name}. "
                            "Check the modem's DHCP lease or APN settings."
                        )
                        continue
                    warn(f"Adding default route via {gateway} dev {iface_name} metric 600")
                    exec_fix(
                        f"Add default route via {iface_name}",
                        ["ip", "route", "add", "default", "via", gateway, "dev", iface_name, "metric", "600"],
                    )

        # ── Default route exists but not via modem ──
        elif issue == "default_not_via_modem":
            for iface_obj in diag.modem_ifaces:
                if iface_obj.has_ip:
                    gateway = resolve_gateway(iface_obj)
                    if not gateway:
                        warn(
                            f"Cannot determine gateway for {iface_obj.name}. "
                            "Check the modem's DHCP lease or APN settings."
                        )
                        continue
                    # Add modem default with a lower metric than existing routes
                    min_metric = min((r.metric for r in diag.routes if r.destination == "0.0.0.0"), default=100)
                    new_metric = max(0, min_metric - 10)
                    warn(
                        f"Adding modem default route via {gateway} dev {iface_obj.name} "
                        f"metric {new_metric} (lower than existing {min_metric})"
                    )
                    exec_fix(
                        f"Add preferred default route via {iface_obj.name}",
                        ["ip", "route", "add", "default", "via", gateway,
                         "dev", iface_obj.name, "metric", str(new_metric)],
                    )

        # ── Modem default route has too-high metric ──
        elif issue == "modem_route_metric_too_high":
            for iface_obj in diag.modem_ifaces:
                if iface_obj.has_ip:
                    existing = [
                        r for r in diag.routes
                        if r.destination == "0.0.0.0" and r.iface == iface_obj.name
                    ]
                    for r in existing:
                        # Lower its metric so it wins
                        new_metric = max(0, r.metric - 200)
                        exec_fix(
                            f"Change default route metric on {iface_obj.name} to {new_metric}",
                            ["ip", "route", "change", "default",
                             "via", r.gateway, "dev", iface_obj.name,
                             "metric", str(new_metric)],
                        )

        # ── Missing subnet route ──
        elif issue.startswith("no_subnet_route:"):
            iface_name = issue.split(":", 1)[1]
            iface_obj = next((i for i in diag.modem_ifaces if i.name == iface_name), None)
            if iface_obj and iface_obj.has_ip:
                try:
                    net = ipaddress.IPv4Interface(
                        f"{iface_obj.ipv4}/{iface_obj.prefix_len}"
                    ).network
                    exec_fix(
                        f"Add subnet route for {iface_obj.name}",
                        ["ip", "route", "add", str(net), "dev", iface_obj.name],
                    )
                except Exception as exc:
                    warn(f"Could not add subnet route: {exc}")

        # ── DNS broken ──
        elif issue == "dns_broken":
            info("Checking /etc/resolv.conf …")
            try:
                with open("/etc/resolv.conf") as f:
                    content = f.read()
                info(f"/etc/resolv.conf:\n{content}")
            except OSError:
                warn("Could not read /etc/resolv.conf")

            if not dry_run:
                try:
                    with open("/etc/resolv.conf", "a") as f:
                        f.write(f"\n# added by fix_5g_routing.py\nnameserver {FALLBACK_DNS}\n")
                    ok(f"Appended nameserver {FALLBACK_DNS} to /etc/resolv.conf")
                    diag.fixes_applied.append(f"Added nameserver {FALLBACK_DNS}")
                except OSError as exc:
                    warn(f"Could not write /etc/resolv.conf: {exc}")
            else:
                info(f"[dry-run] would append 'nameserver {FALLBACK_DNS}' to /etc/resolv.conf")
                diag.fixes_applied.append(f"[DRY-RUN] Add nameserver {FALLBACK_DNS}")


# ──────────────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────────────

def print_summary(diag: DiagResult) -> None:
    header("Summary")
    if not diag.issues:
        ok("No routing issues detected. If you still have no internet, check:")
        info("  • Modem APN settings  (mmcli -m 0)")
        info("  • SIM card status     (mmcli -m 0 --simple-status)")
        info("  • Firewall rules      (iptables -L -n -v)")
        info("  • IP forwarding       (sysctl net.ipv4.ip_forward)")
    else:
        print(f"\n  Issues found ({len(diag.issues)}):")
        for issue in diag.issues:
            print(f"    • {issue}")

    if diag.fixes_applied:
        print(f"\n  Fixes applied ({len(diag.fixes_applied)}):")
        for fix in diag.fixes_applied:
            print(f"    ✓ {fix}")

    print()


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Diagnose (and optionally fix) routing-table issues "
                    "preventing a 5G modem from providing internet on a Raspberry Pi 5."
    )
    parser.add_argument(
        "--fix", action="store_true",
        help="Attempt to apply fixes automatically (requires root)."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what fixes would be applied without making changes."
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Print extra diagnostic detail."
    )
    args = parser.parse_args()

    print("5G Modem Routing Diagnostic Tool")
    print("Raspberry Pi 5  –  fix_5g_routing.py")

    if (args.fix or args.dry_run) and sys.platform != "win32":
        import os
        if os.geteuid() != 0:
            print("\nERROR: --fix and --dry-run require root privileges. Re-run with sudo.\n")
            return 1

    diag = diagnose(verbose=args.verbose)

    if args.fix or args.dry_run:
        apply_fixes(diag, dry_run=args.dry_run)

    elif diag.issues:
        print(
            "\n  Run with --fix to attempt automatic repairs, "
            "or --dry-run to preview them."
        )

    print_summary(diag)
    return 0 if not diag.issues else 2


if __name__ == "__main__":
    sys.exit(main())
