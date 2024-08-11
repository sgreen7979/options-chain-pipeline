#!/usr/bin/env python3

import contextlib
import ipaddress
import os
import platform
import socket
from typing import Dict
from typing import List
from typing import NamedTuple

from paramiko import SSHClient

from options_chain_pipeline.lib.env import IS_LINUX
from options_chain_pipeline.lib.env import IS_MAC
from options_chain_pipeline.lib.env import IS_WINDOWS

from .nmap import PortScanner
from .nmap import ScanEntryType
from .nmap import ScanType


class Device(NamedTuple):
    ip_address: str
    hostname: str
    mac: str
    state: str
    reason: str
    vendor: str
    is_local: bool
    is_remote: bool

    @classmethod
    def from_nmap_scan(cls, scan_entry: ScanEntryType) -> "Device":
        """Construct an instance of `Device` given
        information obtained from a nmap scan

        :param scan_info: info from the nmap scan
        :type scan_info: Dict
        :return: the device
        :rtype: Device
        """
        # example scan_info dictionary:
        #     {'hostnames': [{'name': '', 'type': ''}],
        #     'addresses': {'ipv4': '192.168.1.201', 'mac': '70:8B:CD:50:47:23'},
        #     'vendor': {'70:8B:CD:50:47:23': 'ASUSTek Computer'},
        #     'status': {'state': 'up', 'reason': 'arp-response'}}
        ip_address = ""
        hostname = ""
        mac = ""
        state = ""
        reason = ""
        vendor = ""
        is_local = False
        is_remote = False

        addresses = scan_entry["addresses"]

        if "ipv4" in addresses:
            ip_address = addresses["ipv4"]

        if "mac" in addresses:
            mac = addresses["mac"]

        host_info = scan_entry["hostnames"][0]

        if host_info["name"]:
            hostname = host_info["name"]
        else:
            with contextlib.suppress(socket.herror):
                hostname = socket.gethostbyaddr(ip_address)[0]

        status = scan_entry["status"]
        state = status["state"]
        reason = status["reason"]
        is_local = reason == "localhost-response"
        is_remote = not is_local

        if vendor_info := scan_entry["vendor"]:
            vendor = list(vendor_info.values())[0]

        return cls(
            ip_address, hostname, mac, state, reason, vendor, is_local, is_remote
        )


def get_local_uid(local_server: str) -> str:
    """Returns local user as 'SERVER\\user'"""
    if IS_WINDOWS:
        return "{}\\{}".format(local_server, os.getlogin())
    elif IS_LINUX:
        import pwd as _pwd

        login = _pwd.getpwuid(os.geteuid()).pw_name  # type: ignore
        return "{}\\{}".format(local_server, login)
    else:
        raise NotImplementedError


def get_connected_devices() -> List[Device]:
    """List of devices connected to the same network."""
    devices: List[Device] = []

    scan: ScanType = PortScanner().scan(
        hosts='192.168.1.0/24', arguments='-n -sP -PE -PA21,23,80,3389'
    )["scan"]

    for ip_address in scan.keys():
        scan_entry: ScanEntryType = scan[ip_address]
        device = Device.from_nmap_scan(scan_entry)
        devices.append(device)

    return devices


def iter_connected_devices():
    yield from get_connected_devices()


def is_device_connected(host_or_ip: str) -> bool:
    if is_local(host_or_ip):
        return True
    for device in iter_connected_devices():
        if host_or_ip == device.hostname:
            return True
        if host_or_ip == device.ip_address:
            return True
    return False


def get_local_ipv4() -> str:
    """Returns the IPv4 address of the local device"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ipv4 = s.getsockname()[0]
    s.close()
    return ipv4


def get_local_hostname() -> str:
    """Returns the hostname of the local device

    return platform.node()
    """
    return platform.node()


def hostname_from_ip(ip: str) -> str:
    """Get hostname associated with the ip address"""
    return socket.gethostbyaddr(ip)[0]


def ip_from_hostname(host_or_ip: str) -> str:
    """Returns the ip address of a connection that
    may be identified by its hostname or ip address

    If the connection passed is not an ip address
    (i.e., a hostname), we attempt to identify the
    associated ip address

    .. note:
        ssh configuration files sometimes reference
        known hosts by their names, and other times
        by their ip addresses; this is meant to
        disambiguate hostname from ip address
    """
    if is_ip_addr(host_or_ip):
        return host_or_ip
    return socket.gethostbyname(host_or_ip)


def is_ip_addr(host_or_ip: str) -> bool:
    """Determine if the passed connection is its
    ip address
    """
    try:
        ipaddress.ip_address(host_or_ip)
        return True
    except ValueError:
        return False


def get_hostname_if_ip(host_or_ip: str) -> str:
    """Returns the hostname of a connection that
    may be identified by its hostname or ip address

    If the connection passed is an ip address, we
    attempt to identify the associated hostname

    .. note:
        ssh configuration files sometimes reference
        known hosts by their names, and other times
        by their ip addresses; this is meant to
        disambiguate hostname from ip address
    """
    return hostname_from_ip(host_or_ip) if is_ip_addr(host_or_ip) else host_or_ip


get_hostname = get_hostname_if_ip


def is_remote(host_or_ip: str) -> bool:
    """Determine if the passed connection is a
    remote connection"""
    return not is_local(host_or_ip)


def is_local(host_or_ip: str) -> bool:
    """Determine if the passed connection is a
    local connection"""
    return host_or_ip in (get_local_ipv4(), get_local_hostname())


def connected(host_or_ip: str) -> bool:
    """Determine if the passed connection is
    currently recognized on the network"""
    if is_local(host_or_ip):
        return True
    for device in get_connected_devices():
        if device.ip_address == host_or_ip:
            return True
        if device.hostname.upper() == host_or_ip.upper():
            return True
    return False


def get_user_ssh_known_hosts() -> List[str]:
    """Returns a list of known hosts present
    in user ssh config file

    ```python
    >>> from options_chain_pipeline.lib.utils.networking import get_ssh_known_hosts
    >>> print(get_ssh_known_hosts())
    ['desktop-coha35q',
    'fe80::6701:d122:f098:559c%2',
    'github.com',
    '192.168.1.228',
    'sams-mbp']
    ```
    """
    return list(get_user_ssh_config().keys())


def get_user_ssh_config() -> Dict[str, List[str]]:
    ssh_client = SSHClient()
    host_keys_path = os.path.expanduser(os.path.join("~", ".ssh", "known_hosts"))
    ssh_client.load_host_keys(host_keys_path)
    known_hosts = ssh_client.get_host_keys()
    ssh_config = {}
    for host in known_hosts:
        ssh_config[host] = known_hosts[host].keys()
    return ssh_config
