#!/usr/bin/env python3
"""Select the IPv4 Docker bridge gateway used by the coordinator EndpointSlice."""
import ipaddress
import json
import subprocess


def ipv4_gateway(network):
    for config in network.get('IPAM', {}).get('Config', []):
        gateway=config.get('Gateway')
        if gateway and ipaddress.ip_address(gateway).version == 4:
            return gateway
    raise ValueError('the Kind Docker network requires an IPv4 gateway for the coordinator EndpointSlice')


if __name__ == '__main__':
    networks=json.loads(subprocess.check_output(['docker','network','inspect','kind'],text=True))
    print(ipv4_gateway(networks[0]))
