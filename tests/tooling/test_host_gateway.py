import unittest

from deploy.substrate.host_gateway import ipv4_gateway


class HostGatewayTests(unittest.TestCase):
    def test_dual_stack_order_does_not_change_ipv4_endpoint(self):
        gateways=[{'Gateway':'fc00:f853:ccd:e793::1'},{'Gateway':'172.18.0.1'}]
        for configs in (gateways,list(reversed(gateways))):
            self.assertEqual(ipv4_gateway({'IPAM':{'Config':configs}}),'172.18.0.1')

    def test_ipv6_only_network_requires_an_explicit_ipv4_gateway(self):
        with self.assertRaisesRegex(ValueError,'IPv4 gateway'):
            ipv4_gateway({'IPAM':{'Config':[{'Gateway':'fd00::1'}]}})
