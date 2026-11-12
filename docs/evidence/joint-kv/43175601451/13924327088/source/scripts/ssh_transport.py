#!/usr/bin/env python3
"""SSH ProxyCommand with a bounded TCP MSS for WSL/VPN paths that drop large packets."""
import os
import selectors
import socket
import sys


def main():
    host, port = sys.argv[1:]
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as stream:
        stream.setsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG, 1200)
        stream.settimeout(10)
        stream.connect((host, int(port)))
        stream.settimeout(None)
        with selectors.DefaultSelector() as selector:
            selector.register(0, selectors.EVENT_READ)
            selector.register(stream, selectors.EVENT_READ)
            while True:
                for key, _ in selector.select():
                    if key.fileobj == 0:
                        data = os.read(0, 65536)
                        if data:
                            stream.sendall(data)
                        else:
                            selector.unregister(0)
                            stream.shutdown(socket.SHUT_WR)
                    else:
                        data = stream.recv(65536)
                        if not data:
                            return
                        while data:
                            data = data[os.write(1, data):]


if __name__ == '__main__':
    main()
