#!/usr/bin/env python3
"""Forward a task cluster's proxy connection through the WSL host."""
import argparse
import asyncio


async def relay(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def main(args):
    async def connection(reader, writer):
        try:
            remote_reader, remote_writer = await asyncio.open_connection(
                args.target_host, args.target_port
            )
            await asyncio.gather(
                relay(reader, remote_writer), relay(remote_reader, writer)
            )
        except (OSError, ConnectionError) as error:
            print(f"proxy relay: {error}", flush=True)
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(connection, args.bind, args.port)
    print(f"listening on {args.bind}:{args.port}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="172.18.0.1")
    parser.add_argument("--port", type=int, default=24718)
    parser.add_argument("--target-host", default="127.0.0.1")
    parser.add_argument("--target-port", type=int, default=24716)
    asyncio.run(main(parser.parse_args()))
