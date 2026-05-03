"""
=============================================================
  ThreadTalk — WebSocket ↔ TCP Bridge
=============================================================
  Install:  pip install websockets
  Run:      python bridge.py   (after server.py is running)
  Browser connects to:  ws://localhost:8765
=============================================================
"""
import asyncio, websockets, json

TCP_HOST = "127.0.0.1"
TCP_PORT = 9001
WS_PORT  = 8765

async def handle(ws):
    print(f"[bridge] + {ws.remote_address}")
    try:
        reader, writer = await asyncio.open_connection(TCP_HOST, TCP_PORT)
    except ConnectionRefusedError:
        await ws.send(json.dumps({"type":"system","text":"Server offline.","time":"--:--","users":[]}))
        return

    async def fwd_ws_to_tcp():
        async for msg in ws:
            writer.write((msg + "\n").encode())
            await writer.drain()

    async def fwd_tcp_to_ws():
        buf = b""
        while True:
            chunk = await reader.read(4096)
            if not chunk: break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if line.strip():
                    await ws.send(line.decode())

    try:
        await asyncio.gather(fwd_ws_to_tcp(), fwd_tcp_to_ws())
    except (websockets.exceptions.ConnectionClosed, ConnectionResetError):
        pass
    finally:
        writer.close()
        print(f"[bridge] - {ws.remote_address}")

async def main():
    print(f"[bridge] WS:{WS_PORT} → TCP:{TCP_PORT}   (Ctrl-C to stop)")
    async with websockets.serve(handle, "0.0.0.0", WS_PORT):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
