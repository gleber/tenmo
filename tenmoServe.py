import os
import functools
import io
import json
from http import HTTPStatus
import http.server
import socketserver
import asyncio
import websockets

MIME_TYPES = {
    "html": "text/html",
    "js": "text/javascript",
    "css": "text/css"
}


async def process_request(config, path, request_headers):
    """Serves a file when doing a GET request with a valid path."""
    sever_root = config['pwd']
    dotPath = config['dotPath']
    dotCb = config['dotCb']
    pgUri = config['pgUri']

    if "Upgrade" in request_headers:
        return  # Probably a WebSocket connection

    if path == dotPath:
        out = dotCb(pgUri)
        return (HTTPStatus.OK, [('Content-type', 'text/html')], out)

    if path == '/':
        path = '/index.html'

    response_headers = [
        ('Server', 'asyncio websocket server'),
        ('Connection', 'close'),
    ]

    # Derive full system path
    full_path = os.path.realpath(os.path.join(sever_root, path[1:]))

    # Validate the path
    if os.path.commonpath((sever_root, full_path)) != sever_root or \
            not os.path.exists(full_path) or not os.path.isfile(full_path):
        print("HTTP GET {} 404 NOT FOUND".format(path))
        return HTTPStatus.NOT_FOUND, [], b'404 NOT FOUND'

    # Guess file content type
    extension = full_path.split(".")[-1]
    mime_type = MIME_TYPES.get(extension, "application/octet-stream")
    response_headers.append(('Content-Type', mime_type))

    # Read the whole file into memory and send it out
    body = open(full_path, 'rb').read()
    response_headers.append(('Content-Length', str(len(body))))
    print("HTTP GET {} 200 OK".format(path))
    return HTTPStatus.OK, response_headers, body


def serve(pgUri, dotPath, dotCb):
    PORT = 8003

    async def hello(websocket, path):
        print('ws', path)
        # name = await websocket.recv()
        if path == '/wsdot':
            while True:
                res = {'dot': dotCb(pgUri).decode("utf-8")}
                await websocket.send(json.dumps(res))
                await asyncio.sleep(1)


        await websocket.send("")

    handler = functools.partial(process_request,
                                {'pwd': os.getcwd(),
                                 'pgUri': pgUri,
                                 'dotPath': dotPath,
                                 'dotCb': dotCb,
                                })
    ip = "0.0.0.0"
    print('Serving at http://%s:%d/' % (ip, PORT))
    start_server = websockets.serve(hello, ip, PORT, process_request=handler)

    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()
