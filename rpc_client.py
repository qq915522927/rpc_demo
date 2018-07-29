import json
import time
import struct
import socket
import random
from kazoo.client import KazooClient

zk_root = '/demo'

G = {'servers': None}

class RemoteServer(object):

    def __init__(self, addr):
        self.addr = addr
        self._socket = None

    @property
    def socket(self):
        if not self._socket:
            self.connect()
        return self._socket

    def pi(self):
        pass


    def rpc(self,in_, params):
        sock = self.socket
        request = json.dumps({'in': in_, 'params': params})
        length_prefix = struct.pack('I', len(request))
        sock.sendall(length_prefix)
        sock.sendall(request)
        length_prefix = sock.recv(4)
        length, = struct.unpack('I', length_prefix)
        body = sock.recv(length)
        response = json.loads(body)
        return response['out'], response['result']

    def connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host, port = self.addr.split(':')
        sock.connect((host, int(port)))
        self._socket = sock

    def reconnect(self):
        self.close()
        self.connect()

    def close(self):
        if self._socket:
            self._socket.close()
            self._socket = None

    def ping(self, twitter):
        return self.rpc("ping", twitter)

def get_servers():
    zk = KazooClient(hosts = 'localhost:2181')
    zk.start()
    current_addrs = set()

    def watch_servers(*args):
        new_addrs = set()
        for child in zk.get_children(zk_root, watch=watch_servers):
            node = zk.get(zk_root + '/' + child)
            addr = json.loads(node[0])
            new_addrs.add('%s:%d' % (addr['host'], addr['port']))
        add_addrs = new_addrs - current_addrs
        del_addrs = current_addrs - new_addrs

        del_servers = []
        for addr in del_addrs:
            for s in G["servers"]:
                if s.addr == addr:
                    del_servers.append(s)
                    break

        for server in del_servers:
            G["servers"].remove(server)
            current_addrs.remove(server.addr)
        for addr in add_addrs:
            G["servers"].append(RemoteServer(addr))
            current_addrs.add(addr)

    for child in zk.get_children(zk_root, watch=watch_servers):
        node = zk.get(zk_root + "/" + child)
        addr = json.loads(node[0])
        current_addrs.add("%s:%d" % (addr["host"], addr["port"]))
    G["servers"] = [RemoteServer(s) for s in current_addrs]
    return G["servers"]

def random_server():
    if G["servers"] is None:
        get_servers()
    if not G["servers"]:
        return
    return random.choice(G["servers"])



if __name__ == '__main__':
    if __name__ == '__main__':
        for i in range(100):
            server = random_server()
            if not server:
                break
            try:
                out, result = server.ping("ireader %d" % i)
                print server.addr, out, result
            except Exception, ex:
                server.close()
                print ex

