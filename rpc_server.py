import sys
import time
import errno
import signal
import os
import json
import struct
import socket
import asyncore
from cStringIO import StringIO
from kazoo.client import KazooClient


class RPCHandler(asyncore.dispatcher_with_send):


    def __init__(self, sock, addr):
        asyncore.dispatcher_with_send.__init__(self, sock=sock);
        self.addr = addr
        self.handers = {'ping': self.ping}
        self.rbuf = StringIO()

    def handle_connect(self):
        print self.addr, 'comes'

    def handle_close(self):
        print self.addr, 'bye'
        self.close()

    def handle_read(self):
        while True:
            content = self.recv(1024)
            if content:
                self.rbuf.write(content)
            if len(content) < 1024:
                break
        self.handle_rpc()

    def handle_rpc(self):
        while True:
            self.rbuf.seek(0)
            lenth_prefix = self.rbuf.read(4)
            if len(lenth_prefix) < 4:
                break
            length, = struct.unpack('I', lenth_prefix)
            body = self.rbuf.read(length)
            if len(body) < length:
                break
            request = json.loads(body)
            print 'request %s' % request
            in_ = request['in']
            params = request['params']
            handler = self.handers[in_]
            handler(params)
            left = self.rbuf.getvalue()[length + 4:]
            self.rbuf = StringIO()
            self.rbuf.write(left)
        self.rbuf.seek(0, 2)

    def ping(self, params):
        self.send_result('pong', params)

    def send_result(self, out, result):
        response = {'out': out, 'result': result + 'hello' + '\t' +str(os.getpid())}
        print 'response %s' % response
        body = json.dumps(response)
        length_prefix = struct.pack('I', len(body))
        self.send(length_prefix)
        self.send(body)


class RPCServer(asyncore.dispatcher):

    zk_root = '/demo'
    zk_rpc = zk_root + '/rpc'

    def __init__(self, host, port, zoo_host):
        asyncore.dispatcher.__init__(self)
        self.host = host
        self.port = port
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(1)
        self.zoo_host = zoo_host
        self.child_pids = []

        self.pid = os.getpid()
        if self.prefork(10):
            self.register_zk()
            self.register_parent_signal()
        else:
            self.register_child_signal()

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            sock, addr = pair
            RPCHandler(sock, addr)

    def prefork(self, n):
        for i in range(n):
            pid = os.fork()
            if pid < 0:
                return
            if pid > 0:
                self.child_pids.append(pid)
                continue
            if pid == 0:
                return False
        return True

    def register_zk(self):
        self.zk = KazooClient(hosts=self.zoo_host)
        self.zk.start()
        self.zk.ensure_path(self.__class__.zk_root)
        value = json.dumps({'host': self.host, 'port': self.port})
        self.zk.create(self.__class__.zk_rpc, value, ephemeral=True, sequence=True)

    def exit_parent(self, sig, frame):
        self.zk.stop()
        self.close()
        asyncore.close_all()
        pids = []
        for pid in self.child_pids:
            print 'before kill'
            try:
                os.kill(pid, signal.SIGINT)
                pids.append(pid)
            except OSError as ex:
                if ex.args[0] == errno.ECHILD:
                    continue
                raise ex
            print 'after kill %d ' % pid
        for pid in pids:
            while True:
                try:
                    os.waitpid(pid, 0)
                    break
                except OSError as ex:
                    if ex.args[0] == errno.ECHILD:
                        break
                    if ex.args[0] != errno.EINTR:
                        raise ex
            print 'wait over %d' % pid

    def reap_child(self, sig, frame):
        print 'before reap'
        while True:
            try:
                info = os.waitpid(-1, os.WNOHANG)
                break
            except OSError as ex:
                if ex.args[0] == errno.ECHILD:
                    break
                if ex.args[0] != errno.EINTR:
                    raise ex
        pid = info[0]
        try:
            self.child_pids.remove(pid)
        except ValueError:
            pass
        print 'after reap %d' % pid

    def register_parent_signal(self):
        signal.signal(signal.SIGINT, self.exit_parent)
        signal.signal(signal.SIGTERM, self.exit_parent)
        signal.signal(signal.SIGCHLD, self.reap_child)

    def exit_child(self, sig, frame):
        self.close()
        asyncore.close_all()
        print 'all closed'

    def register_child_signal(self):
        signal.signal(signal.SIGINT, self.exit_child)
        signal.signal(signal.SIGTERM, self.exit_child)





if __name__ == '__main__':
    host = sys.argv[1]
    port = sys.argv[2]
    zoo_host = sys.argv[3]
    s = RPCServer(host, int(port), zoo_host)
    print os.getpid()
    print 'start..'
    asyncore.loop()

