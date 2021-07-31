import ast
import sys
import time
import traceback
from _ast import *

import zmq
import matplotlib
from matplotlib.backend_bases import Gcf

from session import Session, Message
from stream import InStream, OutStream
from display import BlackHoleDisplayHook, DisplayHook
from completer import KernelCompleter

import config
from plt import backend_inline
matplotlib.use("module://plt.backend_inline")


class Core(object):

    def __init__(self):
        ip = config.options["host"]
        port_base = config.options["port"]
        connection = ("tcp://%s" % ip) + ":%i"
        rep_conn = connection % port_base
        pub_conn = connection % (port_base + 1)
        req_conn = connection % (port_base + 2)

        self.session = Session(username="kernel")
        self.orig_hook = BlackHoleDisplayHook()

        self.ctx = zmq.Context()

        self.rep_socket = self.ctx.socket(zmq.ROUTER)
        self.rep_socket.bind(rep_conn)

        self.pub_socket = self.ctx.socket(zmq.PUB)
        self.pub_socket.bind(pub_conn)
        sys.stdout = OutStream(self.session, self.pub_socket, "stdout")
        sys.stderr = OutStream(self.session, self.pub_socket, "stderr")
        self.display_hook = DisplayHook(self.session, self.pub_socket)

        self.req_socket = self.ctx.socket(zmq.DEALER)
        self.req_socket.setsockopt(zmq.LINGER, 0)
        self.req_socket.bind(req_conn)
        sys.stdin = InStream(self.session, self.req_socket, "stdin")

        self.poller = zmq.Poller()
        self.poller.register(self.rep_socket, zmq.POLLIN)

        self.user_ns = {}

        self.completer = KernelCompleter(self.user_ns)
        self.handlers = {}
        for msg_type in ["execute_request", "complete_request"]:
            self.handlers[msg_type] = getattr(self, msg_type)

        print("Starting the kernel...", file=sys.__stdout__)
        print("On:", rep_conn, pub_conn, req_conn, file=sys.__stdout__)
        print("Use Ctrl-C to terminate.", file=sys.__stdout__)

    def start(self):
        while True:
            try:
                socks = dict(self.poller.poll(1000))
                if self.rep_socket in socks and socks[self.rep_socket] == zmq.POLLIN:
                    ident = self.rep_socket.recv()
                    assert self.rep_socket.rcvmore, "Unexpected missing message part."
                    msg = self.rep_socket.recv_json()
                    omsg = Message(msg)
                    print("[server]", ident, omsg, file=sys.__stdout__)
                    handler = self.handlers.get(omsg.msg_type, None)
                    if handler is not None:
                        handler(ident, omsg)
            except KeyboardInterrupt:
                self.req_socket.close()
                self.pub_socket.close()
                self.rep_socket.close()
                self.ctx.destroy()
                break

    def execute_request(self, ident, parent):
        try:
            code = parent.content.code
        except:
            return
        pyin_msg = self.session.msg("pyin", {"code": code}, parent=parent)
        self.pub_socket.send_json(pyin_msg)
        try:
            self.display_hook.set_parent(parent)
            sys.stdin.set_parent(parent)
            sys.stdout.set_parent(parent)
            sys.stderr.set_parent(parent)
            self.run_code(code)
        except:
            if Gcf.get_all_fig_managers():
                matplotlib.pyplot.close("all")

            etype, evalue, tb = sys.exc_info()
            exc_content = {
                "status": "error",
                "traceback": traceback.format_exception(etype, evalue, tb),
                "etype": str(etype),
                "evalue": str(evalue)
            }
            exc_msg = self.session.msg("pyerr", exc_content, parent)
            self.pub_socket.send_json(exc_msg)
            reply_content = exc_content
        else:
            reply_content = {"status": "ok"}

        reply_msg = self.session.msg("execute_reply", reply_content, parent)
        self.rep_socket.send(ident, zmq.SNDMORE)
        self.rep_socket.send_json(reply_msg)
        if reply_msg["content"]["status"] == "error":
            self.abort_queue()

    def run_code(self, code):
        block = ast.parse(code)
        if len(block.body) <= 0:
            raise Exception("Failed to generate abstract syntax tree")
        _ast_Interactive = Interactive()
        sys.displayhook = self.orig_hook
        for source in block.body[:-1]:
            _ast_Interactive.body = [source]
            comp_code = compile(_ast_Interactive, "<zmq-kernel>", "single")
            if comp_code:
                exec(comp_code, self.user_ns, self.user_ns)
        sys.displayhook = self.display_hook
        _ast_Interactive.body = [block.body[-1]]
        comp_code = compile(_ast_Interactive, "<zmq-kernel>", "single")
        if comp_code:
            exec(comp_code, self.user_ns, self.user_ns)

    def complete_request(self, ident, parent):
        matches = {"matches": self.completer.complete(parent.content.text), "status": "ok"}
        self.session.send(self.rep_socket, "complete_reply", matches, parent, ident)

    def abort_queue(self):
        while True:
            try:
                ident = self.rep_socket.recv(zmq.DONTWAIT)
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    break
            assert self.rep_socket.rcvmore, "Unexpected missing message part."
            msg = self.rep_socket.recv_json()
            reply_msg = self.session.msg(msg.msg_type.split("_")[0] + "_reply", {"status": "aborted"}, msg)
            self.rep_socket.send(ident, zmq.SNDMORE)
            self.rep_socket.send_json(reply_msg)
            time.sleep(0.1)


def main():
    kernel = Core()
    backend_inline.kernel = kernel
    kernel.start()


if __name__ == "__main__":
    main()
