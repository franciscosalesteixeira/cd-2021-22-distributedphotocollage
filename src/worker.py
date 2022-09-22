import json
import os
import socket
import shutil

from src.utilities import *


class Worker:

    def __init__(self, broker_address):
        self.broker_address = broker_address
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.connected = False
        self.finished = False
        self.sock.settimeout(1000)

    def resize_reply(self, height):
        img = recv_img(self.sock, self.sock.getsockname()[1])
        basename = os.path.basename(img)
        print("Received " + basename)
        resize_img(img, height)
        addr = self.broker_address.split(":")
        addr = (addr[0], int(addr[1]))
        reply = json.dumps({
            "type": "reply",
            "task": "resize",
        })
        self.sock.sendto(reply.encode(), addr)
        send_img(self.sock, addr, img)
        print("Resizing completed" + "\n")

    def merge_reply(self):
        img1 = recv_img(self.sock, self.sock.getsockname()[1])
        img2 = recv_img(self.sock, self.sock.getsockname()[1])
        basename1 = os.path.basename(img1)
        basename2 = os.path.basename(img2)
        print("Received " + basename1 + " and " +
              basename2 + " to merge" + "\n")
        img = merge_img(img1, img2, self.sock.getsockname()[1])
        addr = self.broker_address.split(":")
        addr = (addr[0], int(addr[1]))
        reply = json.dumps({
            "type": "reply",
            "task": "merge"
        })
        self.sock.sendto(reply.encode(), addr)
        send_img(self.sock, addr, img)
        print("Merge Completed\n")

    @timeout(500)
    def process_messages(self):
        json_msg = self.sock.recvfrom(4096)
        if json_msg:
            if(str(json_msg[0]).startswith("b'{")):
                msg = json.loads(json_msg[0])
                if msg["type"] == "request":
                    if msg["request"] == "resize":
                        self.resize_reply(msg["height"])
                    if msg["request"] == "merge":
                        self.merge_reply()
                if msg["type"] == "status":
                    if msg["status"] == "finish":
                        self.finished = True
                        shutil.rmtree(str(self.sock.getsockname()[1]))
                        self.sock.close()

    def loop(self):

        try:

            while not self.finished:

                if not self.connected:
                    address = self.broker_address.split(":")
                    self.sock.connect((address[0], int(address[1])))
                    ready_msg = json.dumps({
                        "type": "status",
                        "status": "ready",
                        "id": self.sock.getsockname()[1]
                    })
                    self.sock.send(ready_msg.encode())
                    self.connected = True

                try:
                    self.process_messages()

                except Exception as e:
                    print("Broker might have dc'ed!")
                    shutil.rmtree(str(self.sock.getsockname()[1]))
                    self.sock.close()

        except Exception as e:
            shutil.rmtree(str(self.sock.getsockname()[1]))
            self.sock.close()
            print(e)
