import json
from pathlib import Path
import socket
import queue

from src.utilities import *


class Broker:

    def __init__(self, folder, height):
        self.folder = folder
        self.height = height
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.address = "localhost"
        self.port = 5000
        self.sock.bind((self.address, self.port))
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.finished = False
        self.images = sorted(Path(self.folder).iterdir(), key=os.path.getmtime)
        self.original_height = queue.Queue()
        self.goal_height = queue.Queue()
        self.current_workers = []
        self.work_track = {}
        self.total_workers = 0
        self.resize_number = 0
        self.merge_number = 0
        self.mapped = False
        self.working = False
        self.initial_time = 0
        self.total_time_flag = False
        self.resize_initial_time = 0
        self.resize_all_times = []
        self.merge_initial_time = 0
        self.merge_all_times = []

    def tasks(self, worker):

        num_files = len([f for f in os.listdir(self.folder)
                        if os.path.isfile(os.path.join(self.folder, f))])

        if not self.working:

            if self.original_height.qsize() + self.goal_height.qsize() == num_files:
                self.mapped = True

            if not self.mapped:

                for file in self.images:
                    if get_height(file) == self.height:
                        self.goal_height.put(file)
                    else:
                        self.original_height.put(file)

            if not self.working:
                    
                if self.original_height.qsize() > 0:
                    file = self.original_height.queue[0]
                    self.working = True
                    self.resize_req(worker, file)
                    self.resize_initial_time = time.time()

                if not self.total_time_flag:
                    self.initial_time = time.time()
                    self.total_time_flag = True

            if self.mapped:

                if not self.working:

                    if self.goal_height.qsize() > 1 and num_files > 1:
                        file1 = self.goal_height.queue[0]
                        file2 = self.goal_height.queue[1]
                        self.working = True
                        self.merge_req(worker, file1, file2)
                        self.merge_initial_time = time.time()

    def resize_req(self, worker, file):
        self.working = True
        req = json.dumps({
            "type": "request",
            "request": "resize",
            "height": self.height
        })
        self.sock.sendto(req.encode(), worker)
        send_img(self.sock, worker, file)
        print("[BROKER] Send " + os.path.basename(file) +
              " to resize on worker " + addr_format(worker) + "\n")

    def merge_req(self, worker, file1, file2):
        self.working = True
        req = json.dumps({
            "type": "request",
            "request": "merge",
        })
        self.sock.sendto(req.encode(), worker)
        send_img(self.sock, worker, file1)
        send_img(self.sock, worker, file2)
        print("[BROKER] Send " + os.path.basename(file1) + " and " +
              os.path.basename(file2) + " to merge on worker " + addr_format(worker) + "\n")

    def finish(self):
        elapsed_time = time.time()
        total_workers = 0
        for i in self.current_workers:
            req = json.dumps({
                "type": "status",
                "status": "finish",
            })
            self.sock.sendto(req.encode(), i)
        print("Number of resizes: " + str(self.resize_number))
        print("Number of merges: " + str(self.merge_number))
        for worker in self.work_track:
            total_workers += 1
        print("Average number of resizes per worker: " + str(self.resize_number/total_workers))
        print("Average number of merges per worker: " + str(self.merge_number/total_workers))
        if len(self.resize_all_times):
            print("Max time to resize: " + str(round(max(self.resize_all_times), 3)) + "s")
            print("Min time to resize: " + str(round(min(self.resize_all_times), 3)) + "s")
            print("Average time to resize: " + str(round(sum(self.resize_all_times) / len(self.resize_all_times), 3)) + "s")
        if len(self.merge_all_times):
            print("Max time to merge: " + str(round(max(self.merge_all_times), 3)) + "s")
            print("Min time to merge: " + str(round(min(self.merge_all_times), 3)) + "s")
            print("Average time to merge: " + str(round(sum(self.merge_all_times) / len(self.merge_all_times), 3)) + "s")
        for worker in self.work_track:
            resizes = self.work_track[worker][0]
            merges = self.work_track[worker][1]
            print("Worker " + worker + " performed " + str(resizes) + " resizes and " + str(merges) + " merges")
        print("Total time to finish: " + str(round((elapsed_time - self.initial_time), 3)) + "s" )
        self.sock.close()

    @timeout(100)
    def process_messages(self):

        json_msg = self.sock.recvfrom(4096)

        if json_msg:

            if(str(json_msg[0]).startswith("b'{")):
                msg = json.loads(json_msg[0])

                if msg["type"] == "status":

                    if msg["status"] == "ready":
                        self.current_workers.append(json_msg[1])
                        print("Worker connected from: " +
                              addr_format(json_msg[1]))
                        if addr_format(json_msg[1]) not in self.work_track:
                            self.work_track[addr_format(json_msg[1])] = (0,0)

                if msg["type"] == "reply":

                    if msg["task"] == "resize":

                        img = recv_img(self.sock, self.folder)
                        print("[BROKER] Received " + os.path.basename(img) +
                              " after resizing on worker " + addr_format(json_msg[1]) + "\n")
                        self.resize_number += 1
                        self.original_height.get()
                        self.goal_height.put(img)
                        self.working = False
                        old_value = self.work_track[addr_format(json_msg[1])]
                        new_value = old_value[0] + 1, old_value[1]
                        self.work_track[addr_format(json_msg[1])] = new_value 
                        self.resize_all_times.append(time.time() - self.resize_initial_time)

                    if msg["task"] == "merge":
                        img = recv_img(self.sock, self.folder)
                        print("[BROKER] Received " + os.path.basename(img) +
                              " after merge on worker " + addr_format(json_msg[1]) + "\n")
                        self.merge_number += 1
                        img1 = self.goal_height.get()
                        img2 = self.goal_height.get()
                        self.goal_height.put(img)
                        self.working = False
                        delete_img(img1)
                        delete_img(img2)
                        old_value = self.work_track[addr_format(json_msg[1])]
                        new_value = old_value[0], old_value[1] + 1
                        self.work_track[addr_format(json_msg[1])] = new_value 
                        self.merge_all_times.append(time.time() - self.merge_initial_time)
        
        return addr_format(json_msg[1])

    def loop(self):

        worker_flag = True

        print("BROKER in address " + self.address + ":" + str(self.port))

        try:

            while not self.finished:
                
                try:
                    worker_addr = self.process_messages()

                except Exception as e:
                    worker_flag = False
                    print("Worker on " + worker_addr + " is not responding!")

                if len(self.current_workers) > 0:

                    worker = self.current_workers.pop(0)

                    if worker_flag:
                        self.current_workers.append(worker)

                    for i in self.current_workers:
                        self.tasks(i)

                    num_files = len([f for f in os.listdir(
                        self.folder)if os.path.isfile(os.path.join(self.folder, f))])
                    if num_files == 1:
                        self.finished = True
                        self.finish()

                worker_flag = True

        except Exception as e:
            print(e)
            self.finish()