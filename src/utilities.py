from functools import wraps
import json
import os
import random
import signal
import string
import time
from PIL import Image, ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True

def timeout(timeout_secs: int):
    def wrapper(func):
        @wraps(func)
        def time_limited(*args, **kwargs):
            def handler(signum, frame):
                raise Exception(f"Timeout for function '{func.__name__}'")
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(timeout_secs)
            result = None
            try:
                result = func(*args, **kwargs)
            except Exception as exc:
                raise exc
            finally:
                signal.alarm(0)
            return result
        return time_limited
    return wrapper

def delete_img(image_path):
    os.remove(image_path) 

def send_img(sock, addr, image):
    finished = False
    basename = os.path.basename(image)
    basename = basename.rsplit(".", 1)
    while not finished:
        title = json.dumps({
            "type": "info",
            "info": "title",
            "title": basename
        })
        sock.sendto(title.encode(), addr)
        f = open(image, "rb") 
        data = f.read(4096)
        while data:
            if sock.sendto(data, addr):
                data = f.read(4096)
                time.sleep(.001)
        f.close()
        sock.sendto(b"end image", addr)
        finished = True

def recv_img(sock, folder):
    finished = False
    json_msg = sock.recvfrom(4096)
    if json_msg:
        msg = json.loads(json_msg[0])
        if msg["type"] == "info" and msg["info"] == "title":
           image_name = msg["title"]
    data, addr = sock.recvfrom(4096)
    if(os.path.isdir(str(folder))):
        dir = str(folder)
    else:
        dir = new_directory(folder)
    f = open(dir + "/" + str(image_name[0]) + "." + str(image_name[1]), "wb")
    while not finished:
        if not f.closed:
            f.write(data)
        data, addr = sock.recvfrom(4096)
        if data == b"end image":
            f.close()
            finished = True
    return f.name
    
def resize_img(image_name, pref_height):
    image = Image.open(image_name)
    size = image.size
    width, height = size
    ratio = height / pref_height
    im = image.resize((int(width/ratio), pref_height), Image.ANTIALIAS)
    im.save(image_name)

def merge_img(first_image, second_image, folder):
    first_image = os.path.basename(first_image)
    second_image = os.path.basename(second_image)
    image1 = Image.open(str(folder) + "/" + first_image)
    image2 = Image.open(str(folder) + "/" + second_image)
    new_image = Image.new('RGB', (image1.width + image2.width, image1.height))
    new_image.paste(image1, (0,0))
    new_image.paste(image2, (image1.width,0))
    name = str(folder) + "/" + ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(10)) + ".png"
    new_image.save(name)
    return name

def new_directory(folder):
    current_directory = os.getcwd()
    final_directory = os.path.join(current_directory, str(folder))
    if not os.path.exists(final_directory):
        os.makedirs(final_directory)
    return str(final_directory)

def addr_format(worker_addr):
    return worker_addr[0] + ":" + str(worker_addr[1])

def get_height(image_path):
    img = Image.open(str(image_path))
    return img.height
