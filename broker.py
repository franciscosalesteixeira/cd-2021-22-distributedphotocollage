
import argparse
from src.broker import Broker

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "folder", help="folder containing the initial images", type=str
    )
    parser.add_argument(
        "height", help="height of the collage image", type=int
    )
    args = parser.parse_args()
    b = Broker(args.folder, args.height)
    b.loop()