import argparse
from src.worker import Worker

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "broker_address", help="address of the broker to connect to", type=str
    )
    args = parser.parse_args()
    w = Worker(args.broker_address)
    w.loop()