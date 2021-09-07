from plumbersdk import Processor, PlumberResponse
import os
import time

env = {}

processor = Processor(__name__)


@processor.handler
def my_handler(data, ctx):
    data["version"] = env["VERSION"]
    if env["SLEEP_TIME"] != "":
        time.sleep(float(env["SLEEP_TIME"]))
    return PlumberResponse.newResultOk(data)


def main():
    env["VERSION"] = os.getenv("VERSION")
    env["SLEEP_TIME"] = os.getenv("SLEEP_TIME")
    processor.run()


if __name__ == "__main__":
    main()
