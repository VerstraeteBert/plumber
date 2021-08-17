from plumbersdk import Processor, PlumberResponse
import os
import time

env = {}

processor = Processor(__name__)


@processor.handler
def my_handler(data, ctx):
    time.sleep(float(env['SLEEP']))
    ctx.logger.info(f"original msg: {data['msg']}")
    data["msg"] = data["msg"] + ", " + env['MSG']
    ctx.logger.info(f"transformed msg: {data['msg']}")
    return PlumberResponse.newResultOk(data)


def main():
    env['MSG'] = os.getenv("MSG")
    env['SLEEP'] = os.getenv("SLEEP")
    processor.run()


if __name__ == "__main__":
    main()
