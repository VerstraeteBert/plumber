from plumbersdk import Processor, PlumberResponse
import os

env = {}

processor = Processor(__name__)


@processor.handler
def my_handler(data, ctx):
    ctx.logger.info(f"original msg: {data['msg']}")
    data["msg"] = data["msg"] + ", " + env['MSG']
    ctx.logger.info(f"transformed msg: {data['msg']}")
    return PlumberResponse.newResultOk(data)


def main():
    env['MSG'] = os.getenv("MSG")
    processor.run()


if __name__ == "__main__":
    main()
