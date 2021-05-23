from plumbersdk import Processor, PlumberResponse

env = {}

processor = Processor(__name__)


@processor.handler
def my_handler(data, ctx):
    ctx.logger.info(data["msg"])
    data["msg"] = data["msg"] + ", " + env['MSG']
    ctx.logger.info(data)
    return PlumberResponse.newResultOk(data)


def main():
    env['MSG'] = "test"
    processor.run()


if __name__ == "__main__":
    main()
