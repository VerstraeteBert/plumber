import logging
from collections import Callable
import json

from flask import Flask, request
from cloudevents.http import from_http, CloudEvent
from http import HTTPStatus
from enum import Enum


# TODO when implementing observability aspects, ensure the format here is consistent with all other components
def _initLogger(name):
    # Define the log format
    log_format = (
        "[%(asctime)s] %(levelname)-8s %(name)-12s %(message)s")
    logging.basicConfig(
        # Define logging level
        level=logging.DEBUG,
        # Declare the object we created to format the log messages
        format=log_format,
        # Declare handlers
        handlers=[
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(name)


class HandlerCtx:
    def __init__(self, logger):
        self.logger = logger


class PlumberAction(Enum):
    OK = HTTPStatus.OK
    DROP = HTTPStatus.BAD_REQUEST


class PlumberResponse:
    def __init__(self, data, action):
        self._data = data
        self._action = action

    def getData(self):
        return self._data

    def isResponseDrop(self) -> bool:
        return self._action == PlumberAction.DROP

    def isResponseOk(self) -> bool:
        return self._action == PlumberAction.OK

    @staticmethod
    def newResultOk(data):
        return PlumberResponse(data, PlumberAction.OK)

    @staticmethod
    def newResultDrop():
        return PlumberResponse("", PlumberAction.DROP)


errorResult = (
    "",
    PlumberAction.DROP.value
)


# noinspection PyBroadException
class Processor:
    def __init__(self, name):
        self._name = name
        self._logger = _initLogger(name)
        self._reqHandler = None
        self._flaskApp = Flask(name)
        self._handlerCtx = HandlerCtx(self._logger)

    def handler(self, userhandlefunc: Callable[[dict, HandlerCtx], PlumberResponse]):
        def inner():
            # 1. pre middleware
            try:
                clevent = self._handle_ingress()
            except Exception as Argument:
                self._logger.exception(self)
                return errorResult
            # 2. run user logic
            #   if processor logic throws any errors -> drop the message
            try:
                result = userhandlefunc(clevent.data, self._handlerCtx)
                if not isinstance(result, PlumberResponse):
                    self._logger.error(
                        f"handler function did not return a PlumberResponse object, got {type(result)} instead")
                    result = PlumberResponse("", PlumberAction.DROP)
            except Exception as Argument:
                self._logger.exception(self)
                result = PlumberResponse("", PlumberAction.DROP)
            # 3. post middleware
            return self._handle_egress(result)

        self._reqHandler = inner

    def _handle_ingress(self) -> CloudEvent:
        evt = from_http(dict(request.headers), request.get_data())
        return evt

    def _handle_egress(self, result) -> (str, int):
        if result.isResponseOk():
            return (
                json.dumps(result.getData()),
                result._action.value,
            )
        else:
            return errorResult

    def run(self):
        if self._reqHandler is None:
            exception = Exception("no event handler registered")
            self._logger.exception(exception)
            raise exception

        app = Flask(self._name)
        app.add_url_rule('/', 'handle', self._reqHandler, None, methods=['POST'])
        app.run(port=8080, debug=True)
