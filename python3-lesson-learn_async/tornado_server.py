#!/usr/bin/env python
# encoding: utf-8

import tornado.ioloop
import tornado.web
import time
import datetime
from concurrent.futures import ThreadPoolExecutor
import tornado.httpserver


class MainHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)

    @tornado.gen.coroutine
    def get(self):
        # print("req begin id", id(self))
        print(datetime.datetime.now())
        time.sleep(1)
        self.write("Hello, world")
        print("reqed")


def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
    ])


if __name__ == "__main__":
    app = make_app()
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.bind(8080)
    http_server.start(10)
    tornado.ioloop.IOLoop.current().start()

