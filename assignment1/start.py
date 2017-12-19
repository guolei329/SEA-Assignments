import logging, socket, hashlib, getpass
from tornado.ioloop import IOLoop
from tornado import web, gen, process, httpserver, httpclient, netutil

log = logging.getLogger(__name__)

# Pick a base port based on username
MAX_PORT = 49152
MIN_PORT = 10000
BASE_PORT = int(hashlib.md5(getpass.getuser().encode()).hexdigest()[:8], 16) % \
    (MAX_PORT - MIN_PORT) + MIN_PORT
# Static backend configuration
NUM_BACKENDS = 3
BACKENDS = ["{host}:{port}".format(host=socket.gethostname(), port=BASE_PORT + i + 1) for i in range(NUM_BACKENDS)]

# Main entry point
class FrontendHandler(web.RequestHandler):
    next_backend_ix = 0 # class-scope variable

    @gen.coroutine
    def get(self):
        http = httpclient.AsyncHTTPClient()
        response = yield http.fetch("http://" + BACKENDS[FrontendHandler.next_backend_ix])
        FrontendHandler.next_backend_ix = (FrontendHandler.next_backend_ix + 1) % len(BACKENDS)
        if response.error: raise web.HTTPError(500)
        self.finish(response.body)

# Backend
class BackendHandler(web.RequestHandler):
    def initialize(self, port):
        self._port = port

    def get(self):
        self.finish("{host}:{port}".format(host=socket.gethostname(), port=self._port))

def main():
    task_id = process.fork_processes(4)
    if task_id == 0:
        app = httpserver.HTTPServer(web.Application([web.url(r"/", FrontendHandler)]))
        app.add_sockets(netutil.bind_sockets(BASE_PORT))
        log.info("Front end is listening on %d", BASE_PORT)
    else:
        port = BASE_PORT + task_id
        app = httpserver.HTTPServer(web.Application([web.url(r"/", BackendHandler, dict(port=port))]))
        app.add_sockets(netutil.bind_sockets(port))
        log.info("Back end %d listening on %d", task_id, port)
    IOLoop.current().start()

if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s - %(asctime)s - %(message)s', level=logging.DEBUG)
    main()
