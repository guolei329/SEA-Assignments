import pickle, logging
from tornado import web, process, httpserver, netutil
from tornado.ioloop import IOLoop
import inventory

log = logging.getLogger(__name__)

class Gradient(web.RequestHandler):
    def post(self):
        input_file = self.get_argument('input_file')
        compute_gradient, model = pickle.loads(self.request.body)
        self.finish(pickle.dumps(compute_gradient(model, input_file)))

def main():
    task_id = process.fork_processes(inventory.NUM_WORKERS, max_restarts=0)
    port = inventory.BASE_PORT + task_id
    app = httpserver.HTTPServer(web.Application([
        web.url(r'/compute_gradient', Gradient)]))
    log.info('Worker %d listening on %d', task_id, port)

    app.add_sockets(netutil.bind_sockets(port))
    IOLoop.current().start()

if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s - %(asctime)s - %(message)s', level=logging.INFO)
    main()
