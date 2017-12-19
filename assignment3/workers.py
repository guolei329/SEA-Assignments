import logging
from tornado.ioloop import IOLoop
from tornado import web, process, httpserver, netutil
from . import inventory, mapper, reducer, coordinator
log = logging.getLogger(__name__)

def main():
    task_id = process.fork_processes(inventory.NUM_WORKERS, max_restarts=0)
    port = inventory.BASE_PORT + task_id
    app = httpserver.HTTPServer(web.Application([
        web.url(r'/map', mapper.Map),
        web.url(r'/retrieve_map_output', mapper.RetrieveMapOutput),
        web.url(r'/reduce', reducer.Reduce),
        web.url(r'/retrieve_reduce_output', reducer.RetrieveReduceOutput),
        web.url(r'/coordinator', coordinator.Runner)]))
    log.info('Worker %d listening on %d', task_id, port)

    app.add_sockets(netutil.bind_sockets(port))
    IOLoop.current().start()

if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s - %(asctime)s - %(message)s', level=logging.DEBUG)
    main()
