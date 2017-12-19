import urllib.parse, glob, argparse, importlib, logging
from tornado.ioloop import IOLoop
from tornado import gen, httpclient
import inventory
import cloudpickle as pickle

WORKERS = inventory.servers['worker']

log = logging.getLogger(__name__)

class Coordinator:
    def __init__(self, **job_args):
        self._app = importlib.import_module(job_args['app'])
        self._input_files = glob.glob(job_args['job_path'] + '/*.in')
        self._job_args = job_args

    def run(self):
        IOLoop.current().run_sync(self.run_coroutine)

    @gen.coroutine
    def run_coroutine(self):
        http = httpclient.AsyncHTTPClient()
        model = self._init_model()
        for _ in range(int(self._job_args['iterations'])):
            gradients = yield self._gradient(http, model)
            self._update(gradients, model)
        with open(self._job_args['job_path'] + '/0.out', 'wb') as f:
            pickle.dump(model, f)

    def _init_model(self):
        return self._app.init_model(self._input_files)

    @gen.coroutine
    def _gradient(self, http, model):
        params = [urllib.parse.urlencode({'input_file': input_file})
                  for input_file in self._input_files]
        body = pickle.dumps((self._app.compute_gradient, model))
        futures = [http.fetch('http://%s/compute_gradient?%s' % (WORKERS[i % len(WORKERS)], p),
                              method='POST', body=body, headers={'Content-Type': 'application/traindist'})
                   for i, p in enumerate(params)]
        raw_responses = yield futures
        gradients = [pickle.loads(r.body) for r in raw_responses]
        raise gen.Return(gradients)

    def _update(self, gradients, model):
        for gradient in gradients:
            self._app.update_model(model, gradient)

if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s - %(asctime)s - %(message)s', level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--app', required=True)
    parser.add_argument('--job_path', required=True)
    parser.add_argument('--iterations', required=False, type=int, default=100)
    args = parser.parse_args()
    Coordinator(**vars(args)).run()


