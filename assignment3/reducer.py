import json, urllib.request, urllib.parse, urllib.error, heapq, subprocess, os.path
from tornado import web, gen, httpclient
from . import inventory

WORKERS = inventory.servers['worker']

class RetrieveReduceOutput(web.RequestHandler):
    def head(self):
        self.finish()

    def get(self):
        job_path = self.get_argument('job_path')
        num_reducers = int(self.get_argument('num_reducers'))
        self.write('<pre>')
        for filename in [os.path.join(job_path, '%d.out') % i for i in range(num_reducers)]:
            self.write(filename + ':\n' + str(open(filename, 'r').read()) + '\n')
        self.finish()

class Reduce(web.RequestHandler):
    def head(self):
        self.finish()

    @gen.coroutine
    def get(self):
        reducer_ix = int(self.get_argument('reducer_ix'))
        reducer_path = self.get_argument('reducer_path')
        job_path = self.get_argument('job_path')
        map_task_ids = self.get_argument('map_task_ids').split(',')

        http = httpclient.AsyncHTTPClient()

        # The Shuffle
        params = [urllib.parse.urlencode({'reducer_ix': reducer_ix, 'map_task_id': map_task_id}) for map_task_id in map_task_ids]
        futures = [http.fetch('http://%s/retrieve_map_output?%s' % (WORKERS[i % len(WORKERS)], p)) for i, p in enumerate(params)]
        raw_responses = yield futures

        # Merge
        kv_pairs = heapq.merge(*[json.loads(r.body.decode()) for r in raw_responses])

        self._reducer(kv_pairs, reducer_path, os.path.join(job_path, '%d.out' % reducer_ix))
        self.write(json.dumps({'status': 'success'}))
        self.finish()

    def _reducer(self, kv_pairs, reducer_path, output_file):
        with open(output_file, 'w') as output:
            p = subprocess.Popen([reducer_path], stdin=subprocess.PIPE, stdout=output)
            for pair in kv_pairs:
                p.stdin.write(('%s\t%s\n' % (pair[0], pair[1])).encode())
            p.stdin.close()
            p.wait()
