import json, subprocess, hashlib, time
from tornado import web
from collections import defaultdict
from itertools import chain

class RetrieveMapOutput(web.RequestHandler):
    def head(self):
        self.finish()

    def get(self):
        partition = self.get_argument('reducer_ix')
        map_task_id = self.get_argument('map_task_id')
        pairs = chain.from_iterable([[(key, value) for value in values]
                       for key, values in Map.map_outputs[map_task_id][int(partition)]])
        self.write(json.dumps(list(pairs)))

class Map(web.RequestHandler):
    map_outputs = defaultdict(lambda: defaultdict(list))

    def head(self):
        self.finish()

    def get(self):
        mapper_path = self.get_argument('mapper_path')
        num_reducers = self.get_argument('num_reducers')
        input_file = self.get_argument('input_file')
        partitioner_type = self.get_argument('partitioner_type', DIGEST_PARTITIONER_TYPE)

        num_reducers = int(num_reducers)
        partitioner = PARTITIONERS[partitioner_type]
        map_task_id = hashlib.md5((mapper_path + input_file + str(time.time())).encode()).hexdigest()

        out_by_partition = defaultdict(lambda: defaultdict(list))
        for k_out, v_out in self._mapper(mapper_path, input_file):
            out_by_partition[partitioner(k_out, num_reducers)][k_out].append(v_out)
        for p in out_by_partition.keys():
            Map.map_outputs[map_task_id][p] = [(k, out_by_partition[p][k]) for k in sorted(out_by_partition[p].keys())]
        self.write(json.dumps({'status': 'success', 'map_task_id': map_task_id}))

    def _mapper(self, mapper_path, input_file):
        p = subprocess.Popen(mapper_path, stdin=open(input_file), stdout=subprocess.PIPE)
        for line in p.stdout:
            line = line.decode().strip()
            if len(line) == 0:
                continue
            yield line.split('\t', 1)

DIGEST_PARTITIONER_TYPE = 'digest'
INTEGER_PARTITIONER_TYPE = 'integer'
PARTITIONERS = {DIGEST_PARTITIONER_TYPE: lambda key, num_reducers: int(hashlib.md5(key.encode()).hexdigest()[:8], 16) % num_reducers,
                INTEGER_PARTITIONER_TYPE: lambda key, num_reducers: int(key) % num_reducers}

