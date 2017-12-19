import hashlib, getpass

NUM_WORKERS = 4
MAX_PORT = 49152
MIN_PORT = 10000
BASE_PORT = int(hashlib.md5(getpass.getuser().encode()).hexdigest()[:8], 16) % (MAX_PORT - MIN_PORT) + MIN_PORT - 800

servers = {}
servers['worker'] = ["127.0.0.1:%d" % (port)
  for port in range(BASE_PORT, BASE_PORT + NUM_WORKERS)]


