#!/usr/bin/env python3

from itertools import groupby
from operator import itemgetter
import sys

data = map(lambda x: x.strip().split('\t'), sys.stdin)
for word, group in groupby(data, itemgetter(0)):
    total = sum(int(count) for _, count in group)
    print('%s\t%d' % (word, total))
