# -*- coding: utf-8 -*-
import redis
import sys
import random

if __name__ == '__main__':
    r = redis.Redis(host=sys.argv[1], port=sys.argv[2], db=0)
    s = set()
    for i in range(200):
        k = random.random()
        v = random.random()
        r.set(k, v)
        s.add(k)
    for e in s:
        r.delete(e)

