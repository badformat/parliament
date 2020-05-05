# -*- coding: utf-8 -*-
import redis
import sys
import random

if __name__ == '__main__':
    r = redis.Redis(host=sys.argv[1], port=sys.argv[2], db=0)
    for i in range(200):
        r.set(random.random(), random.random())
        r.delete(random.random())
