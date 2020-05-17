# -*- coding: utf-8 -*-
import redis
import time
import sys
import random

def test():
    repeat = 200
    if(len(sys.argv)==4):
        repeat = int(sys.argv[3])
    
    r = redis.Redis(host=sys.argv[1], port=sys.argv[2], db=0)
    s = set()
    d = dict()
    begin = time.time()
    for i in range(repeat):
        k = str(random.random())
        v = str(random.random())
        assert(r.set(k, v))
        s.add(k)
        d[k]=v
    end = time.time()
    print('average response time of set:',(end-begin)/repeat)

    begin = time.time()
    for k in s:
        v = r.get(k)
        if(v == None):
           print("value of key:"+k +" is missing")
           return
        v = v.decode('utf-8')
        if(d[k] != v):
           print("value of key:"+k +" is wrong.expected:"+d[k]+",actual:" + v)
           return
    end = time.time()
    print('average response time of get:', (end-begin)/repeat)

    begin = time.time()    
    for e in s:
        if(1 != r.delete(e)):
           print("delete value failed. Key is :"+k)
           return
    end = time.time()
    print('average response time of del:', (end-begin)/repeat)
    
if __name__ == '__main__':
    test()
