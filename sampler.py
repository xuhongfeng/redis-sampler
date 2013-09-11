#!/usr/bin/env python

"""

@author xuhongfeng <life.xhf@gmail.com>

"""

import argparse
from argparse import RawTextHelpFormatter
import redis

class Sampler(object):
    def __init__(self, host, port, sampleSize=None, password=None):
        self.host = host
        self.port = port
        self.sampleSize = sampleSize
        self.password = password

    def _setup(self):
        self.conn = redis.StrictRedis(host=self.host, port=self.port, db=0)
        self.dbSize = self.conn.dbsize()
        if not self.sampleSize:
            self.sampleSize = int(0.1 * self.dbSize)

    def info(self):
        print 'INFO host=%s' % self.host
        print 'INFO port=%d' % self.port
        print 'INFO dbSize=%d' % self.dbSize
        print 'INFO sampleSize=%d' % self.sampleSize

    def start(self):
        self._setup()
        self.info()
        if self.dbSize == 0:
            return

        trie = Trie()
        """
        for i in range(self.sampleSize):
            key = self.conn.randomkey()
        """
        for key in self.conn.keys():
            objInfo = self.conn.debug_object(key)
            objSize = objInfo['serializedlength']
            trie.insert(key, objSize)

        def cbk(node):
            print '%s %s\t%d' % ('>'*node.depth, node.prefix, node.bytes)

        trie.dfs(cbk)



class Trie(object):
    def __init__(self):
        self.root = Node('others')
    
    def insert(self, key, objSize):
        parts = key.split(':')
        partsCount = len(parts)
        p = self.root

        prefix = ''
        for i in range(partsCount-1):
            part = parts[i]
            if prefix:
                prefix = prefix + ':'
            prefix = prefix + part
            child = p.children.get(part)
            if not child:
                child = Node(prefix + ':*')
                child.depth = p.depth + 1
                p.children[part] = child
            p = child
            p.bytes = p.bytes + objSize
        if p is self.root:
            p.bytes = p.bytes + objSize

    def dfs(self, cbk):
        """
            cbk(node)
        """
        for child in self.root.children.itervalues():
            self._dfs(child, cbk)
        cbk(self.root)

    def _dfs(self, node, cbk):
        cbk(node)
        for child in node.children.itervalues():
            self._dfs(child, cbk)

class Node(object):
    def __init__(self, prefix):
        self.prefix = prefix
        self.bytes = 0
        self.children = {}
        self.depth = 0
    



def main():
    epilog = """
        
        EXAMPLES:

        python sampler.py localhost 6379 -s $sampleSize  -a $password

    """
    parser = argparse.ArgumentParser(description='this script sample and analyze the memory usage of the redis server, group by common key prefix', epilog=epilog, formatter_class=RawTextHelpFormatter)
    parser.add_argument('host', help='host of the redis server')
    parser.add_argument('port', type=int, help='port of the redis server')
    parser.add_argument('-s', type=int, dest='sampleSize', help='the sample size, which default is 10%% of the dbSize')
    parser.add_argument('-a', dest='password', help='the password, if the redis server ask for authentication')

    args = parser.parse_args()
    sampler = Sampler(args.host, args.port, sampleSize=args.sampleSize, password=args.password)
    sampler.start()



if __name__ == '__main__':
    main()