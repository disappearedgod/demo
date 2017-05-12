from pymongo import MongoClient
import ConfigParser as ConfigParser

class mongo:
    def __init__(self):
        cp = ConfigParser.SafeConfigParser()
        cp.read('../myapp.conf')
        self.port = int(cp.get('mongo', 'port'))
        self.mongo_host = cp.get('mongo', 'host')

    def set_up(self):
        self.mongo = MongoClient(self.mongo_host, self.port)
        print "mongo set_up fin"

if __name__ == '__main__':
