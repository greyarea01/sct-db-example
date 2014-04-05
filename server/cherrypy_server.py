#!/usr/bin/env python
import cherrypy
from crateapi2 import CrateApi
from moduleapi import ModuleApi
from jsonpickle import encode
from dbtools import DBTools as DB
import sys

class Api(object):
    def __init__(self,db):
        self.crateapi = CrateApi(db)
        self.moduleapi = ModuleApi(db)
        
    def crates(self,iov='now',cr='list',sl='none',ps='none',md='none',ch='none'):
        if cr=='list':
            sl='none'
            ps='none'
            md='none'
            ch='none'
        if sl=='list':
            ps='none'
            md='none'
            ch='none'
        if ps=='list':
            md='none'
            ch='none'
        if md=='list':
            ch='none'
            
        results = self.crateapi.getInfo(iov,cr,sl,ps,md,ch)
        payload = encode(results,unpicklable=False)
        print 'Delivering payload of size : ',len(payload)
        return payload
    crates.exposed=True

    def modules(self,iov='now',module='none',chip='none'):
        if module=='none':
            return 'Nothing'
        results = self.moduleapi.getInfo(iov,module,chip)
        payload = encode(results,unpicklable=False)
        return payload
    modules.exposed=True
    
class App(object):
        
    def index(self):
        host= cherrypy.request.headers['Host']
        realhost = cherrypy.request.headers['X-Forwarded-Host']
        return 'Hello! I am '+realhost+' though I look like '+host
    index.exposed=True

    
if __name__ == '__main__':
    argc = len(sys.argv)

    db = DB()
    if argc == 1:
        db.initDB()
    else:
        db.initDB(sys.argv[1])

    if db.connected!=True:
        print "Connection failed :("
        exit()
        
    app = App()
    # start building the route /api/ is not exposed
    app.api = Api(db)
    # /api/crates served by CrateApi
    # get it started
    cherrypy.quickstart(app)
    
