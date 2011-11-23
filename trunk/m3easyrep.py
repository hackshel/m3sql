import pymysql
import easyrep
import types
import easysqllite as esql
import easyprotocol as ezp

class M3EasyReplication( easyrep.EasyReplication ):
    com_binlog_dump = 18
    DB_SINASTORE = 'SinaStore'
    TABLE_TABLEMETA = 'tablemetas'
    
    def __init__( self, db, location, tablefilter=None, ):
        self.conn = esql.ConnLite( pymysql.connect( **db ) )
        self.dbarg = db
        self.serverid = 1
        
        self.logname, self.pos = location or (None, None)
        self.tablest, self.tableKeys = self.getTableMeta()
        
        if self.tablest == None :
            raise Exception, 'can not get table meta'
        
        if type(tablefilter) == types.TupleType :
            
            if len(tablefilter) != 2 or tablefilter[0] == None :
                raise TypeError, 'tablefilter argument type error'
            
            if tablefilter[1] == None :
                tf = tablefilter[0]
                tablefilter = lambda x : ( x[0] == tf )
            else :
                tf = tablefilter
                tablefilter = lambda x : ( x == tf )
        
        self.tablefilter = tablefilter or ( lambda x : True )
        self.dumpfilter = lambda x : True
        self.ebp.namespaces['rows'].tablefilter = self.tablefilter

        self.run_pos = None
        
    def querybinlog( self, ):
        self.conn.querybinlog( self.pos, self.serverid, self.logname, self.com_binlog_dump )
        
    def readloop( self, onconn=None, evyield=False ):
        
        #arg = struct.pack('<LHLs',self.pos,0,self.serverid,self.logname)
        
        #if (self.logname == None and self.pos == None) or self.tablest == None :
        #    for r in self.firstdump():
        #        yield r
        
        self.querybinlog()
        if onconn != None :
            onconn(self.conn, self.logname, self.pos, self.tablest)
            
        coltype = ()
        metadata = ()
        idt = {}
        
        self.run_pos = self.pos
        
        while(True):
            
            if evyield :
                yield EventWait
            
            try :
                r = self.ebp.read( 'binlog', self.conn.conn.socket.makefile(), 
                                         extra_headers_length=0,
                                         coltype=coltype, 
                                         metadata=metadata,
                                         tst = self.tablest, idt=idt )
            except ezp.ConnectionError, e:
                self.conn = pymysql.connect( **self.dbarg )
                self.querybinlog()
                if onconn != None :
                    onconn(self.conn, self.logname, self.pos, self.tablest)
                continue
            
            try :
                self.run_pos = r['body']['content']['event']['header']['next_position']
            except :
                pass
            
            try :
                if 'error' in r['body']['content'] :
                    err = r['body']['content']['error']
                    raise MySQLBinLogError, (err['state'],err['code'],err['message'])
            except KeyError, e :
                pass
            
            try :
                coltype = r['body']['content']['event']['data']['table']['columns_type']
                metadata = r['body']['content']['event']['data']['table']['metadata']
                idt[r['body']['content']['event']['data']['table']['table_id']] = \
                    (r['body']['content']['event']['data']['table']['dbname'],
                     r['body']['content']['event']['data']['table']['tablename'])
            except KeyError, e :
                pass
            
            try :
                rot = r['body']['content']['event']['data']['rotate']
                self.logname, self.pos = rot['binlog'], rot['position']
                yield (self.logname, self.pos, self.tablest), None, None, None
                continue
            except KeyError, e :
                pass
            
            try :
                op, d = r['body']['content']['event']['data'].items()[0]
            except KeyError, e :
                yield None, None, None, None
                continue
            
            if op == 'query':
                
                _q = d['query'].strip().split()
                qtype = [ q.lower() for q in _q[:2] ]
                if q == ['create','table'] :
                    table = [ x.strip('`') for x in _q[3].split('.',1)]
                    table = [dbname] + table if dbname else table  
                    yield None, table[0], self.stablest, None
                elif q == ['alter','table'] :
                    table = [ x.strip('`') for x in _q[3].split('.',1)]
                    table = [dbname] + table if dbname else table  
                    yield None, table[0], self.stablest, None
                else :
                    yield None, None, None, None
                continue
            
            if op not in ('write_rows','update_rows','delete_rows') :
                yield None, None, None, None
                continue
                
            self.pos = r['body']['content']['event']['header']['next_position']
            t = idt[d['table_id']]
            
            if not self.tablefilter(t) :
                yield None, None, None, None
                continue
            
            if op == 'write_rows' :
                
                for x in d['value'][:-1] :
                    yield None, t, x, None
            
                yield (self.logname, self.pos, self.tablest), t, d['value'][-1], None
            
            elif op == 'update_rows' :
                
                v = zip( d['value'][::2], d['value'][1::2] )
                for b, a in v[:-1] :
                    yield None, t, a, b
                    
                yield (self.logname, self.pos, self.tablest), t, v[-1][1], v[-1][0]
                
            elif op == 'delete_rows' :
                
                for x in d['value'][:-1] :
                    yield None, t, x, None
            
                yield (self.logname, self.pos, self.tablest), t, None, d['value'][-1]
                
            else :
                
                yield None, None, None, None
        
        return

    def getTableMeta( self, ):
        tablest = {}
        tableKeys = {}

        databases = self.conn.getdatabases()
        for db in databases:
            print db
            try:
                tableMeta = self.conn.gets( ( db, self.TABLE_TABLEMETA, ), order = ['Index'] )
            except Exception:
                continue
                
            for meta in tableMeta:
                tb = (db, meta['Table'])
                if meta['Key'] == 'PRI':
                    keyList = tableKeys.get( tb, [], )
                    keyList.append( meta['Field'] )
                    tableKeys[tb] = keyList
                    
                metaList = tablest.get( tb, [] )
                metaList.append( ( meta['Field'], easyrep.RowType.colparse(meta['Type']), ) )
                tablest[tb] = metaList
                
        for k, v in tablest.items():
            tablest[k] = zip(*v)
            
        import pprint
        pprint.pprint( tablest )
        pprint.pprint( tableKeys )
            
        return tablest, tableKeys
    
    def executesqlone( self, query ):
        
        if self.dbarg is None :
            raise UnkownDBArgs, 'unkown db arg'
            
        conn = esql.ConnLite( pymysql.connect( **db ) )

        return conn.read( query )
        
    def getUptime( self ):
        
        if self.dbarg is None :
            raise UnkownDBArgs, 'unkown db arg'
            
        conn = esql.ConnLite( pymysql.connect( **self.dbarg ) )

        return conn.getUptime()
        
    def getcols( self, tb ):
        
        if self.dbarg is None :
            raise UnkownDBArgs, 'unkown db arg'
            
        conn = esql.ConnLite( pymysql.connect( **self.dbarg ) )

        return conn.getcols( tb )