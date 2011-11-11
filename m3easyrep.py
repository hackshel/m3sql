import pymysql
import easyrep
import types
import easysqllite as esql

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

    def getTableMeta( self, ):
        tablest = {}
        tableKeys = {}

        tableMeta = self.conn.gets( ( self.DB_SINASTORE, self.TABLE_TABLEMETA, ), order = ['Index'] )
        for meta in tableMeta:
            tb = (self.DB_SINASTORE, meta['Table'])
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