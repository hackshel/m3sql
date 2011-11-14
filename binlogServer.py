import time
import types
import pymysql
import socket

import easysqllite as esql
import m3easyrep

from config import LOCAL_IDC as li
from config import TEST_LOCATION
from config import DB_CONFIG
from config import CALLBACK_CONFIG

from easyrep import UnknownTableDefine

class ConnDisabled(Exception):
    pass

def getIP( s ):
    result = ''
    infos = s.split('.')
    if len( infos ) == 4 and False not in [info.isdigit() for info in infos]:
        return s
    else:
        try:
            result = socket.getaddrinfo( s, 'http' )
            #result = socket.gethostbyaddr(myaddr)
        except Exception, e:
            import traceback
            traceback.print_exc()
            raise
    result = set( [r[4][0] for r in result] )
    return list( result )

def combinDict( names, dicts ):
    combined = {}
    for n, d in zip( names, dicts ):
        if dicts:
            for k, v in d.items():
                combined['%s.%s' %( n, k )] = esql.formatvalue( v )
    return combined

class BinlogServer( object ):
    TABLE_FILTER = ( 'SinaStore', None )
    IDC_PRI = {
        'bj' : 1,
        'gz' : 2,
        'tj' : 3
    }
    DEFAULT_IDC_PRI = 9
    TABLE_HEART_BEAT = 'heartbeat'
    TABLE_ANNALS = 'annals'
    TABLE_CONFIG = 'config'
    COL_ORIGO = 'Origo'
    COL_MARK_DELETE = 'Mark-Delete'
    COL_LAST_MODIFY = 'Last-Modified'
    COL_TIME = 'Time'
    COL_BIN = 'Bin'
    COL_IP = 'IP'
    COL_FILE = 'File'
    COL_OFFSET = 'Offset'
    COL_UPTIME = 'Uptime'
    DB_SINASTORE = 'SinaStore'
    LOCAL_IDC = li
    
    # log the local binlog postion to master database
    POSITION_LOG_INTERVAL = 60
    POSITION_LOG_TABLE = 'POSITION_LOG'

    def __init__( self ):

        localMasterDbConfig = self.getLocalMasterDbConfig()
        self.localMasterConn = esql.Connection( localMasterDbConfig )
        self.verifyIDC()

        sourceMasterDbConfig = self.getSourceMasterDbConfig()
        self.sourceMasterConn = esql.Connection( sourceMasterDbConfig )
        
        self.erep, self.sourceIP, self.sourceLocale = self.establishSourceConn( )
        self.table_keys = self.erep.tableKeys
        
        self.reConnected = False
        
    def getSourceDbConfig( self ):
        return DB_CONFIG[self.LOCAL_IDC]['source']
        
    def getSourceMasterDbConfig( self ):
        return DB_CONFIG[self.LOCAL_IDC]['sourceMaster']
    
    def getLocalMasterDbConfig( self ):
        return DB_CONFIG[self.LOCAL_IDC]['localMaster']
        
    def getSourceConn( self, sourceIPDict, sourceDbConfig, sourceInfos, localOnly = False ):
        
        localPRI = ['local', 'remote']
        if localOnly:
            localPRI = ['local']
        
        for locale in localPRI:
            ipList = sourceIPDict[locale]
            dbConfig = dict( sourceDbConfig[locale] )
            for ip in ipList:
                if ip not in sourceInfos.keys():
                    continue
                dbConfig['host'] = ip
                binlogLocation = self.getBinlogLocation( sourceInfos, ip )
                erep = None
                try:
                    erep = m3easyrep.M3EasyReplication( dbConfig, binlogLocation, self.TABLE_FILTER )
                except Exception:
                    import traceback
                    traceback.print_exc()
                    
                if erep:
                    uptimeNow = self.getSourceDbUptime( erep )
                    uptimeAnnal = sourceInfos[ip][self.COL_UPTIME]
                    if uptimeNow >= uptimeAnnal:
                        return ( erep, ip, locale )

        return ( None, None, None )

    def establishSourceConn( self, isReconnect = False ):

        sourceDbConfig = self.getSourceDbConfig()
        sourceIPDict = self.getsourceIPDict()
        print '*' * 20, sourceIPDict
        while True:
            preTime = ''
            while True:
                markTime, sourceInfos = self.getSourceInfos( preTime, False )
                print markTime, sourceInfos
                # can not get time from annals
                # but first time?
                if not markTime:
                    break
                
                erep, sourceIP, sourceLocale = self.getSourceConn( sourceIPDict, sourceDbConfig, sourceInfos, isReconnect )
                # can not establish connection from source ip list
                if not erep:
                    preTime = markTime
                else:
                    return ( erep, sourceIP, sourceLocale )
                    
                if isReconnect:
                    break
                    
            newsourceIPDict = self.getsourceIPDict()
            if newsourceIPDict == sourceIPDict:
                if isReconnect:
                    return False
                else:
                    print 'can not init the source connection!'
                    exit()
            else:
                sourceIPDict = newsourceIPDict

    def getsourceIPDict( self ):
        sourceIPDict = {}
        for k, v in DB_CONFIG[self.LOCAL_IDC]['source'].items():
            try:
                host = v['host']
                sourceIPDict[k] = getIP( host )
            # can not get ip from source slave's domain name
            except Exception:
                pass
        return sourceIPDict
            
    def getSourceInfos( self, preTime = '', test = True ):
        
        if test:
            return TEST_LOCATION
        else:
            cols = [self.COL_TIME]
            wc = {self.COL_BIN : 1}
            if preTime:
                wc[self.COL_TIME] = ( None, preTime )
            order = [ self.COL_TIME ]
            heartBeatInfos = self.sourceMasterConn.gets( (self.DB_SINASTORE, self.TABLE_HEART_BEAT), where = wc, order = order, reverse = True, limit = 1 )
    
            if heartBeatInfos:
                t = heartBeatInfos[0][self.COL_TIME]
                wc = {self.COL_TIME : t}
                annals = self.sourceMasterConn.gets( (self.DB_SINASTORE, self.TABLE_ANNALS), where = wc )
                result = {}
                for annal in annals:
                    result[annal[self.COL_IP]] = annal
                return ( t, result )
            
            else:
                return ( None, None )

    def getBinlogLocation( self, sourceInfos, sourceIP, test = True ):
        sourceInfo = sourceInfos[sourceIP]
        return ( sourceInfo[self.COL_FILE], sourceInfo[self.COL_OFFSET] )

    def getLocalIDC( self ):
        try:
            result = self.localMasterConn.gets( (self.DB_SINASTORE, self.TABLE_CONFIG) )
            return result[0]['IDC']
        except Exception, e:
            import traceback
            traceback.print_exc()
            raise Exception('Can not get local master config!')
        
    def reConnectSource( self ):
        self.sourceConn = esql.Connection( self.sourceDbConfig )
        
    def reConnectLocalMasterDb( self ):
        localMasterDbConfig = self.getLocalMasterDbConfig()
        self.localMasterConn = esql.Connection( localMasterDbConfig )
        
    def reConnectSourceMasterDb( self ):
        sourceMasterDbConfig = self.getSourceMasterDbConfig()
        self.sourceMasterConn = esql.Connection( sourceMasterDbConfig )

    def verifyIDC( self ):
        if self.LOCAL_IDC != self.getLocalIDC():
            print 'IDC CONFIG WRONG!'
            exit()
            
    #def verifySourceUptime( self, sourceConn, updateAnnal ):
    #    uptimeNow = self.getSourceDbUptime( sourceConn )
    #    if uptimeNow < updateAnnal:
    #        return False
    #    return True

    def processBinlog( self ):
        while True:
            try:
                for binlog in self.erep.readloop():
                    self.duplicateToLocalMaster( binlog )
                    if self.reConnected:
                        self.reConnected = False
                        break
            # when parse binlog, the table's info is not in tablemata, do not process
            except UnknownTableDefine:
                print '[]' * 10, 'ignore'
                continue
            # some exceptions make the connection disabled, reconnect
            except ConnDisabled:
                # reconnect
                while True:
                    result = self.establishSourceConn( )
                    if result:
                        self.erep, self.sourceIP, self.sourceLocale = result
                        break
                    else:
                        time.sleep( 3 )

    def duplicateToLocalMaster( self, binlog ):
        print binlog
        newLocation, tableName, newValues, oldValues = binlog
        if not tableName:
            return
        
        # insert for timepoint to source database
        if tableName == ( self.DB_SINASTORE, self.TABLE_ANNALS ):
            return
        
        if tableName == ( self.DB_SINASTORE, self.TABLE_HEART_BEAT ):
            if newValues[self.COL_BIN] == 0:
                while True:
                    try:
                        self.doHeartBeat( newLocation, newValues )
                        return
                    except Exception:
                        import traceback
                        traceback.print_exc()
            else:
                return
        
        # insert
        if newValues and not oldValues:
            if newValues.get(self.COL_ORIGO, self.LOCAL_IDC) == self.LOCAL_IDC:
                return
            self.processInsert( tableName, newValues )
        # update
        elif newValues and oldValues:
            if newValues[self.COL_ORIGO] == self.LOCAL_IDC:
                if newValues[self.COL_MARK_DELETE] == 1:
                    # mark delete success, do call back
                    print 'call back\n'
                    self.doCallback( 'delete', tableName, newValues, oldValues )
                    # real delete
                    self.doDelete( tableName, oldValues, self.localMasterConn )
                else:
                    return
            else:
                self.processUpdate( tableName, newValues )
        # del
        elif not newValues and oldValues:
            self.doDelete( tableName, oldValues, self.localMasterConn )
        else:
            pass

    #def doLocationLog( self, newLocation ):
    #    logname, pos, tablest = newLocation
    #    
    #    data = {}
    #    data['Time'] = self.logTimePoint
    #    data['logname'] = logname
    #    data['pos'] = pos
    #    data['sourceip'] = self.sourceIP
    #    
    #    self.sourceMasterConn.put( self.POSITION_LOG_TABLE, data)

    def compareIDCPRI( self, sourceIDC, localIDC ):
        sourceIDCPRI = self.IDC_PRI.get( sourceIDC, self.DEFAULT_IDC_PRI )
        localIDCPRI = self.IDC_PRI.get( localIDC, self.DEFAULT_IDC_PRI )

        if sourceIDCPRI < localIDCPRI:
            return True
        elif sourceIDCPRI > localIDCPRI:
            return False
        else:
            return sourceIDC < localIDC
        
    def getComparison( self, sourceIDC):
        if self.compareIDCPRI( sourceIDC, self.LOCAL_IDC ):
            return '<='
        else:
            return '<'
        
    def processInsert(  self, tb, data ):
        try:
            affectRows = self.doInsert( tb, data, self.localMasterConn )
        # duplicatge key
        except pymysql.err.IntegrityError, e:
             affectRows = self.doUpdate( tb, data, self.localMasterConn )
            # before update after insert , local delete happened
    
    def processUpdate( self, tb, data ):
        affectRows = 0
        affectRows = self.doUpdate( tb, data, self.localMasterConn )

        if affectRows == 0:
            try:
                affectRows = self.doInsert( tb, data, self.localMasterConn )
            # duplicatge key, ignore
            except pymysql.err.IntegrityError, e:
                pass

    def doInsert( self, tb, data, conn ):
        return conn.put( tb, data )

    def doUpdate( self, tb, data, conn, extCondition = True ):
        PRIKeys = self.table_keys.get( tb, None )
        if PRIKeys == None:
            PRIKeys = self.getKeys( tb )
            self.table_keys[tb] = PRIKeys
        
        condition = [(k, '=', v) for k, v in data.items() if k in PRIKeys]
        if extCondition:
            condition.append( (self.COL_LAST_MODIFY, self.getComparison( data[self.COL_ORIGO] ), data[self.COL_LAST_MODIFY].strftime('%Y-%m-%d %H:%M:%S')) )
        return conn.update( tb, data, condition )

    def doDelete( self, tb, data, conn ):
        PRIKeys = self.table_keys.get( tb, None )
        if PRIKeys == None:
            PRIKeys = self.getKeys( tb )
            self.table_keys[ tb ] = PRIKeys
           
        condition = [( k, '=', v) for k, v in data.items() if k in PRIKeys]
        condition.append( (self.COL_MARK_DELETE, '=', 1) )
        
        conn.delete( tb, condition )

    def doHeartBeat( self, newLocation, datas ):
        logname, position, tablest = newLocation
        
        annalData = {}
        annalData['Time'] = datas['Time']
        annalData['IP'] = self.sourceIP
        annalData['File'] = logname
        annalData['Offset'] = position
        annalData['Uptime'] = self.getSourceDbUptime( self.erep )

        tb  = ( self.DB_SINASTORE, self.TABLE_ANNALS )
        try:
            self.doInsert( tb, annalData, self.sourceMasterConn )
        # duplicate key, when source from remote idc slave
        except pymysql.err.IntegrityError, e:
            # do update
            self.doUpdate( tb, annalData, self.sourceMasterConn, False )
        
        tb  = ( self.DB_SINASTORE, self.TABLE_HEART_BEAT )
        heartBeatData = {}
        heartBeatData['Time'] = datas['Time']
        heartBeatData['Bin'] = 1
        self.doUpdate( tb, heartBeatData, self.sourceMasterConn, False )

        print self.sourceLocale
        if self.sourceLocale == 'remote':
            # reconnect
            result = self.establishSourceConn( isReconnect = True )
            if result:
                self.erep, self.sourceIP, self.sourceLocale = result
                self.reConnected = True
                
    def doCallback( self, act, tb, newValues, oldValues ):
        config = CALLBACK_CONFIG.get( act, {} )
        print config
        if config:
            print tb, config.keys()
            if tb in config.keys():
                print 'LL' * 20, 'enter call back'
                try:
                    db, sqlFormat = config[tb]
                    con = esql.Connection( db )
                    
                    combinedDict = combinDict( ( 'new', 'old' ), ( newValues, oldValues ) )
                    sql = sqlFormat %combinedDict
                    print '|' * 30, sql
    
                    con.write( sql )
                except Exception, e:
                    import traceback
                    traceback.print_exc()
            else:
                return
        else:
            return

    def getKeys( self, tb ):
        PRIKeys = []
        tableDesc = self.erep.getcols( tb )
        for d in tableDesc:
            if 'PRI' in d['Key']:
                PRIKeys.append( d['Field'] )
        return PRIKeys
    
    def getSourceDbUptime( self, sourceConn ):
        result = sourceConn.getUptime()
        return result[0]['Value']
    
if __name__ == '__main__':
    bs = BinlogServer()
    bs.processBinlog()