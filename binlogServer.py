import time
import types
import pymysql
import socket
import getopt
import sys

import easysqllite as esql
import m3easyrep

from config import LOCAL_IDC as li
from config import TEST_LOCATION
from config import DB_CONFIG
from config import CALLBACK_CONFIG

from easyrep import UnknownTableDefine

class ConnDisabled(Exception):
    pass

def getIP( host ):
    result = ''
    infos = host.split('.')
    if len( infos ) == 4 and False not in [info.isdigit() for info in infos]:
        return host
    else:
        try:
            result = socket.getaddrinfo( host, 'http' )
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
    TABLE_ANNALS = 'annales'
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
    COL_PORT = 'PORT'
    DB_SINASTORE = 'SinaStore'
    LOCAL_IDC = li
    
    # log the local binlog postion to master database
    POSITION_LOG_INTERVAL = 60
    POSITION_LOG_TABLE = 'POSITION_LOG'

    def __init__( self, log_location ):
        
        localMasterDbConfig = self.getLocalMasterDbConfig()
        self.localMasterConn = esql.Connection( localMasterDbConfig )
        self.verifyIDC()

        sourceMasterDbConfig = self.getSourceMasterDbConfig()
        self.sourceMasterConn = esql.Connection( sourceMasterDbConfig )
        
        result = self.establishSourceConn( log_location = log_location )
        if result:
            self.erep, self.sourceIP, self.sourcePort, self.sourceLocale = result
        else:
            print 'can not establish source binlog connection.'
            exit()
        #if not self.
        self.table_keys = self.erep.tableKeys
        
        self.reConnected = False
        self.log = None
        self.offset = None
        
    def getSourceDbConfig( self ):
        return DB_CONFIG[self.LOCAL_IDC]['source']
        
    def getSourceMasterDbConfig( self ):
        return DB_CONFIG[self.LOCAL_IDC]['sourceMaster']
    
    def getLocalMasterDbConfig( self ):
        return DB_CONFIG[self.LOCAL_IDC]['localMaster']
        
    def getSourceConn( self, sourceIPDict, sourceDbConfig, sourceAnnals, log_location = None, localOnly = False ):
        
        if log_location:
            ip, port, log, offset = log_location
            if ip and port and log and offset:
                locale = None
                hosts = []
                for k, v in sourceIPDict.items():
                    for _host, ipList in v.items():
                        if ip in ipList:
                            locale = k
                            hosts.append( _host )
                            break
                    if locale:
                        break
                
                dbConfig = None
                if locale:
                    dbConfigs = sourceDbConfig[locale]
                    for _dbConfig in dbConfigs:
                        if _dbConfig['host'] in hosts:
                            dbConfig = dict( _dbConfig )
                            if port != dbConfig['port']:
                                print '+' * 20, port, dbConfig['port']
                                dbConfig = None
                                continue
                            break
                else:
                    print 'can not init form the ip : %s' %ip
                    exit()
                    
                if not dbConfig:
                    print 'can not init form the given ip : %s' %ip
                    exit()
                    
                dbConfig['host'] = ip
                binlogLocation = ( log, offset )

                erep = None
                try:
                    erep = m3easyrep.M3EasyReplication( dbConfig, binlogLocation, self.TABLE_FILTER )
                    print 'x-x' * 10, binlogLocation
                except Exception:
                    print 'can not init by given ip,log and offset.'
                    exit()
                return ( erep, ip, port, locale )
        
        localPRI = ['local', 'remote']
        if localOnly:
            localPRI = ['local']
        
        for locale in localPRI:
            #sourceIPInfos = sourceIPDict.get( locale, [] )
            
            for _dbConfig in sourceDbConfig[locale]:
                dbConfig = dict( _dbConfig )
                host = _dbConfig['host']
                ipList = sourceIPDict[locale].get( host, [] )
                for ip in ipList:
                    if ip not in sourceAnnals.keys():
                        continue
                    if dbConfig['port'] != sourceAnnals[ip].get( self.COL_PORT, '' ):
                        print dbConfig['port'], sourceAnnals[ip].get( 'PORT', '' )
                        continue
                    dbConfig['host'] = ip
                    binlogLocation = self.getBinlogLocation( sourceAnnals, ip )
                    erep = None
                    try:
                        erep = m3easyrep.M3EasyReplication( dbConfig, binlogLocation, self.TABLE_FILTER )
                        print 'x-x' * 10, binlogLocation
                    except Exception:
                        import traceback
                        traceback.print_exc()

                    if erep:
                        uptimeNow = self.getSourceDbUptime( erep )
                        uptimeAnnal = sourceAnnals[ip][self.COL_UPTIME]
                        if uptimeNow >= uptimeAnnal:
                            return ( erep, ip, dbConfig['port'], locale )

        return ( None, None, None, None )

    def establishSourceConn( self, log_location = None, isReconnect = False, preTime = '' ):

        sourceDbConfig = self.getSourceDbConfig()
        sourceIPDict = self.getsourceIPDict()
        print '*' * 20, sourceIPDict
        while True:
            while True:
                # get annals from database
                markTime, sourceInfos = self.getSourceInfos( preTime, isReconnect, False )
                print '()', markTime, sourceInfos
                # can not get time from annals
                # but first time?
                if not markTime:
                    break
                
                erep, sourceIP, sourcePort, sourceLocale = self.getSourceConn( sourceIPDict, sourceDbConfig, sourceInfos, log_location, isReconnect )
                # can not establish connection from source ip list
                if not erep:
                    preTime = markTime
                else:
                    return ( erep, sourceIP, sourcePort, sourceLocale )
                    
                # reconnect just for given preTime annals
                if isReconnect:
                    break
                    
            newsourceIPDict = self.getsourceIPDict()
            if newsourceIPDict == sourceIPDict:
                if isReconnect:
                    return None
                else:
                    print 'can not init the source connection!'
                    return None
            else:
                sourceIPDict = newsourceIPDict

    def getsourceIPDict( self ):
        sourceIPDict = {}
        for locale, dbList in DB_CONFIG[self.LOCAL_IDC]['source'].items():
            localeIPDict = {}
            for dbConfig in dbList:
                try:
                    host = dbConfig['host']
                    localeIPDict[host] = getIP( host )
                    
                # can not get ip from source slave's domain name
                except Exception:
                    pass
            sourceIPDict[locale] = localeIPDict
        return sourceIPDict
            
    def getSourceInfos( self, preTime = '', isReconnect = False, test = True ):
        
        if test:
            return TEST_LOCATION
        else:
            cols = [self.COL_TIME]
            wc = {self.COL_BIN : 1}
            if preTime:
                # just for reconnect
                if isReconnect:
                    wc[self.COL_TIME] = preTime
                else:
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
            except Exception:
                import traceback
                traceback.print_exc()
                # first attempt to reconnect to same slave
                retry = 0
                result = None
                while retry < 5:
                    try:
                        result = self.establishSourceConn( log_location = ( self.sourceIP, self.sourcePort, self.log, self.offset ) )
                        if result:
                            self.erep, self.sourceIP, self.sourcePort, self.sourceLocale = result
                            break
                        else:
                            retry += 1
                            time.sleep( 1 )
                    except Exception, e:
                        import traceback
                        traceback.print_exc()
                        retry += 1
                        time.sleep( 1 )
                        
                if not result:
                    retry = 0
                    while retry < 5:
                            try:
                                result = self.establishSourceConn( )
                                if result:
                                    self.erep, self.sourceIP, self.sourcePort, self.sourceLocale = result
                                    break
                                else:
                                    retry += 1
                                    print '~' * 100, 'sleep?'
                                    time.sleep( 3 )
                            except Exception, e:
                                import traceback
                                traceback.print_exc()
                                retry += 1
                                time.sleep( 3 )

    def duplicateToLocalMaster( self, binlog ):
        print binlog
        newLocation, tableName, newValues, oldValues = binlog

        if newLocation:
            self.log, self.offset = newLocation[:2]
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
                self.doCallback( 'insert', tableName, newValues, oldValues )
            else:
                self.processInsert( tableName, newValues )
        # update
        elif newValues and oldValues:
            if newValues[self.COL_ORIGO] == self.LOCAL_IDC:
                if newValues[self.COL_MARK_DELETE] == 1:
                    # mark delete success, do call back
                    self.doCallback( 'delete', tableName, newValues, oldValues )
                    # real delete
                    self.doDelete( tableName, oldValues, self.localMasterConn )
                else:
                    self.doCallback( 'update', tableName, newValues, oldValues )
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

        # if want to update, but the date is not exist , then insert 
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
        annalData['PORT'] = self.sourcePort
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
            result = self.establishSourceConn( isReconnect = True, preTime = heartBeatData['Time'] )
            if result:
                print '~' * 20, 'reconnect succ'
                self.erep, self.sourceIP, self.sourcePort, self.sourceLocale = result
                self.reConnected = True
            else:
                print '~' * 20, 'reconnect failed'
                
    def doCallback( self, act, tb, newValues, oldValues ):
        config = CALLBACK_CONFIG.get( act, {} )
        #print 'do call back', config
        if config:
            #print tb, config.keys()
            if tb in config.keys():
                #print 'LL' * 20, 'enter call back'
                try:
                    db, sqlFormat = config[tb]
                    con = None
                    if db == 'source':
                        con = self.sourceMasterConn
                    elif db == 'local':
                        con = self.localMasterConn
                    else:
                        try:
                            con = esql.Connection( db )
                        except Exception, e:
                            print 'can init callback conn!'
                            raise
                    
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
        
def help( n ):
    print ''
    
if __name__ == '__main__':
    opts, args = ( None, None )
    try:
        opts, args = getopt.getopt(sys.argv[1:],'hi:l:o:p:')
    except getopt.GetoptError:
        help(1)
        
    ip, port, log, offset = ( None, None, None, None )
    for o,a in opts:
        if o == '-h':
            help(2)
        if o == '-i':
            ip = a.strip()
        if o == '-p':
            try:
                port = int( a.strip() )
            except Exception, e:
                print 'port : %s can not converto int' %port
                exit()
        if o == '-l':
            log = a.strip()
        if o == '-o':
            try:
                offset = int( a.strip() )
            except Exception, e:
                print 'offset : %s can not converto int' %offset
                exit()
    
    print ip, log, offset
    bs = BinlogServer( ( ip, port, log, offset ) )
    bs.processBinlog()