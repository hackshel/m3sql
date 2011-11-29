import time
import types
import pymysql
import socket
import getopt
import sys
import traceback

import easysqllite as esql
import m3easyrep

from config import LOCAL_IDC as li
from config import TEST_LOCATION
from config import DB_CONFIG
from config import CALLBACK_CONFIG
from config import TRACE_BACK_TIME

from easyrep import UnknownTableDefine

class SlaveConnDisabled(Exception):
    pass

class MasterConnDisabled(Exception):
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
        if d:
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
    
    ## log the local binlog postion to master database
    #POSITION_LOG_INTERVAL = 60
    #POSITION_LOG_TABLE = 'POSITION_LOG'

    def __init__( self, log_location ):
        
        localMasterDbConfig = self.getLocalMasterDbConfig()
        try:
            self.localMasterConn = esql.Connection( localMasterDbConfig )
        except Exception:
            traceback.print_exc()
            print("can init local master's connection")
            exit(1)
        self.verifyIDC()

        sourceMasterDbConfig = self.getSourceMasterDbConfig()
        try:
            self.sourceMasterConn = esql.Connection( sourceMasterDbConfig )
        except Exception:
            traceback.print_exc()
            print("can init local master's connection")
            exit(1)

        try:
            self.erep, self.sourceIP, self.sourcePort, self.sourceLocale, binlogLocation = self.establishSourceConn( log_location = log_location )
            self.log, self.offset = binlogLocation
        except Exception:
            traceback.print_exc()
            print("can init local source slave's connection")
            exit(1)
        
        self.table_keys = self.erep.tableKeys
        self.reConnected = False
        self.MAX_RETRY = 3

    def getSourceDbConfig( self ):
        return DB_CONFIG[self.LOCAL_IDC]['source']
        
    def getSourceMasterDbConfig( self ):
        return DB_CONFIG[self.LOCAL_IDC]['sourceMaster']
    
    def getLocalMasterDbConfig( self ):
        return DB_CONFIG[self.LOCAL_IDC]['localMaster']
        
    def getSourceConnByArgs( self, sourceIPDict, sourceDbConfig, log_location ):
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
                raise SlaveConnDisabled('no host in dbconfig has the ip : %s' %ip)
                #print 'can not init form the ip : %s' %ip
                #return None
                
            if not dbConfig:
                raise SlaveConnDisabled('no host has the same ip : %s and the same port : %s in dbconfig' %( ip, port) )
                #print 'can not init form the given ip : %s' %ip
                #return None
                
            dbConfig['host'] = ip
            binlogLocation = ( log, offset )

            erep = None
            try:
                erep = m3easyrep.M3EasyReplication( dbConfig, binlogLocation, self.TABLE_FILTER )
                print 'x-x' * 10, binlogLocation
            except Exception:
                traceback.print_exc()
                raise SlaveConnDisabled('can not init the source binlog conn by given ip : %s, port : %s, log : %s and offset : %s' %( ip, port, log, offset ) )
                #print 'can not init by given ip,log and offset.'
                #return None
            return ( erep, ip, port, locale, binlogLocation )
        
    def getSourceConn( self, sourceIPDict, sourceDbConfig, sourceAnnals, localOnly = False ):

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
                            return ( erep, ip, dbConfig['port'], locale, binlogLocation )

        return None

    def establishSourceConn( self, log_location = None, isReconnect = False, preTime = None ):

        sourceDbConfig = self.getSourceDbConfig()
        sourceIPDict = self.getsourceIPDict()
        print '*' * 20, sourceIPDict
        
        if log_location:
            print 'connect by log_location'
            return self.getSourceConnByArgs( sourceIPDict, sourceDbConfig, log_location )
        
        now = int( time.time() )
        while True:
            while True:
                markTime, sourceInfos = ( None, None )

                # get annals from database
                markTime, sourceInfos = self.getSourceInfos( preTime, isReconnect, False )
                print '()', markTime, sourceInfos
                # can not get time from annals and no location given, then exception
                if not markTime:
                    break
                else:
                    iMarkTime = time.mktime(time.strptime(markTime,'%Y-%m-%d %H:%M:%S'))
                    if now - iMarkTime > TRACE_BACK_TIME:
                        print 'mark time over max TRACE_BACK_TIME'
                        break
                
                result = self.getSourceConn( sourceIPDict, sourceDbConfig, sourceInfos, isReconnect )
                
                if result:
                    return result
                # can not establish connection from source ip list
                else:
                    # reconnect just for given preTime annals
                    if isReconnect:
                        break
                    else:
                        preTime = markTime

            newsourceIPDict = self.getsourceIPDict()
            if newsourceIPDict == sourceIPDict:
                #if isReconnect:
                #    return None
                #else:
                #    print 'can not init the source connection!'
                #    return None
                raise SlaveConnDisabled("can not init the source connection!")
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
                return ( t.strftime('%Y-%m-%d %H:%M:%S'), result )
            
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
                    # just raise SlaveConnDisabled
                    self.duplicateToLocalMaster( binlog )
            # when parse binlog, the table's info is not in tablemata, do not process
            except UnknownTableDefine:
                print '[]' * 10, 'ignore'
                continue
            # some exceptions make the connection disabled, reconnect
            # only slave exception was raised
            except pymysql.OperationalError, e:
                traceback.print_exc()
                # first attempt to reconnect to same slave
                retry = 0
                result = None
                while retry < self.MAX_RETRY:
                    try:
                        print 'connect last conn'
                        result = self.establishSourceConn( log_location = ( self.sourceIP, self.sourcePort, self.log, self.offset ) )
                    except SlaveConnDisabled:
                        retry += 1
                        time.sleep( 1 )
                    else:
                        self.erep, self.sourceIP, self.sourcePort, self.sourceLocale, binlogLocation = result
                        self.log, self.offset = binlogLocation
                        break

                while result == None:
                    try:
                        print 'connect conn'
                        result = self.establishSourceConn( )
                    except SlaveConnDisabled:
                        retry += 1
                        time.sleep(1)
                        if retry > self.MAX_RETRY:
                            pass
                    else:
                        self.erep, self.sourceIP, self.sourcePort, self.sourceLocale, binlogLocation = result
                        self.log, self.offset = binlogLocation
                        break

    def duplicateToLocalMaster( self, binlog ):
        print binlog
        newLocation, tableName, newValues, oldValues = binlog

        if not tableName:
            return
        
        # insert for timepoint to source database
        if tableName == ( self.DB_SINASTORE, self.TABLE_ANNALS ):
            return
        
        if tableName == ( self.DB_SINASTORE, self.TABLE_HEART_BEAT ):
            if newValues and not oldValues and newValues[self.COL_BIN] == 0:
                while True:
                    try:
                        self.doHeartBeat( newLocation, newValues )
                        return
                    except Exception:
                        import traceback
                        traceback.print_exc()
                        raise
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
        
        if newLocation:
            self.log, self.offset = newLocation[:2]

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
        
        while True:
            try:
                return conn.put( tb, data )
            except pymysql.OperationalError, e:
                retry = 0
                while True:
                    try:
                        print 'conn reconnect'
                        conn.reconnect()
                        break
                    except pymysql.OperationalError, e:
                        retry += 1
                        time.sleep(1)
                        if retry > self.MAX_RETRY:
                            pass

            except pymysql.ProgrammingError, e :
                e.args = tuple( list(e.args)+[sql,] )
                raise

    def doUpdate( self, tb, data, conn, extCondition = True ):
        PRIKeys = self.table_keys.get( tb, None )
        if PRIKeys == None:
            PRIKeys = self.getKeys( tb )
            self.table_keys[tb] = PRIKeys
        
        condition = [(k, '=', v) for k, v in data.items() if k in PRIKeys]
        if extCondition:
            condition.append( (self.COL_LAST_MODIFY, self.getComparison( data[self.COL_ORIGO] ), esql.formatvalue( data[self.COL_LAST_MODIFY]) ) )
        
        while True:
            try:
                return conn.update( tb, data, condition )
            except pymysql.OperationalError, e:
                retry = 0
                while True:
                    try:
                        print 'conn reconnect'
                        conn.reconnect()
                        break
                    except pymysql.OperationalError, e:
                        retry += 1
                        time.sleep(1)
                        if retry > self.MAX_RETRY:
                            pass

            except pymysql.ProgrammingError, e :
                e.args = tuple( list(e.args)+[sql,] )
                raise

    def doDelete( self, tb, data, conn ):
        PRIKeys = self.table_keys.get( tb, None )
        if PRIKeys == None:
            PRIKeys = self.getKeys( tb )
            self.table_keys[ tb ] = PRIKeys
           
        condition = [( k, '=', v) for k, v in data.items() if k in PRIKeys]
        condition.append( (self.COL_MARK_DELETE, '=', 1) )
        
        while True:
            try:
                return conn.delete( tb, condition )
            except pymysql.OperationalError, e:
                retry = 0
                while True:
                    try:
                        print 'conn reconnect'
                        conn.reconnect()
                        break
                    except pymysql.OperationalError, e:
                        retry += 1
                        time.sleep(1)
                        if retry > self.MAX_RETRY:
                            pass

            except pymysql.ProgrammingError, e :
                e.args = tuple( list(e.args)+[sql,] )
                raise

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
            try:
                result = self.establishSourceConn( isReconnect = True, preTime = heartBeatData['Time'] )
            except SlaveConnDisabled:
                print '~' * 20, 'reconnect failed'
            else:
                print '~' * 20, 'reconnect succ'
                self.erep, self.sourceIP, self.sourcePort, self.sourceLocale, binlogLocation = result
                self.log, self.offset = binlogLocation
                self.reConnected = True

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
                    combinedDict['ACT'] = esql.formatvalue( act )
                    combinedDict['TABLE'] = esql.formatvalue( tb ) if type( tb ) in types.StringTypes else esql.formatvalue( '.'.join(tb) )
                    
                    import pprint
                    pprint.pprint( combinedDict )

                    sql = sqlFormat %combinedDict
                    sql = sql
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
    
    # this operation is select from source slave, if disconnect then raise exception and reconnect
    def getSourceDbUptime( self, sourceConn ):
        result = sourceConn.getUptime()
        return result[0]['Value']

def help( n ):
    if n == 1:
        print '''please enter four argss: ip, port, logname, offset
                 ex : python binlogServer.py -i 1.1.1.1 -p 3306 -l logname -o offset'''
    exit(1)
    
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
    
    init_args = ( ip, port, log, offset )
    argv = sum( [1 for arg in init_args if arg != None] )
    if argv == 0:
        init_args = None
    elif argv != 4:
        help(1)    
    
    bs = BinlogServer( init_args )
    bs.processBinlog()