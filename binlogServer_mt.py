import time
import types
import pymysql
import socket
import getopt
import sys
import traceback
import threading
import Queue

import easysqllite as esql
import m3easyrep

from config import LOCAL_IDC
from config import TEST_LOCATION
from config import DB_CONFIG
from config import CALLBACK_CONFIG
from config import TRACE_BACK_TIME

from easyrep import UnknownTableDefine

TABLE_FILTER = ( 'SinaStore', None )
TABLE_HEART_BEAT = 'heartbeat'
TABLE_ANNALS = 'annalss'
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
QUEUE_LENGTH = 500
SQL_EXE_THREAD_CNT = 10

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

class slaveWatcher( threading.Thread ):
    def __init__(self, conn ):
        threading.Thread.__init__(self)

class BinlogServer( object ):
    
    ## log the local binlog postion to master database
    #POSITION_LOG_INTERVAL = 60
    #POSITION_LOG_TABLE = 'POSITION_LOG'

    def __init__( self, log_location ):

        sourceMasterDbConfig = dbProcessor.getSourceMasterDbConfig()
        try:
            self.sourceMasterConn = esql.Connection( sourceMasterDbConfig )
        except Exception:
            traceback.print_exc()
            print("can init source master's connection")
            exit(1)

        try:
            self.erep, self.sourceIP, self.sourcePort, self.sourceLocale, binlogLocation = self.establishSourceConn( log_location = log_location )
            self.log, self.offset = binlogLocation
        except Exception:
            traceback.print_exc()
            print("can init local source slave's connection")
            exit(1)
        
        self.tablest = self.erep.tablest
        self.table_keys = self.erep.tableKeys
        self.binlog_queue = Queue.Queue( QUEUE_LENGTH )
        self.reConnected = False
        self.MAX_RETRY = 3
        
        self.sql_exe_thread_list = []
        for i in xrange( SQL_EXE_THREAD_CNT ):
            t = binlogProcessor( self.binlog_queue, self.table_keys, i )
            self.sql_exe_thread_list.append( t )
            t.setDaemon( True )
            t.start()



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
                erep = m3easyrep.M3EasyReplication( dbConfig, binlogLocation, TABLE_FILTER )
                print 'x-x' * 10, erep, ip, port, locale, binlogLocation
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
                    if str( dbConfig['port'] ) != sourceAnnals[ip].get( COL_PORT, '' ):
                        print dbConfig['port'], type(dbConfig['port']), sourceAnnals[ip].get( 'PORT', '' ), type(sourceAnnals[ip].get( 'PORT', '' ))
                        continue
                    dbConfig['host'] = ip
                    binlogLocation = self.getBinlogLocation( sourceAnnals, ip )
                    erep = None
                    try:
                        erep = m3easyrep.M3EasyReplication( dbConfig, binlogLocation, TABLE_FILTER )
                        print '*-*' * 10, erep, ip, dbConfig['port'], locale, binlogLocation
                    except Exception:
                        import traceback
                        traceback.print_exc()

                    if erep:
                        uptimeNow = self.getSourceDbUptime( erep )
                        uptimeAnnal = sourceAnnals[ip][COL_UPTIME]
                        if uptimeNow >= uptimeAnnal:
                            return ( erep, ip, dbConfig['port'], locale, binlogLocation )

        return None

    def establishSourceConn( self, log_location = None, isReconnect = False, preTime = None ):

        sourceDbConfig = dbProcessor.getSourceDbConfig()
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
        for locale, dbList in DB_CONFIG[LOCAL_IDC]['source'].items():
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
            cols = [COL_TIME]
            wc = {COL_BIN : 1}
            if preTime:
                # just for reconnect
                if isReconnect:
                    wc[COL_TIME] = preTime
                else:
                    wc[COL_TIME] = ( None, preTime )
            order = [COL_TIME]
            heartBeatInfos = self.sourceMasterConn.gets( (DB_SINASTORE, TABLE_HEART_BEAT), where = wc, order = order, reverse = True, limit = 1 )
    
            if heartBeatInfos:
                t = heartBeatInfos[0][COL_TIME]
                wc = {COL_TIME : t}
                annals = self.sourceMasterConn.gets( (DB_SINASTORE, TABLE_ANNALS), where = wc )
                result = {}
                for annal in annals:
                    result[annal[COL_IP]] = annal
                return ( t.strftime('%Y-%m-%d %H:%M:%S'), result )
            
            else:
                return ( None, None )

    def getBinlogLocation( self, sourceInfos, sourceIP, test = True ):
        sourceInfo = sourceInfos[sourceIP]
        return ( sourceInfo[COL_FILE], sourceInfo[COL_OFFSET] )

    #def reConnectSource( self ):
    #    self.sourceConn = esql.Connection( self.sourceDbConfig )
    #    
    #def reConnectLocalMasterDb( self ):
    #    localMasterDbConfig = self.getLocalMasterDbConfig()
    #    self.localMasterConn = esql.Connection( localMasterDbConfig )
    #    
    #def reConnectSourceMasterDb( self ):
    #    sourceMasterDbConfig = dbProcessor.getSourceMasterDbConfig()
    #    self.sourceMasterConn = esql.Connection( sourceMasterDbConfig )


            
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
            except UnknownTableDefine, e:
                print '[]' * 10, 'no table defin, ignore', str(e)
                continue
            # some exceptions make the connection disabled, reconnect
            # only slave exception was raised
            #except pymysql.OperationalError, e:
            except socket.error, e:
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
                        time.sleep(1)
                    else:
                        self.erep, self.sourceIP, self.sourcePort, self.sourceLocale, binlogLocation = result
                        self.log, self.offset = binlogLocation
                        break

                while result == None:
                    try:
                        print 'connect conn'
                        result = self.establishSourceConn()
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
        #print binlog
        newLocation, tableName, newValues, oldValues = binlog

        if tableName == ( DB_SINASTORE, TABLE_HEART_BEAT ):
            if newValues and not oldValues and newValues[COL_BIN] == 0:
                # maybe the self.log, self.offset is useless
                if newLocation:
                    self.log, self.offset = newLocation[:2]
                while True:
                    try:
                        import pprint
                        pprint.pprint(binlog)
                        self.doHeartBeat( newLocation, newValues )
                        return
                    except Exception:
                        import traceback
                        traceback.print_exc()
                        raise
            else:
                return
        
        self.binlog_queue.put( binlog )
        print 'queuesize : %s' %(self.binlog_queue.qsize())
        print 'processing_cnt : %s' %binlogProcessor.processing_cnt
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

    #def compareIDCPRI( self, sourceIDC, localIDC ):
    #    sourceIDCPRI = self.IDC_PRI.get( sourceIDC, self.DEFAULT_IDC_PRI )
    #    localIDCPRI = self.IDC_PRI.get( localIDC, self.DEFAULT_IDC_PRI )
    #
    #    # the lower the precedence
    #    if sourceIDCPRI < localIDCPRI:
    #        return True
    #    elif sourceIDCPRI > localIDCPRI:
    #        return False
    #    else:
    #        return sourceIDC < localIDC
    #    
    #def getComparison( self, sourceIDC):
    #    if self.compareIDCPRI( sourceIDC, self.LOCAL_IDC ):
    #        return '<='
    #    else:
    #        return '<'
        


    def doHeartBeat( self, newLocation, datas ):
        logname, position, tablest = newLocation
        
        annalData = {}
        annalData['Time'] = datas['Time']
        annalData['IP'] = self.sourceIP
        annalData['PORT'] = self.sourcePort
        annalData['File'] = logname
        annalData['Offset'] = position
        annalData['Uptime'] = self.getSourceDbUptime( self.erep )

        tb  = ( DB_SINASTORE, TABLE_ANNALS )
        try:
            dbProcessor.doInsert( tb, annalData, self.sourceMasterConn )
        # duplicate key, when source from remote idc slave
        except pymysql.err.IntegrityError, e:
            # do update
            dbProcessor.doUpdate( tb, annalData, self.table_keys, self.sourceMasterConn, False )
        
        tb  = ( DB_SINASTORE, TABLE_HEART_BEAT )
        heartBeatData = {}
        heartBeatData['Time'] = datas['Time']
        heartBeatData['Bin'] = 1
        dbProcessor.doUpdate( tb, heartBeatData, self.table_keys, self.sourceMasterConn, False )

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
        
class binlogProcessor( threading.Thread ):
    
    processing_cnt = 0
    lock = threading.Lock()
    _start = 0
    _end = 0
    
    def __init__( self, queue, table_keys, cnt ):
        threading.Thread.__init__(self)
        self.queue = queue
        self.table_keys = table_keys
        self.cnt = cnt
        self.over = False
        
        self.localMasterConn = None
        localMasterDbConfig = dbProcessor.getLocalMasterDbConfig()
        try:
            self.localMasterConn = esql.Connection( localMasterDbConfig )
        except Exception:
            traceback.print_exc()
            print("can init local master's connection")
            exit(1)
        dbProcessor.verifyIDC( self.localMasterConn )
        
        print 'sub thread %s has inited' %self.cnt
        
    def run( self ):
        while not self.over:

            self.lock.acquire()
            binlog = self.queue.get()
            self.processing_cnt += 1
            self.lock.release()
            newLocation, tableName, newValues, oldValues = binlog
            
            if not tableName:
                continue
            
            # insert for timepoint to source database
            if tableName == ( DB_SINASTORE, TABLE_ANNALS ):
                continue
            
            offset = newLocation[1]
            if offset >= 65571771 and self._start == 0:
                self._start = time.time()
            if offset >= 67941000 and self._end == 0:
                self._end = time.time()
                print '*-* ' * 20
                print 'elapse : %s' %(self._end - self._start)
                print '*-* ' * 20

            # insert
            if newValues and not oldValues:
                #if newValues.get(COL_ORIGO, LOCAL_IDC) == LOCAL_IDC:
                if newValues[COL_ORIGO] == LOCAL_IDC:
                    
                    dbProcessor.doCallback( 'insert', tableName, newValues, oldValues, self.table_keys )
                else:
                    dbProcessor.processInsert( tableName, newValues, self.table_keys, self.localMasterConn )
            # update
            elif newValues and oldValues:
                if newValues[COL_ORIGO] == LOCAL_IDC:
                    if newValues[COL_MARK_DELETE] == 1:
                        # mark delete success, do call back
                        dbProcessor.doCallback( 'delete', tableName, newValues, oldValues, self.table_keys )
                        # real delete
                        dbProcessor.doDelete( tableName, oldValues, self.table_keys, self.localMasterConn )
                    else:
                        dbProcessor.doCallback( 'update', tableName, newValues, oldValues, self.table_keys )
                else:
                    dbProcessor.processUpdate( tableName, newValues, self.table_keys, self.localMasterConn )
            # del
            elif not newValues and oldValues:
                dbProcessor.doDelete( tableName, oldValues, self.table_keys, self.localMasterConn )
            else:
                pass
            
            self.processing_cnt -= 1
            
class dbProcessor( object ):
    @classmethod
    def getLocalMasterDbConfig( cls ):
        return DB_CONFIG[LOCAL_IDC]['localMaster']
        
    @classmethod
    def getSourceDbConfig( cls ):
        return DB_CONFIG[LOCAL_IDC]['source']
        
    @classmethod
    def getSourceMasterDbConfig( cls ):
        return DB_CONFIG[LOCAL_IDC]['sourceMaster']
            
    @classmethod
    def verifyIDC( cls, conn ):
        if LOCAL_IDC != cls.getLocalIDC( conn ):
            print 'IDC CONFIG WRONG!'
            exit()
            
    @classmethod
    def getLocalIDC( cls, conn ):
        try:
            result = conn.gets( (DB_SINASTORE, TABLE_CONFIG) )
            return result[0]['IDC']
        except Exception, e:
            import traceback
            traceback.print_exc()
            raise Exception('Can not get local master config!')
           
    @classmethod 
    def processInsert(  cls, tb, data, table_keys, conn ):
        try:
            affectRows = cls.doInsert( tb, data, conn )
        # duplicatge key
        except pymysql.err.IntegrityError, e:
             affectRows = cls.doUpdate( tb, data, table_keys, conn )
            # before update after insert , local delete happened
    
    @classmethod
    def processUpdate( cls, tb, data, table_keys, conn ):
        affectRows = 0
        affectRows = cls.doUpdate( tb, data, table_keys, conn )

        # if want to update, but the date is not exist , then insert 
        if affectRows == 0:
            try:
                affectRows = cls.doInsert( tb, data, conn )
            # duplicatge key, ignore
            except pymysql.err.IntegrityError, e:
                pass

    @classmethod
    def doInsert( cls, tb, data, conn ):
        
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
                        if retry > MAX_RETRY:
                            pass

            except pymysql.ProgrammingError, e :
                import traceback
                traceback.print_exc()
                e.args = tuple( list(e.args))
                raise

    @classmethod
    def doUpdate( cls, tb, data, table_keys, conn, extCondition = True ):

        PRIKeys = table_keys.get( tb, None )
        #if PRIKeys == None:
        #    PRIKeys = self.getKeys( tb )
        #    self.table_keys[tb] = PRIKeys
        print data
        print PRIKeys
        
        condition = [(k, '=', v) for k, v in data.items() if k in PRIKeys]
        if extCondition:
            condition = [y for x in zip(condition, ['and'] * len(condition)) for y in x]
            #condition.append( (self.COL_LAST_MODIFY, self.getComparison( data[self.COL_ORIGO] ), data[self.COL_LAST_MODIFY]) )
            tmp_condition = []
            tmp_condition.append( (COL_LAST_MODIFY, '=', data[COL_LAST_MODIFY]) )
            tmp_condition.append('and')
            tmp_condition.append( (COL_ORIGO, '>=', data[COL_ORIGO]) )
            
            tmp_condition1 = []
            tmp_condition1.append( (COL_LAST_MODIFY, '<', data[COL_LAST_MODIFY]) )
            tmp_condition1.append('or')
            tmp_condition1.append(tmp_condition)
            
            condition.append(tmp_condition1)
        
        while True:
            try:
                return conn.update( tb, data, condition, extCondition )
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
                        if retry > MAX_RETRY:
                            pass

            except pymysql.ProgrammingError, e :
                import traceback
                traceback.print_exc()
                e.args = tuple( list(e.args))
                raise

    @classmethod
    def doDelete( cls, tb, data, table_keys, conn ):
        PRIKeys = table_keys.get( tb, None )
        #if PRIKeys == None:
        #    PRIKeys = self.getKeys( tb )
        #    self.table_keys[ tb ] = PRIKeys
           
        condition = [( k, '=', v) for k, v in data.items() if k in PRIKeys]
        condition.append( (COL_MARK_DELETE, '=', 1) )
        
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
                        if retry > MAX_RETRY:
                            pass

            except pymysql.ProgrammingError, e :
                e.args = tuple( list(e.args)+[sql,] )
                raise

    @classmethod
    def doCallback( cls, act, tb, newValues, oldValues, table_keys ):
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
                        con = esql.Connection( cls.getSourceMasterDbConfig() )
                    elif db == 'local':
                        con = esql.Connection( cls.getLocalMasterDbConfig() )
                    else:
                        try:
                            con = esql.Connection( db )
                        except Exception, e:
                            print 'can init callback conn!'
                            raise
                        
                    PRIKeys = table_keys.get( tb, None )
                    #if PRIKeys == None:
                    #    PRIKeys = self.getKeys( tb )
                    #    self.table_keys[tb] = PRIKeys
                    if newValues:
                        newValues['ALL_KEYS'] = '|'.join([esql.formatvalue( newValues.get(k) ) for k in PRIKeys])
                    if oldValues:
                        oldValues['ALL_KEYS'] = '|'.join([esql.formatvalue( oldValues.get(k) ) for k in PRIKeys])
                    
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
                    raise
            else:
                return
        else:
            return

def help( n ):
    if n == 1:
        print '''please enter four argss: ip, port, logname, offset
                 ex : python binlogServer.py -i 1.1.1.1 -p 3306 -l logname -o offset'''
    exit(1)
    
def main():
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
    
if __name__ == '__main__':
    main()