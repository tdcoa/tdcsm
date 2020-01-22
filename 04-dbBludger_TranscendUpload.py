# dbBludger -- Upload to Transcend
import os, sys, yaml, json
import pandas as pd
import datetime as dt
from sqlalchemy.engine import create_engine


### Setup Logging:
logpath = ''
def writelog(msg):
    global logpath
    with open(logpath,'a') as logfile:
        logfile.write(msg + '\n')

logs =[]
def log(msgleft='', msgright='', header=False, buffer=False):
    delim = ':'
    if  msgright=='': delim=''
    msg = '%s%s' %(str(msgleft+delim).ljust(25), msgright)
    if header: msg = '\n\n%s\n%s\n%s' %('='*40, msg.upper(), '-'*40)
    global logs
    if buffer:
        logs.append(msg)
    else:
        if len(logs) ==0:
            print(msg)
            writelog(msg)
        else:
            for log in logs:
                print(log)
                writelog(msg)
                logs=[]
            print(msg)
            writelog(msg)



log('process start', '04-dbBludger_TranscendUpload', header=True, buffer=True)
log('time', str(dt.datetime.now()), buffer=True)


### Load config
log('loading config', 'config.yaml', buffer=True)
scanfor = 'config.yaml'
rtnpath = ''
log('scanning directory structure for %s' %scanfor, buffer=True)
if  os.path.isfile(scanfor):
    rtnpath = scanfor
elif os.path.isfile(os.path.join('..', scanfor)):
    rtnpath = os.path.join('..', scanfor)
elif os.path.isfile(os.path.join('..', '..', scanfor)):
    rtnpath = os.path.join('..','..', scanfor)
elif os.path.isfile(os.path.join('..', '..', '..', scanfor)):
    rtnpath = os.path.join('..','..','..',  scanfor)
else:
    print('\n\nERROR: %s File Cannot Be Found!!!\nPlease re-run 01-dbBludger process to download updated %s' %(scanfor,scanfor))
    exit()
configpath = rtnpath

with open(configpath, 'r') as file:
    configyaml = yaml.load(file.read()) # , Loader=yaml.FullLoader) #<-- will need eventually
log('config found', buffer=True)
coafolders = {}
for fo in configyaml['folders']:
    coafolders.update(fo)
    log('defined folder', str(fo), buffer=True)
coatranscend = {}
for sy in configyaml['transcend']:
    coatranscend.update(sy)
    log('defined transcend', str(sy), buffer=True)


# clear log buffer
logpath = os.path.join('.', 'runlog.txt') # remember, this should have been moved local
log('logflie defined', logpath)


# resolve which directory to upload (important!)
approot=''
log('resolving which directory to upload')
log('first, cmdline', str(sys.argv))
if len(sys.argv)>1:
    if os.path.isdir(sys.argv[1]):
        approot = sys.argv[1]
        log('cmdline path found', approot)
if approot=='':
    scanfor = '.last_run_output_path.txt'
    rtnpath = ''
    log('scanning directory structure for %s' %scanfor, buffer=True)
    if  os.path.isfile(scanfor):
        rtnpath = '.'
    elif os.path.isfile(os.path.join('..', scanfor)):
        rtnpath = os.path.join('..')
    elif os.path.isfile(os.path.join('..', '..', scanfor)):
        rtnpath = os.path.join('..','..')
    elif os.path.isfile(os.path.join('..', '..', '..', scanfor)):
        rtnpath = os.path.join('..','..','..')
    else:
        print('\n\nERROR: %s File Cannot Be Found!!!\nPlease re-run 01-dbBludger process to download updated %s' %(scanfor,scanfor))
        exit()
    with open(os.path.join(rtnpath,scanfor), 'r') as lastrun:
        approot = lastrun.read()
log('%s found' %scanfor, approot)

#coadir = sys.argv[1]

# define upload manifest:
log('open file', 'upload-manifest.json')
with open(os.path.join(approot, 'upload-manifest.json'),'r') as manfile:
    manjson = manfile.read()
    manifest = json.loads(manjson)
    log('upload count found', str(len(manifest)) )


# loop thru all TRANSCEND systems and execute manifest
for name, connstring in coatranscend.items():
    log('connecting to', name, header=True)
    conn = create_engine(connstring) # <------------------------------- Connect to the database

    for entry in manifest['entries']:

        log('\nPROCESSING NEW ENTRY')
        log('  load file', entry['file'])
        log('  into table', entry['table'])
        log('  then call', entry['call'])

        # open CSV file for reading
        log('opening file', entry['file'])
        dfcsv = pd.read_csv(os.path.join(approot, entry['file']))
        log('records found', str(len(dfcsv)))

        for col in dfcsv.columns:
            if col[:8] == 'Unnamed:':
                log('unnamed column dropped', col)
                log('  (usually the pandas index as a column, "Unnamed: 0")')
                dfcsv = dfcsv.drop(columns=[col])
        log('final column count', str(len(dfcsv.columns)))

        if len(entry['table'].split('.')) == 1:
            db = 'adlste_coa'
            tbl = entry['table']
        else:
            db = entry['table'].split('.')[0]
            tbl = entry['table'].split('.')[1]

        # APPEND data set to table
        log('uploading', str(dt.datetime.now()))
        dfcsv.to_sql(tbl, conn, schema=db, if_exists='append', index=False)
        log('complete', str(dt.datetime.now()))

        # CALL any specified SPs:
        if str(entry['call']).strip() != "":
            log('Stored Proc', str(dt.datetime.now()) )
            conn.execute('call %s ;' %str(entry['call']) )
            log('complete', str(dt.datetime.now()))


log('\ndone!')
log('time', str(dt.datetime.now()))
