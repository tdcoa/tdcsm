import os, sys, yaml, shutil
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


def get_sqlcommands(sql):
    cmdstart = '/*{{'
    cmdend = '}}*/'
    cmd = {}
    while cmdstart in sql:
        pos1 = sql.find(cmdstart)
        pos2 = sql.find(cmdend)
        cmdstr = sql[pos1:pos2+len(cmdend)]
        cmdlst = cmdstr.replace(cmdstart,'').replace(cmdend,'').split(':')
        if len(cmdlst) ==2:
            cmd[cmdlst[0].strip()] = cmdlst[1].strip()
            log('special command found', '%s = %s' %(cmdlst[0].strip(), cmdlst[1].strip()))
            sql = sql.replace(cmdstr,'')
        else:
            sql = sql.replace(cmdstr,'/* %s */' %cmdlst[0])
    cmd['sql'] = sql
    return cmd


log('process start', '03-dbBludger_LoopAndExecute', header=True, buffer=True)
log('time', str(dt.datetime.now()), buffer=True)



### Load config
log('loading config', 'config.yaml', buffer=True)
if  os.path.isfile('config.yaml'):
    configpath = 'config.yaml'
elif os.path.isfile(os.path.join('..', 'config.yaml')):
    configpath = os.path.join('..', 'config.yaml')
elif os.path.isfile(os.path.join('..', '..', 'config.yaml')):
    configpath = os.path.join('..','..', 'config.yaml')
elif os.path.isfile(os.path.join('..', '..', '..', 'config.yaml')):
    configpath = os.path.join('..','..','..',  'config.yaml')
else:
    log('\n\nERROR: config.yaml File Cannot Be Found!!!\nPlease re-run 01-dbBludger process to download updated config.yaml')
    exit()

with open(configpath, 'r') as file:
    configyaml = yaml.load(file.read()) # , Loader=yaml.FullLoader) #<-- will need eventually
coafolders = {}
for fo in configyaml['folders']:
    coafolders.update(fo)
    log('defined folder', str(fo), buffer=True)
coasystems = {}
for sy in configyaml['siteids']:
    coasystems.update(sy)
    log('defined siteid', str(sy), buffer=True)


# clear log buffer
logpath = os.path.join('.', coafolders['run'], 'runlog.txt')
log('logflie defined', logpath)


# collect all prepared sql files, place in alpha order
coasqlfiles=[]
for coafile in os.listdir(coafolders['run']):
    if coafile[:1] != '.' and coafile[-8:]=='.coa.sql':
        log('found prepared sql file', coafile )
        coasqlfiles.append(coafile)
coasqlfiles.sort()
log('all sql files alpha-sorted for exeuction consistency')

# create output folder:
outputfo = os.path.join('.', coafolders['output'], str(dt.datetime.now())[:-7].replace(' ','_').replace(':',''))
log('output folder defined', outputfo)
if not os.path.exists(outputfo):
    os.mkdir(outputfo)
    log('output folder created', outputfo)

# create our upload-manifest:
log('creating upload manifest file')
with open(os.path.join(outputfo,'upload-manifest.json'),'w') as manifest:
    manifest.write('{"entries":[ ')
manifestdelim='\n '

# loop thru systems and execute all
for siteid, connstring in coasystems.items():
    log('connecting to ', siteid, header=True )
    conn = create_engine(connstring) # <------------------------------- Connect to the database

    for coasqlfile in sorted(coasqlfiles):  # loop thru all sql files:
        log('\nOPENING SQL FILE', coasqlfile)
        with open(os.path.join('.', coafolders['run'], coasqlfile), 'r') as coasqlfilehdlr:
            sqls = coasqlfilehdlr.read()

        #  do any run-time substitutions (just SiteID)
        log('perform run-time substitutions: {siteid} == %s' %str(siteid).strip())
        sqls = sqls.replace('{siteid}', str(siteid).strip()).strip()

        sqlcnt = 0
        for sql in sqls.split(';'):  # loop thru the sql in the files
            sqlcnt +=1

            if sql == '':
                log('null statement, skipping')
            else:
                log('----')

                # pull out any embedded SQLcommands:
                sqlcmd = get_sqlcommands(sql)
                sql = sqlcmd['sql']
                del sqlcmd['sql']

                log('execute sql %i' %sqlcnt, sql[:50].replace('\n',' ').strip() + '...')

                log('sql submitted', str(dt.datetime.now()))
                df = pd.read_sql(sql, conn)# <------------------------------- Run SQL
                log('sql completed', str(dt.datetime.now()))
                log('record count', str(len(df)))

                if len(df) != 0:  # Save non-empty returns to .csv

                    # finally report on the get_sqlcommand(sql) line above
                    if len(sqlcmd)==0:
                        log('no special commands found')

                    if 'save' not in sqlcmd:
                        sqlcmd['save'] = coasqlfile + '%04d' %sqlcnt + '.csv'

                    # once built, append output folder, SiteID on the front, iterative counter on back if needed for uniquess
                    csvfile = os.path.join('.', outputfo, sqlcmd['save'])
                    i=0
                    while os.path.isfile(csvfile):
                        i +=1
                        csvfile = csvfile[:-4] + '.%03d' %i + csvfile[-4:]
                    log('CSV save location', csvfile)

                    log('saving file...')
                    df.to_csv(csvfile) # <---------------------- Save to .csv
                    log('file saved!')

                    if 'load' in sqlcmd:  # add to manifest
                        log('file marked for loading to Transcend, adding to upload-manifest.json')
                        if 'call' not in sqlcmd:   sqlcmd['call']=''
                        manifest_entry = '%s{"file": "%s",  "table": "%s",  "call": "%s"}' %(manifestdelim, sqlcmd['save'], sqlcmd['load'], sqlcmd['call'])
                        manifestdelim='\n,'

                        with open(os.path.join(outputfo,'upload-manifest.json'),'a') as manifest:
                            manifest.write(manifest_entry)
                            log('Manifest updated', str(manifest_entry).replace(',',',\n'))

log('post-processing', header=True)

# close JSON object
with open(os.path.join(outputfo,'upload-manifest.json'),'a') as manifest:
    manifest.write("\n  ]}")
    log('closing out upload-manifest.json')

# Move all files from run folder to output, for posterity:
log('moving all run artifacts to output folder, for archiving')
for file in os.listdir(os.path.join('.', coafolders['run'])):
    if file[:1]!='.' and file!='runlog.txt':
        os.replace( os.path.join('.', coafolders['run'], file), os.path.join(outputfo, file) )
        log('  moved', file)

# create hidden file containing last run's output -- to remain in the root folder
with open(os.path.join('.', '.last_run_output_path.txt'), 'w') as lastoutput:
    lastoutput.write(outputfo)
    log('save location of last-run output folder to hidden file')
    log('last-run output', outputfo)

# also COPY a few other operational files to output folder, for ease of use:
files = ['04-dbBludger_TranscendUpload.py','.last_run_output_path.txt', configpath]
for file in files:
    log('copy %s to run folder, for ease of use' %file)
    shutil.copyfile( os.path.join('.', file), os.path.join(outputfo, file) )


log('\ndone!')
log('time', str(dt.datetime.now()))

file = 'runlog.txt'
os.replace( os.path.join('.', coafolders['run'], file), os.path.join(outputfo, file) )
