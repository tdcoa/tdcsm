import os, sys, yaml, errno
import datetime as dt
from shutil import copyfile
import pandas as pd


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

log('process start', '02-dbBludger_PrepRun', header=True, buffer=True)
log('time', str(dt.datetime.now()), buffer=True)


### Load config
log('loading config', 'config.yaml', buffer=True)
with open('config.yaml', 'r') as file:
    configyaml = yaml.load(file.read()) # , Loader=yaml.FullLoader) #<-- will need eventually
coafolders = {}
for fo in configyaml['folders']:
    coafolders.update(fo)
    log('defined folder', str(fo), buffer=True)

# clear log buffer
logpath = os.path.join('.', coafolders['run'], 'runlog.txt')
log('logflie defined', logpath)


# either commandline directory, or coa_default
log('processing cmdline', str(sys.argv))
coadir = coafolders['download'] # default
if len(sys.argv) > 1:
    if os.path.isdir(sys.argv[1]):
        coadir = sys.argv[1]
    else:
        log('\n\tERROR:\n\tsupplied path could not be found:\n%s' %sys.argv[1], buffer=True)
        exit()
log('folder to exeucte', coadir, buffer=True)


# clear out any pre-existing files in run folder:
log('empty run folder', coafolders['run'])
for file in os.listdir(os.path.join('.', coafolders['run'])):
    if file[:1] != '.' and file!='runlog.txt':
        log('deleting', file)
        os.remove(os.path.join('.', coafolders['run'],file))


#load .coa.sql files into the _run directory, with all replacements done (ready to run sql)
log('processing all .coa.sql files found in %s and save output to %s' %(coadir, coafolders['run']), header=True)
for coafile in os.listdir(coadir):
    if coafile[:1] != '.':
        if coafile[-8:]=='.coa.sql':  # if SQL, do substitutions
            log('\nPROCESSING COA.SQL FILE', coafile)
            with open(os.path.join(coadir, coafile), 'r') as coasqlfile:           # read from template
                coasqls = coasqlfile.read()
                log('characters in file:', str(len(coasqls)))

            with open(os.path.join(coafolders['run'],coafile),'w') as runsqlfile:  # write to _run file

                #light formatting of supplied sql
                sqls = coasqls.split(';')
                log('sql statements in file', str(len(sqls)))
                i=0
                for sql in sqls:
                    while '\n\n' in sql:
                        sql = sql.replace('\n\n','\n').strip()
                    sql = sql.strip() + '\n;\n\n'

                    if sql != '\n;\n\n':  # exclude null statements (only ; and newlines)
                        i+=1
                        log('\nprocessing sql %i' %i, '%s...' %sql[:50].replace('\n',' '))

                        # do substitutions first... (allows for substitution in {{loop}} command)
                        log('perform substitutions')
                        for setting in configyaml['substitutions']:
                            for find,replace in setting.items():
                                log('find: %s' %str(find), 'replace: %s' %str(replace))
                                sql = sql.replace('{%s}' %str(find),str(replace))

                        # if there are any {{loop: commands, go get the csv
                        log('process any loop commands')
                        if '/*{{loop:' in sql:

                            # parse csv file name from {{loop:csvfilename}} string
                            csvfile = sql[(sql.find('/*{{loop:') + len('/*{{loop:')):sql.find('}}*/')].strip()
                            log('loop found, file', csvfile)

                            # can we find the file?
                            if os.path.isfile(os.path.join(coafolders['run'],csvfile)): # csv file found!  let's open:
                                log('file found!')
                                df = pd.read_csv(os.path.join(coafolders['run'],csvfile))
                                log('rows in file', str(len(df)) )

                                for index, row in df.iterrows():  # one row = one sql written to file
                                    tempsql = sql
                                    for col in df.columns:
                                        tempsql = tempsql.replace(str('{%s}' %col), str(row[col]))
                                    tempsql = tempsql.replace(csvfile,' csv row %i out of %i' %(index+1, len(df)))
                                    tempsql = tempsql.replace('/*{{loop:','/*{{')
                                    log('sql generated from row data', 'character length = %i' %len(tempsql))
                                    runsqlfile.write(tempsql)


                            else:  # file not found, raise error
                                log('\n\tERROR:\n\tFile Not Found\n\t%s' %csvfile)
                                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), csvfile)

                        else:  # not a loop, just write file as-is (post-replace)
                            log('no loop commands found')
                            runsqlfile.write(sql)

        else:  # if not a .coa.sql file... just copy
            log('file not .coa.sql','copy only')
            copyfile(os.path.join('.', coadir, coafile), os.path.join('.', coafolders['run'], coafile))

log('\ndone!')
log('time', str(dt.datetime.now()))
