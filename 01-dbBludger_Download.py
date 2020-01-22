import  os, sys, io, yaml, datetime as dt
import datetime as dt
import pandas as pd
import requests

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


log('process start', '01-dbBludger_Download', header=True, buffer=True)
log('time', str(dt.datetime.now()), buffer=True)
            
### Load config 
log('loading config', 'config.yaml', buffer=True)
with open('config.yaml', 'r') as file:
    configyaml = yaml.load(file.read()) # , Loader=yaml.FullLoader) #<-- will need eventually
    
### Create required subfolders
coafolders = {}
for fo in configyaml['folders']:
    coafolders.update(fo)
    log('defined folder', str(fo), buffer=True)
    
for nm, subfo in coafolders.items():
    if not os.path.exists(subfo):
        log('creating missing folder', subfo, buffer=True)
        os.mkdir(subfo)
        
# clear log buffer
logpath = os.path.join('.', coafolders['run'], 'runlog.txt')
log('logflie defined', logpath)  


### define the files to download (preserve names)
githost = 'https://raw.githubusercontent.com/tdcoa/usage/master/'
gitfiles=configyaml['download-files']
log('githost set',githost,header=True)

for gitfile in gitfiles:
    log('attempting to download', gitfile)
    giturl = githost+gitfile
    log('requesting url', giturl)
    filecontent = requests.get(giturl).content
    log('saving locally', 'file character length = %i' %len(filecontent))
    file = open(os.path.join('.', coafolders['download'].strip(), gitfile), 'w+')
    file.write(filecontent.decode('utf-8'))
    file.close
    log('file saved', 'OK!\n')
    
    if gitfile=='motd.txt' or gitfile[-3:]=='.py': 
        os.replace(os.path.join('.', coafolders['download'].strip(), gitfile), os.path.join('.', gitfile))
        
log('done!')
log('time', str(dt.datetime.now()))