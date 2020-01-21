import os, sys, yaml
import pandas as pd
import datetime as dt
from sqlalchemy.engine import create_engine

def get_sqlcommands(sql):
    cmdstart = '/*{{'
    cmdend = '}}*/'
    cmd = {}

    while cmdstart in sql:
        pos1 = sql.find(cmdstart)
        pos2 = sql.find(cmdend)
        cmdstr = sql[pos1:pos2+len(cmdend)]
        cmdlst = cmdstr.replace(cmdstart,'').replace(cmdend,'').split(':')
        cmd[cmdlst[0].strip()] = cmdlst[1].strip()
        sql = sql.replace(cmdstr,'')
    return cmd

#load config:
with open('config.yaml', 'r') as file:
    configyaml = yaml.load(file.read()) # , Loader=yaml.FullLoader) #<-- will need eventually
coafolders = {}
for fo in configyaml['folders']:
    coafolders.update(fo)
coasystems = {}
for sy in configyaml['siteids']:
    coasystems.update(sy)

# collect all prepared sql files, place in alpha order
coasqlfiles=[]
for coafile in os.listdir(coafolders['run']):
    if coafile[:1] != '.' and coafile[-8:]=='.coa.sql':
        coasqlfiles.append(coafile)
coasqlfiles.sort()

# create output folder:
outputfo = os.path.join('.', coafolders['output'], str(dt.datetime.now())[:-7].replace(' ','_').replace(':',''))
if not os.path.exists(outputfo):
    os.mkdir(outputfo)

# create our upload-manifest:
with open(os.path.join(outputfo,'upload-manifest.json'),'w') as manifest:
    manifest.write('{"entries":[ ')
manifestdelim='\n '

# loop thru systems and execute all
for siteid, connstring in coasystems.items():
    conn = create_engine(connstring) # <------------------------------- Connect to the database

    for coasqlfile in sorted(coasqlfiles):  # loop thru all sql files:
        with open(os.path.join('.', coafolders['run'], coasqlfile), 'r') as coasqlfilehdlr:
            sqls = coasqlfilehdlr.read()
        sqlcnt = 1
        for sql in sqls.split(';'):  # loop thru the sql in the files
            sqlcnt +=1

            #  do any run-time substitutions (just SiteID)
            sql = sql.replace('{siteid}', str(siteid).strip()).strip()
            if sql != '':
                print('\nSQL Run started:%s\n%s' %(str(dt.datetime.now()), sql))
                df = pd.read_sql(sql, conn)# <------------------------------- Run SQL

                # pull out any embedded SQLcommands:
                sqlcmd = get_sqlcommands(sql)

                if len(df) != 0:  # Save non-empty returns to .csv

                    if 'save' not in sqlcmd:
                        sqlcmd['save'] = coasqlfile + '%04d' %sqlcnt + '.csv'

                    # once built, append output folder, SiteID on the front, iterative counter on back if needed for uniquess
                    csvfile = os.path.join('.', outputfo, sqlcmd['save'])
                    i=0
                    while os.path.isfile(csvfile):
                        i +=1
                        csvfile[:-4] + '.%03d' %i + csvfile[-4:]

                    df.to_csv(csvfile) # <---------------------- Save to .csv

                    if 'load' in sqlcmd:  # add to manifest
                        if 'call' not in sqlcmd:   sqlcmd['call']=''
                        manifest_entry = '%s{"file": "%s",  "table": "%s",  "call": "%s"}' %(manifestdelim, sqlcmd['save'], sqlcmd['load'], sqlcmd['call'])
                        manifestdelim='\n,'

                        with open(os.path.join(outputfo,'upload-manifest.json'),'a') as manifest:
                            manifest.write(manifest_entry)


# close JSON object
with open(os.path.join(outputfo,'upload-manifest.json'),'a') as manifest:
    manifest.write("\n  ]}")

# Move all files from run folder to output, for posterity:
for file in os.listdir(os.path.join('.', coafolders['run'])):
    if file[:1]!='.':
        os.replace( os.path.join('.', coafolders['run'], file), os.path.join(outputfo, file) )

# also move  04-dbBludger_TranscendUpload.py  to output folder, for ease of use:
file = '04-dbBludger_TranscendUpload.py'
os.replace( os.path.join('.', file), os.path.join(outputfo, file) )
