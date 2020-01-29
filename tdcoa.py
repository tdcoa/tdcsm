import os, sys, io, re, errno, yaml, json, requests
from shutil import copyfile
import datetime as dt
import pandas as pd
from sqlalchemy.engine import create_engine

class tdcoa():

    # paths
    approot = ''
    configpath = ''
    logpath = ''
    secretpath = ''
    outputpath = ''

    # log settings
    logs =  []
    logspace = 30
    bufferlogs = True
    configyaml = ''
    printlog = True

    # dictionaries
    secrets = {}
    folders = {}
    substitutions = {}
    siteids = {}
    transcend = {}
    settings = {}
    gitfiles = []


    def __init__(self, approot='.', printlog=True):
        self.printlog = printlog
        self.bufferlog = True
        self.approot = self.getvalidpath(approot)
        self.configpath = os.path.join(self.approot,'config.yaml')
        self.secretpath = os.path.join(self.approot,'secrets.yaml')
        self.log('tdcoa started', header=True)
        self.log('time', str(dt.datetime.now()))

        if not os.path.isfile(self.configpath):
            self.log('missing config.yaml', 'creating %s' %self.configpath)
            self.create_config()
        if not os.path.isfile(self.secretpath):
            self.log('missing secrets.yaml', 'creating %s' %self.secretpath)
            self.create_secrets()

        self.load_config()


    def __str__(self):
        self.help()


    def load_config(self):
        """Load configuration YAML file from the supplied path.
           Default is ./config.yaml"""

        self.log('load_config started', header=True)
        self.log('time',str(dt.datetime.now()))
        self.bufferlogs = True

        # load secret.yaml
        self.log('loading secrets', os.path.basename(self.secretpath))
        with open(self.secretpath, 'r') as fh:
            secretsyaml = yaml.load(fh.read()) # , Loader=yaml.FullLoader) #<-- will need eventually
        for secret in secretsyaml['secrets']:
            self.secrets.update(secret)
        self.log('secrets loaded', str(len(self.secrets)))

        # load config.yaml
        self.log('loading config', os.path.basename(self.configpath))
        with open(self.configpath, 'r') as fh:
            configstr = fh.read()
        self.log('performing secret substitutions...')
        for n,v in self.secrets.items():
            configstr = configstr.replace('{%s}' %n, v)
        self.log('parsing config.yaml into dictionaries')
        self.configyaml = yaml.load(configstr)

        # load folders
        dlst = {  'folders'        : self.folders
                , 'substitutions'  : self.substitutions
                , 'siteids'        : self.siteids
                , 'transcend'      : self.transcend
                , 'settings'       : self.settings }
        for nm, obj in dlst.items():
            self.log('  parsing %s' %nm)
            for x in self.configyaml[nm]:
                obj.update(x)
                self.log('    %s' %str(x))

        self.log('  parsing gitfiles')
        self.gitfiles=self.configyaml['gitfiles']
        for x in self.gitfiles:
            self.log('    %s' %str(x))

        # create missing folders
        self.log('validating folder structures')
        for nm, subfo in self.folders.items():
            if not os.path.exists(subfo):
                self.log('creating missing folder', subfo)
                os.mkdir(subfo)

        self.logpath = os.path.join(self.folders['run'], 'runlog.txt')
        if os.path.isfile(self.logpath): os.remove(self.logpath)
        self.bufferlogs = False
        self.log('unbuffering log to "run" folder')

        self.log('done!')
        self.log('time', str(dt.datetime.now()))



    def download_files(self):
        self.log('download_files started', header=True)
        self.log('time',str(dt.datetime.now()))
        githost = self.settings['githost']
        self.log('githost',githost)

        for gitfile in self.gitfiles:
            self.log('attempting to download', gitfile)
            giturl = githost+gitfile
            self.log('requesting url', giturl)
            filecontent = requests.get(giturl).content
            self.log('saving locally', 'file character length = %i' %len(filecontent))
            file = open(os.path.join(self.approot, self.folders['download'].strip(), gitfile), 'w+')
            file.write(filecontent.decode('utf-8'))
            file.close
            self.log('file saved', 'OK!\n')

            if gitfile=='motd.txt':
                os.replace(os.path.join(self.approot, self.folders['download'].strip(), gitfile), os.path.join(self.approot, gitfile))

        self.log('done!')
        self.log('time', str(dt.datetime.now()))



    def prepare_sql(self, sqlfolder=''):
        self.log('prepare_sql started', header=True)
        self.log('time',str(dt.datetime.now()))

        if sqlfolder != '':
            self.log('sql folder override', sqlfolder)
            self.folders['sql'] = sqlfolder
        self.log(' sql folder', self.folders['sql'])
        self.log(' run folder', self.folders['run'])

        # clear out any pre-existing files in run folder:
        self.log('empty run folder...')
        for file in os.listdir(os.path.join(self.approot, self.folders['run'])):
            if file[:1] != '.' and file!='runlog.txt':
                self.log('deleting', file)
                os.remove(os.path.join(self.approot, self.folders['run'],file))

        #load .coa.sql files into the _run directory, with all replacements done (ready to run sql)
        self.log('\nprocessing all .coa.sql files found in %s and save output to %s' %(self.folders['sql'], self.folders['run']))
        for coafile in os.listdir(self.folders['sql']):
            if coafile[:1] != '.':
                self.log('\n'+ ('-'*20))
                if coafile[-8:]=='.coa.sql':  # if SQL, do substitutions
                    self.log('\nPROCESSING COA.SQL FILE', coafile)
                    with open(os.path.join(self.folders['sql'], coafile), 'r') as coasqlfile:           # read from template
                        coasqls = coasqlfile.read()
                        self.log('characters in file', str(len(coasqls)))

                    with open(os.path.join(self.folders['run'],coafile),'w') as runsqlfile:  # write to _run file

                        #light formatting of supplied sql
                        sqls = coasqls.split(';')
                        self.log('sql statements in file', str(len(sqls)))
                        i=0
                        for sql in sqls:
                            while '\n\n' in sql:
                                sql = sql.replace('\n\n','\n').strip()
                            sql = sql.strip() + '\n;\n\n'

                            if sql != '\n;\n\n':  # exclude null statements (only ; and newlines)
                                i+=1
                                self.log('\nprocessing sql %i' %i, '%s...' %sql[:50].replace('\n',' '))

                                # do substitutions first... (allows for substitution in {{loop}} command)
                                self.log('perform config.yaml substitutions')
                                for find,replace in self.substitutions.items():
                                    self.log('find: %s' %str(find), 'replace: %s' %str(replace))
                                    sql = sql.replace('{%s}' %str(find),str(replace))

                                # if there are any {{loop: commands, go get the csv
                                self.log('process any loop commands')
                                if '/*{{loop:' in sql:

                                    # parse csv file name from {{loop:csvfilename}} string
                                    csvfile = sql[(sql.find('/*{{loop:') + len('/*{{loop:')):sql.find('}}*/')].strip()
                                    self.log('LOOP FOUND, file', csvfile)

                                    # can we find the file?
                                    if os.path.isfile(os.path.join(self.folders['run'],csvfile)): # csv file found!  let's open:
                                        self.log('file found!')
                                        df = pd.read_csv(os.path.join(self.folders['run'],csvfile))
                                        self.log('rows in file', str(len(df)) )

                                        # perform csv substitutions
                                        self.log('perform csv file substitutions (find {column_name}, replace row value)')
                                        for index, row in df.iterrows():  # one row = one sql written to file
                                            tempsql = sql
                                            for col in df.columns:
                                                tempsql = tempsql.replace(str('{%s}' %col), str(row[col]))
                                            tempsql = tempsql.replace(csvfile,' csv row %i out of %i ' %(index+1, len(df)))
                                            tempsql = tempsql.replace('/*{{loop:','/*').replace('}}*/','*/')
                                            self.log('sql generated from row data', 'character length = %i' %len(tempsql))
                                            runsqlfile.write(tempsql)


                                    else:  # file not found, raise error
                                        self.log('\n\tERROR:\n\tFile Not Found\n\t%s' %csvfile)
                                        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), csvfile)

                                elif '/*{{temp' in sql:
                                    self.log('temp command found, will upload during "execute_run()"')
                                    runsqlfile.write(sql)

                                else:  # not a loop or temp, just write file as-is (post-replace)
                                    self.log('no special commands found')
                                    runsqlfile.write(sql)

                else:  # if not a .coa.sql file... just copy
                    self.log('\nPROCESSING NON-COA.SQL', coafile)
                    self.log('  (copy only)')
                    copyfile(os.path.join(self.approot, self.folders['sql'], coafile), os.path.join(self.approot, self.folders['run'], coafile))

        self.log('done!')
        self.log('time', str(dt.datetime.now()))






    def execute_run(self):
        self.log('execute_run started', header=True)
        self.log('time',str(dt.datetime.now()))

        # collect all prepared sql files, place in alpha order
        coasqlfiles=[]
        for coafile in os.listdir(os.path.join(self.approot, self.folders['run'])):
            if coafile[:1] != '.' and coafile[-8:]=='.coa.sql':
                self.log('found prepared sql file', coafile )
                coasqlfiles.append(coafile)
        coasqlfiles.sort()
        self.log('all sql files alpha-sorted for exeuction consistency')

        # create output folder:
        outputfo = os.path.join(self.approot, self.folders['output'], str(dt.datetime.now())[:-7].replace(' ','_').replace(':',''))
        self.log('output folder defined', outputfo)
        if not os.path.exists(outputfo):
            os.mkdir(outputfo)
            self.log('output folder created', outputfo)
        self.outputpath = outputfo

        # create our upload-manifest:
        self.log('creating upload manifest file')
        with open(os.path.join(outputfo,'upload-manifest.json'),'w') as manifest:
            manifest.write('{"entries":[ ')
        manifestdelim='\n '

        # loop thru systems and execute all
        for siteid, connstring in self.siteids.items():
            self.log('connecting to ', siteid, header=True )
            conn = create_engine(connstring) # <------------------------------- Connect to the database

            for coasqlfile in sorted(coasqlfiles):  # loop thru all sql files:
                self.log('\nOPENING SQL FILE', coasqlfile)
                with open(os.path.join(self.approot, self.folders['run'], coasqlfile), 'r') as coasqlfilehdlr:
                    sqls = coasqlfilehdlr.read()

                #  do any run-time substitutions (just SiteID)
                self.log('perform run-time substitutions: {siteid} == %s' %str(siteid).strip())
                sqls = sqls.replace('{siteid}', str(siteid).strip()).strip()

                sqlcnt = 0
                for sql in sqls.split(';'):  # loop thru the sql in the files
                    sqlcnt +=1

                    if sql == '':
                        self.log('null statement, skipping')
                    else:
                        self.log('----')
                        self.log('execute sql %i' %sqlcnt, sql[:50].replace('\n',' ').strip() + '...')

                        # pull out any embedded SQLcommands:
                        sqlcmd = self.__get_sqlcommands(sql)
                        sqlformatted = sqlcmd['sql']
                        del sqlcmd['sql']
                        sql = sqlformatted.replace('\n',' ')
                        while '  ' in sql:
                            sql = sql.replace('\n',' ').replace('  ',' ')

                        if len(sql.strip()) !=0:
                            self.log('sql submitted', str(dt.datetime.now()))
                            df = pd.read_sql(sql, conn)# <------------------------------- Run SQL
                            self.log('sql completed', str(dt.datetime.now()))
                            self.log('record count', str(len(df)))

                        if 'temp' in sqlcmd: # build a create table statement, so we're sure it's volatile...
                            tbl = sqlcmd['temp']
                            self.log('fill temp table', tbl)

                            # open csv
                            self.log('open csv', sqlcmd['temp'])
                            csvfile = sqlcmd['temp']
                            dfcsv  = pd.read_csv(csvfile)

                            # upload to temp table:
                            self.log('uploading', str(dt.datetime.now()))
                            # dfcsv.to_sql(tbl, conn, if_exists='append', index=False)

                            # build manual transactions for now...
                            sql = []
                            sqlprefix = 'create multiset volatile table "%s" as (\nselect ' %csvfile
                            for idx, row in dfcsv.iterrows():
                                sql.append(sqlprefix)
                                sqlprefix = 'union all \nselect '
                                delim = ' '
                                for col, val in row.items():
                                    colnm = re.sub('[^0-9a-zA-Z]+', '_', col)
                                    if type(val) is int:
                                        coltype = 'BIGINT'
                                    elif type(val) is float:
                                        coltype = 'FLOAT'
                                    else:
                                        collen = dfcsv[col].map(len).max()
                                        coltype = 'VARCHAR(%i)' %(collen+100)
                                    sql.append('%scast(\'%s\' as %s) as %s' %(delim,val,coltype,colnm))
                                    delim = ','
                                sql.append('from (sel 1 one) i%i' %idx)
                            sql.append(') with data \n  no primary index \n  on commit preserve rows;')

                            sql = '\n'.join(sql)
                            df = pd.read_sql(sql, conn)
                            self.log('complete', str(dt.datetime.now()))


                        if len(df) != 0:  # Save non-empty returns to .csv

                            # finally report on the get_sqlcommand(sql) line above
                            if len(sqlcmd)==0:
                                self.log('no special commands found')

                            if 'save' not in sqlcmd:
                                sqlcmd['save'] = '%s--%s' %(siteid, coasqlfile) + '%04d' %sqlcnt + '.csv'

                            # once built, append output folder, SiteID on the front, iterative counter on back if needed for uniquess
                            csvfile = os.path.join(self.approot, outputfo, sqlcmd['save'])
                            i=0
                            while os.path.isfile(csvfile):
                                i +=1
                                if i==1:
                                    csvfile = csvfile[:-4] + '.%03d' %i + csvfile[-4:]
                                else:
                                    csvfile = csvfile[:-8] + '.%03d' %i + csvfile[-4:]
                            self.log('CSV save location', csvfile)

                            self.log('saving file...')
                            df.to_csv(csvfile) # <---------------------- Save to .csv
                            self.log('file saved!')

                            if 'load' in sqlcmd:  # add to manifest
                                self.log('file marked for loading to Transcend, adding to upload-manifest.json')
                                if 'call' not in sqlcmd:   sqlcmd['call']=''
                                manifest_entry = '%s{"file": "%s",  "table": "%s",  "call": "%s"}' %(manifestdelim, sqlcmd['save'], sqlcmd['load'], sqlcmd['call'])
                                manifestdelim='\n,'

                                with open(os.path.join(outputfo,'upload-manifest.json'),'a') as manifest:
                                    manifest.write(manifest_entry)
                                    self.log('Manifest updated', str(manifest_entry).replace(',',',\n'))

        self.log('post-processing', header=True)


        # close JSON object
        with open(os.path.join(outputfo,'upload-manifest.json'),'a') as manifest:
            manifest.write("\n  ]}")
            self.log('closing out upload-manifest.json')


        # Move all files from run folder to output, for posterity:
        self.log('moving all run artifacts to output folder, for archiving')
        for file in os.listdir(os.path.join(self.approot, self.folders['run'])):
            if file[:1]!='.' and file!='runlog.txt':
                os.replace( os.path.join(self.approot, self.folders['run'], file), os.path.join(outputfo, file) )
                self.log('  moved', file)

        # create hidden file containing last run's output -- to remain in the root folder
        with open(os.path.join(self.approot, '.last_run_output_path.txt'), 'w') as lastoutput:
            lastoutput.write(outputfo)
            self.log('save location of last-run output folder to hidden file')
            self.log('last-run output', outputfo)

        # also COPY a few other operational files to output folder, for ease of use:
        file = '.last_run_output_path.txt'
        self.log('copy %s to run folder, for ease of use' %file)
        copyfile( os.path.join(self.approot, file), os.path.join(outputfo, file) )
        file = os.path.basename(self.configpath)
        self.log('copy %s to run folder, for ease of use' %file)
        copyfile(self.configpath, os.path.join(outputfo, file))

        self.log('\ndone!')
        self.log('time', str(dt.datetime.now()))

        file = 'runlog.txt'
        os.replace( os.path.join(self.approot, self.folders['run'], file), os.path.join(outputfo, file) )





    def upload_to_transcend(self, outputpath=''):
        self.bufferlogs = True
        self.log('upload_to_transcend started', header=True)
        self.log('time',str(dt.datetime.now()))

        # three ways to get the outputfolder, in this priority:
        #  parameter: outputpath
        #  self.outputpath
        #  file: .last_run_output_path.txt
        if outputpath !='':   # supplied parameter
            outputfo = outputpath
            self.log('output folder= manual param', outputfo)
        elif self.outputpath !='':  # local variable set
            outputfo = self.outputpath
            self.log('output folder= class var', outputfo)
        else:
            if os.path.isfile(os.path.join(self.approot, '.last_run_output_path.txt')):
                with open(os.path.join(self.approot, '.last_run_output_path.txt'),'r') as fh:
                    outputfo = fh.read().strip().split('\n')[0]
                self.log('output folder= .file', outputfo)
            else:
                outputfo =''

        # now that outputfo is defined, let's make sure the dir actually exists:
        if not os.path.isdir(outputfo):
            self.log('\nERROR = invalid path', outputfo)
            raise NotADirectoryError('Invalid Path: %s' %outputfo)
            exit()
        else:
            self.outputpath = outputfo

        # update log file  to correct location
        self.logpath = os.path.join(outputfo, 'runlog.txt')
        self.bufferlogs = False

        # define upload manifest:
        self.log('open file', 'upload-manifest.json')
        with open(os.path.join(outputfo, 'upload-manifest.json'),'r') as manfile:
            manjson = manfile.read()
            manifest = json.loads(manjson)
            self.log('upload count found', str(len(manifest)) )

        # loop thru all TRANSCEND systems and execute manifest
        for name, connstring in self.transcend.items():
            self.log('connecting to', name, header=True)
            conn = create_engine(connstring) # <-------------------- Connect to the database

            for entry in manifest['entries']:

                self.log('\nPROCESSING NEW ENTRY')
                self.log('  load file', entry['file'])
                self.log('  into table', entry['table'])
                self.log('  then call', entry['call'])

                # open CSV file for reading
                self.log('opening file', entry['file'])
                dfcsv = pd.read_csv(os.path.join(outputfo, entry['file']))
                self.log('records found', str(len(dfcsv)))

                for col in dfcsv.columns:
                    if col[:8] == 'Unnamed:':
                        self.log('unnamed column dropped', col)
                        self.log('  (usually the pandas index as a column, "Unnamed: 0")')
                        dfcsv = dfcsv.drop(columns=[col])
                self.log('final column count', str(len(dfcsv.columns)))

                if len(entry['table'].split('.')) == 1:
                    db = 'adlste_coa'
                    tbl = entry['table']
                else:
                    db = entry['table'].split('.')[0]
                    tbl = entry['table'].split('.')[1]

                # APPEND data set to table
                self.log('uploading', str(dt.datetime.now()))
                dfcsv.to_sql(tbl, conn, schema=db, if_exists='append', index=False)
                self.log('complete', str(dt.datetime.now()))

                # CALL any specified SPs:
                if str(entry['call']).strip() != "":
                    self.log('Stored Proc', str(dt.datetime.now()) )
                    conn.execute('call %s ;' %str(entry['call']) )
                    self.log('complete', str(dt.datetime.now()))

        self.log('\ndone!')
        self.log('time', str(dt.datetime.now()))



    def help(self):
        h=['HELP on Teradata Consumption Analytics']
        h.append('-'*30)
        h.append("This library will perform 4 descrete steps: ")
        h.append("  (1) DOWNLOAD sql and csv from github respositories, ")
        h.append("  (2) PREPARE sql locally, including variable substitutions and csv merging,  ")
        h.append("  (3) EXECUTE the prepared sql against customer site ids, and export any ")
        h.append("      indicated data sets to csv, then finally ")
        h.append("  (4) UPLOAD specifically indicated csv to Transcend, and call named stored procs")
        h.append("      to merge uploaded temp tables with final target tables.")
        h.append("")
        h.append("Each step is designed to be autonomous and can be run independently, assuming")
        h.append("all dependencies are met.")
        h.append("This allows CSMs to download and prepare SQL first, insepct and make manual")
        h.append("changes to reflect the particular needs of their customer, all on their local PC.")
        h.append("Once happy with the script, they can move the process over and execute on a ")
        h.append("customer-owned laptop, where the process should pick-up seemlessly. Continuing ")
        h.append("on, the CSM could execute on the customer's TD system, export CSVs, and move")
        h.append("back to the CSM's laptop where results can be uploaded to Transcend.")
        h.append("")
        h.append('Sample Usage:')
        h.append('1  from tdcsm.tdcoa import tdcoa')
        h.append('2  coa = tdcoa() \n')
        h.append('3  coa.download_files()')
        h.append('4  coa.prepare_sql()')
        h.append('5  coa.execute_run()')
        h.append('6  coa.upload_to_transcend() \n')
        h.append('what stuff does, by line (python 3.6+):')
        h.append("Line 1 = import the class.  Pretty standard python stuff.")
        h.append("Line 2 = instantiate the class.  This will also setup the local running environment ")
        h.append("         in the same directory as your calling script (by default, but you can supply")
        h.append("         an alternate path as a parameter).  If they are missing, it will also create")
        h.append("         default files such as secrets.yaml and config.yaml, which are critical for")
        h.append("         the process.  It will NOT overwrite existing files.")
        h.append("         ** if you are running for the first time, it is recommended you run just ")
        h.append("             line 1 & 2 first, and modify secrets.yaml and config.yaml as needed.")
        h.append("             This makes subsequent tests more interesting, as credentials will work.")
        h.append("Line 3 = download any missing files or sql from GitHub. URL and file inventory are")
        h.append("         both stored in the config.yaml.  the 0000.*.sql files are example files.")
        h.append("         While line 3 only needs to be run once, sql *will* be overwritten with ")
        h.append("         newer content, so it is recommended you update periodically.")
        h.append("Line 4 = iterates all .coa.sql and .csv files in in the 'sql' folder and preapres")
        h.append("         the sql for later execution. This step includes several sustitution")
        h.append("         steps: from secrets.yaml (never printed in logs), from config.yaml in ")
        h.append("         the substitution section, and from any embedded /*{{loop:myfile.csv}} ")
        h.append("         command. In the last case, the csv is opened and the process generates")
        h.append("         one new sql per row in the file, substituting {{column_name}} with the ")
        h.append("         value for that row. All of these sql scripts are written out to the ")
        h.append("         'run' directory as defined in the config.yaml, awaiting the next step.")
        h.append("Line 5 = iterates thru all site id connection strings first, then thru  all sql ")
        h.append("         files found in the 'run' directory *in alpha order*, and executes the sql.")
        h.append("         all substitutions are done in the previous step, so besides secrets.yaml ")
        h.append("         replacements and certain runtime values like {{siteid}}, the sql will be")
        h.append("         exactly what is run.  There are three special commands, formatted in ")
        h.append("         comments:")
        h.append("           /*{{save:MyFile.csv}}*/ = save the sql results to the named csv")
        h.append("           /*{{load:db.Tablename}}*/ = load the above csv to the Transcend table")
        h.append("           /*{{call:db.StoredProc}}*/ = call the stored proc after above load")
        h.append("         The first special command will execute during step 5, exporting data from")
        h.append("         each siteid.   Note that it is possible to use {{substitution}} in these")
        h.append("         commands, so /*{{save:{{siteid}}_export.csv}}*/ is valid, and will ")
        h.append("         produce a different csv for each site id you run against.  Without this,")
        h.append("         the process might run against many siteids, but each would overwrite the")
        h.append("         same filename.")
        h.append("         The last two special commands, load and call, are intended to run against")
        h.append("         transcend, and so in step five are only written to 'upload_manifest.json' ")
        h.append("         and saved in the same 'output' folder, awaiting the next and final step.")
        h.append("Line 6 = for each line in the 'upload_manifest.json', perform the requested load")
        h.append("         and subsequent stored proc call.  The intention is to upload csv files")
        h.append("         into Transcend Global Temporary Tables, then initiate a stored procedure ")
        h.append("         to complete whatever cleansing, validation, and data movement is required")
        h.append("         to merge into the final target table.  This keeps most of the business")
        h.append("         logic as close to the data as possible.")
        h.append("\n")
        h.append("DEBUGGING:\n")
        h.append("Missing tdcoa -- if you get an error stating you're missing tdcoa, then first, I'm")
        h.append("unclear on how you're reading this text.  That aside, open up a command prompt and type:")
        h.append("pip install tdcoa")
        h.append("if that gives you errors, then maybe this wasn't meant to be. Call Stephen Hilton.")
        h.append("")
        h.append("If you get errors stating you're missing the 'teradata dialect' of sqlalchemy, open")
        h.append("up a command prompt and type: ")
        h.append("pip install sqlalchemy-teradata")
        h.append("this will prompt python to install the teradata drivers needed to connect.")
        print( '\n'.join(h) )





    def log(self, msgleft='', msgright='', header=False):
        delim = ':'
        if  msgright=='': delim=''
        msg = '%s%s' %(str(msgleft+delim).ljust(self.logspace), msgright)
        # prevent secrets from appearing in log:
        for nm,secret in self.secrets.items():
            if secret in msg:
                msg = msg.replace(secret, '%s%s%s' %(secret[:1],'*'*(len(secret)-2),secret[-1:]))
        if header: msg = '\n\n%s\n%s\n%s' %('='*40, msg.upper(), '-'*40)
        if self.bufferlogs:
            self.logs.append(msg)
            if self.printlog: print(msg)
        else: # no buffer
            if len(self.logs) ==0:
                if self.printlog: print(msg)
                self.__writelog(msg)
            else:
                if self.printlog: print(msg)
                for log in self.logs:
                    self.__writelog(log)
                    self.logs=[]
                self.__writelog(msg)
    def __writelog(self, msg):
        with open(self.logpath,'a') as logfile:
            logfile.write(msg + '\n')



    def getvalidpath(self, pathcandidate, exit_on_error=False):
        path = pathcandidate
        if path == '':
            return str(os.getcwd())
        if os.path.isdir(path):
            return path
        if os.path.isfile(path):
            return os.path.dirname(path)
        self.log('\nERROR = Invalid Path', pathcandidate, buffer=True)
        if exit_on_error:
            log('File required to proceed, terminating process...', buffer=True)
            raise NotADirectoryError('TDCOA: Invalid Path',pathcandidate)
            exit()


    def create_upload_helper(self):
        helper = os.path.join(self.approot, self.folders['run'], 'upload_helper.py' )
        pf=['# this is a helper script to more eaily upload to Transcend ']
        pf.append('# from inside a dated "output" folder.')
        pf.append('')
        pf.append('# todo: get cwd, backup two levels, then ')
        pf.append('import os')
        pf.append('os.setcwd("../..")')
        pf.append('from tdcoa import tdcoa')
        pf.append("coa = tdcoa()")
        pf.append('coa.test()')
        self.log('creating helper.py file', os.path.basename(helper))
        with open(helper, 'w') as fh:
            fh.write('\n'.join(pf))

    def create_secrets(self):
        secrets = os.path.join(self.getvalidpath(self.approot,True), 'secrets.yaml' )
        pf=['# this file contains sensitive information in a Name:Value format, i.e.,']
        pf.append('#     tdpwd: "P@22w0rd"')
        pf.append('# At runtime, the config.yaml file will be scrubbed for any ')
        pf.append('# name in the secret.yaml within {}, and substituted.  For example,')
        pf.append('# any {tdpwd} in the config.yaml file will become P@22w0rd')
        pf.append('# This isolates sensitive passwords in one file, and allows for')
        pf.append('# sharing of the config.yaml without sharing logins.')
        pf.append('')
        pf.append('secrets:')
        pf.append('  - tdpwd1:   "P@22w0rd"')
        pf.append('  - custpwd2: "p0Pc0r4-@2020"')
        pf.append('  - custIP:   "123.234.231.100"')
        pf.append('')
        pf.append('  - quicklook:   "ab123456"')
        pf.append('  - td_password: "ilovevantage"')
        pf.append('')
        pf.append('  - username:   "vantagelabuser"')
        pf.append('  - password:   "vantagelabpwd"')
        with open( os.path.join(secrets), 'w') as fh:
            fh.write('\n'.join(pf))


    def create_config(self, configpath='', exit_on_error=False):
        if self.configpath == '' and configpath=='':
            self.log('\nERROR = Invalid Path', configpath, buffer=True)
            if exit_on_error:
                log('File required to proceed, terminating process...', buffer=True)
                raise NotADirectoryError('TDCOA: Invalid Path',pathcandidate)
                exit()
            else:
                return None
        if configpath == '': configpath = self.configpath
        if self.configpath != configpath: self.configpath = configpath

        cf = ['']
        cf.append('substitutions:')
        cf.append('  - account:     "Teradata"')
        cf.append('  - startdate:   "\'2020-01-01\'"')
        cf.append('  - enddate:     "Current_Date - 1"')
        cf.append('  - whatever:    "anything you want"')
        cf.append('\n\nsiteids:')
        cf.append('  # for testing:')
        cf.append('  - Altans_VDB:  "teradatasql://{username}:{password}@tdap278t1.labs.teradata.com"')
        cf.append('  # customer systems:')
        cf.append('#  - CUSTOMER001: "teradatasql://{username}:{password}@{host}"')
        cf.append('\n\ntranscend:')
        cf.append('  - TranscendIFX:  "teradatasql://{quicklook}:{td_password}@tdprdcop3.td.teradata.com/?logmech=LDAP"')
        cf.append('\n\nsettings:')
        cf.append('  - debug:  True  # if true, will NOT connect to database, instead emulate output only (for testing)')
        cf.append('  - githost: "https://raw.githubusercontent.com/tdcoa/sql/master/"')
        cf.append('\n\nfolders:')
        cf.append('  - download:  "coa_default" # download_files step saves downloaded sql to this folder')
        cf.append('  - sql:       "coa_default" # prepare_sql step pulls static sql from here, applies substitution, and saves to the run folder')
        cf.append('  - run:       "_run" # execute_run step reads prepared sql from here and executes against database')
        cf.append('  - output:    "_output" # historical run (dated) folders, including all csv output, completed sql files, and logs')
        cf.append('\n\ngitfiles:')
        cf.append('  - motd.txt')
        cf.append('  - 0000.dbcinfo.coa.sql')
        cf.append('  - 0000.dates.csv')
        with open( os.path.join(configpath), 'w') as fh:
            fh.write('\n'.join(cf))


    def __get_sqlcommands(self, sql):
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
                self.log('special command found', '%s = %s' %(str(cmdlst[0]).strip(), str(cmdlst[1]).strip()))
                sql = sql.replace(cmdstr,'')
            else:
                sql = sql.replace(cmdstr,'/* %s */' %cmdlst[0])
        cmd['sql'] = sql
        return cmd
