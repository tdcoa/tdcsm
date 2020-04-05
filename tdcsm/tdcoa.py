import os, sys, io, re, errno, yaml, json, requests, time, shutil
import datetime as dt
import pandas as pd
import numpy as np
# --  Teradata Drivers:
import teradata  # odbc driver
import sqlalchemy
from teradataml.context import context as tdml_context
from teradataml.dataframe import dataframe as tdml_df
from teradataml.dataframe.copy_to import copy_to_sql

class tdcoa():
    """DESCRIPTION:
    tdcsm.tdcoa is a collection of high-level operations to fully automate
    the collection of consumption analytics of a Teradata platform.  This
    class focuses on automating four major steps:
        1 - DOWNLOAD sets of files (sql, csv)
        2 - PREPARE those files to be executed
        3 - EXECUTE files against target system (on target VPN)
        4 - UPLOAD results to a central repository (on "Transcend" VPN)

    More detail on each step is available via help() per step function below.
    The only initial requirement is Python 3.6 or higher, and access /login to
    the required Teradata systems.

    GETTING STARTED:
    From a command line:
      pip install tdcsm

    To pickup the most recent updates, it is recommended you
      pip install tdcsm --upgrade

    EXAMPLE USAGE:
    The entire process, at it's simpliest, looks like  this:
      python
      >>> from tdcsm.tdcoa import tdcoa
      >>> c = tdcoa()
      >>> c.download_files()
      >>> c.copy_download_to_sql()
      >>> c.prepare_sql()
      >>> c.execute_run()          # target system VPN
      >>> c.upload_to_transcend()  # transcend VPN
    """

    # paths
    approot = '.'
    configpath = ''
    logpath = ''
    secretpath = ''
    filesetpath = ''
    outputpath = ''
    version = "0.3.6.2"

    # log settings
    logs =  []
    logspace = 30
    bufferlogs = True
    configyaml = ''
    printlog = True
    show_full_sql = False
    skipgit = False
    skipdbs = False

    # dictionaries
    secrets = {}
    filesets = {}
    files = {}
    systems = {}
    folders = {}
    substitutions = {}
    transcend = {}
    settings = {}


    def __init__(self, approot='.', printlog=True, config='config.yaml', secrets='secrets.yaml', filesets='filesets.yaml', make_setup_file=False):
        self.bufferlog = True
        self.printlog = printlog
        self.approot = os.path.join('.', approot)
        self.configpath = os.path.join(self.approot, config)
        self.secretpath = os.path.join(self.approot, secrets)
        self.log('tdcoa started', header=True)
        self.log('time', str(dt.datetime.now()))
        self.log('app root', self.approot)
        self.log('config file', self.configpath)
        self.log('secrets file', self.secretpath)
        self.log('tdcoa version',  self.version)

        self.skipgit = False
        self.skipdbs = False

        if make_setup_file:
            self.pyfile_setup(filename='setup.py')

        # stub in config.yaml, secrets.yaml, if missing
        if not os.path.isfile(self.configpath):
            self.log('missing config.yaml', 'creating %s' %self.configpath)
            self.yaml_config(writefile=True, skipgit=self.skipgit, skipdbs=self.skipdbs)
        if not os.path.isfile(self.secretpath):
            self.log('missing secrets.yaml', 'creating %s' %self.secretpath)
            self.yaml_secrets(writefile=True)
        # filesets.yaml is validated at download time

        self.reload_config()







# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# -=-=  utilities... some of these should move to a new tdcsm.tdutil.tdutil()
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=


    def yaml_filesets(self, writefile=False):
        self.log('generating filesets.yaml from internal default')
        cy=[]
        cy.append('demo:')
        cy.append('  active:          "True"')
        cy.append('  fileset_version: "1.0"')
        cy.append('  startdate:       "Current_Date - 7"')
        cy.append('  enddate:         "Current_Date - 1"')
        cy.append('  some_value:      "Donkey"')
        cy.append('  files: ')
        cy.append('    - "demo/0000.dates.csv"')
        cy.append('    - "demo/0000.dbcinfo.coa.sql"')
        cy.append('    - "demo/example.sql"')
        cy.append('')


        rtn = '\n'.join(cy)
        if writefile:
            with open( os.path.join(self.filesetpath), 'w') as fh:
                fh.write(rtn)
            self.log('  saving file', self.filesetpath)
        return rtn




    def yaml_secrets(self, writefile=False):
        self.log('generating secrets.yaml from internal default')
        cy=[]
        cy.append('# this file contains sensitive information that')
        cy.append('# is typically NOT SHARED beyond one user, for')
        cy.append('# example, passwords, some usernames, and IPs.')
        cy.append('')
        cy.append('secrets:')
        cy.append('  tdpwd1:   "P@22w0rd"')
        cy.append('')
        cy.append('  custpwd2: "p0Pc0r4-@2020"')
        cy.append('  custIP:   "123.234.231.100"')
        cy.append('')
        cy.append('  td_quicklook: "sh186014"')
        cy.append('  td_password:  "qlikpassword"')
        cy.append('')
        cy.append('  custabc_username: "custUsername"')
        cy.append('  custabc_password: "Ter@custabc_password"')
        cy.append('')
        cy.append('  username:   "vantagelabuser"')
        cy.append('  password:   "vantagelabpwd"')
        cy.append('')
        rtn = '\n'.join(cy)
        if writefile:
            with open( os.path.join(self.secretpath), 'w') as fh:
                fh.write(rtn)
            self.log('  saving file', self.secretpath)
        return rtn



    def yaml_config(self, writefile=False, skipgit=False, skipdbs=True):
        self.log('generating config.yaml from internal default')
        cy=[]
        cy.append('# Substitutions can be used in both the config.yaml (below), ')
        cy.append('#   as well as all .coa.sql files generated in later steps.')
        cy.append('#   format is {name} --> value   (all instances)')
        cy.append('#   Order of config substitution is: ')
        cy.append('#      - secrets.yaml')
        cy.append('#      - config.yaml: substitutions, folders, settings, transcend')
        cy.append('#   Order of sql substitution is, per file set: ')
        cy.append('#      - config.yaml: system, substitutions, filesets (in download folder)')
        cy.append('')
        cy.append('')
        cy.append('substitutions:')
        cy.append('  account:            "Demo Customer"')
        cy.append('  startdate:          "Current_Date - 7"')
        cy.append('  enddate:            "Current_Date - 1"')
        cy.append('  some_value:         "Camel"')
        cy.append('')
        cy.append('')
        cy.append('systems:')
        cy.append('  Altans_VDB:')
        cy.append('    siteid:      "tdaltanvbd01"')
        cy.append('    active:      "True"')
        cy.append('    host:        "tdap278t1.labs.teradata.com"')
        cy.append('    username:    "{username}"')
        cy.append('    password:    "{password}"')
        cy.append('    logmech:     "" ')
        cy.append('    use:         "test"   # prod/qa/dr/dev/etc')
        cy.append('    some_value:  "Bear"')
        cy.append('    filesets:')
        cy.append('      demo:')
        cy.append('        active:     "True"')
        cy.append('        some_value: "Aardvark" ')
        cy.append('')
        cy.append('  Customer_System:')
        cy.append('    siteid:     "custabcprod01"')
        cy.append('    active:     "false"')
        cy.append('    host:       "prod.custabc.com"')
        cy.append('    username:   "{custabc_username}"')
        cy.append('    password:   "{custabc_password}"')
        cy.append('    logmech:    "" ')
        cy.append('    use:        "prod"')
        cy.append('    filesets:')
        cy.append('      demo:')
        cy.append('        active: "False"')
        cy.append('      level1_how_much_1620pdcr:')
        cy.append('        active: "False"')
        cy.append('        startdate:  "Current_Date - 365"')
        cy.append('        enddate:    "Current_Date - 1"')
        cy.append('      level1_how_much_1620dbc:')
        cy.append('        active: "False"')
        cy.append('        startdate:  "Current_Date - 365"')
        cy.append('        enddate:    "Current_Date - 1"')
        cy.append('      level2_how_well_1620pdcr:')
        cy.append('        active: "False"')
        cy.append('        startdate:  "Current_Date - 90"')
        cy.append('        enddate:    "Current_Date - 1"')
        cy.append('      dbql_core_1620pdcr:')
        cy.append('        active: "False"')
        cy.append('        startdate:  "Current_Date - 45"')
        cy.append('        enddate:    "Current_Date - 1"')
        cy.append('')
        cy.append('')
        cy.append('transcend:')
        cy.append('  host:       "tdprdcop3.td.teradata.com"')
        cy.append('  username:   "{td_quicklook}"')
        cy.append('  password:   "{td_password}"')
        cy.append('  logmech:    "LDAP"')
        cy.append('  db_coa:     "adlste_coa"')
        cy.append('  db_region:  "adlste_westcomm"')
        cy.append('')
        cy.append('')
        cy.append('folders:')
        cy.append('  override:  "0_override"')
        cy.append('  download:  "1_download"')
        cy.append('  sql:       "2_sql_store"')
        cy.append('  run:       "3_ready_to_run"')
        cy.append('  output:    "4_output"')
        cy.append('')
        cy.append('')
        cy.append('settings:')
        cy.append('  githost:    "https://raw.githubusercontent.com/tdcoa/sql/master/"')
        cy.append('  gitfileset: "filesets.yaml"')
        cy.append('  gitmotd:    "motd.txt"')
        cy.append('  localfilesets:   "./{download}/filesets.yaml"')
        cy.append('  skip_git:   "%s"' %str(skipgit))
        cy.append('  skip_dbs:   "%s"' %str(skipdbs))
        cy.append('  run_non_fileset_folders: "True" ')
        cy.append('  customer_connection_type:  "sqlalchemy"')
        # cy.append('  transcend_connection_type: "teradataml"')
        cy.append('    # valid connection types: "teradataml", "sqlalchemy", or an odbc driver name')

        rtn = '\n'.join(cy)
        if writefile:
            with open( os.path.join(self.configpath), 'w') as fh:
                fh.write(rtn)
            self.log('  saving file', self.configpath)
        return rtn



    def pyfile_setup(self, filename=''):
        if filename=='': filename = os.path.join('..','setup.py')
        self.log('\ngenerating setup.py for pypi')
        cy=[]
        cy.append('import setuptools ')
        cy.append('')
        cy.append('with open("README.md", "r") as fh: ')
        cy.append('    long_description = fh.read() ')
        cy.append('')
        cy.append('setuptools.setup( ')
        cy.append('    name="tdcsm", ')
        cy.append('    version="%s", ' %self.version)
        cy.append('    author="Stephen Hilton", ')
        cy.append('    author_email="Stephen@FamilyHilton.com", ')
        cy.append('    description="Teradata tools for CSMs", ')
        cy.append('    long_description=long_description, ')
        cy.append('    long_description_content_type="text/markdown", ')
        cy.append('    url="https://github.com/tdcoa/tdcsm", ')
        cy.append('    packages=setuptools.find_packages(), ')
        cy.append('    install_requires=[ ')
        cy.append('          "pandas", ')
        cy.append('          "numpy", ')
        cy.append('          "requests", ')
        cy.append('          "pyyaml", ')
        cy.append('          "teradatasqlalchemy", ')
        cy.append('          "teradataml", ')
        cy.append('          "teradata" ')
        cy.append('      ], ')
        cy.append('    classifiers=[ ')
        cy.append('        "Programming Language :: Python :: 3", ')
        cy.append('        "License :: OSI Approved :: MIT License", ')
        cy.append('        "Operating System :: OS Independent", ')
        cy.append('    ], ')
        cy.append('    python_requires=">=3.6", ')
        cy.append(') ')

        rtn = '\n'.join(cy)
        with open( filename, 'w') as fh:
            fh.write(rtn)
        self.log('  saving file', filename)
        return None




    def sql_create_temp_from_csv(self, csvfilepath):
        tbl = os.path.basename(csvfilepath)
        self.log('    transcribing sql', tbl)

        # open csv
        self.log('    open csv', tbl)
        dfcsv  = pd.read_csv(csvfilepath)
        self.log('    rows in file', str(len(dfcsv)))

        maxrows=100
        tblcreated = False
        sqlclosed=False

        # build manual transactions for now...
        sql = []
        sqlprefix = 'create multiset volatile table "%s" as (\nselect ' %tbl
        for idx, row in dfcsv.iterrows():
            sqlclosed = False
            sql.append(sqlprefix)
            delim = ' '
            for col, val in row.items():
                colnm = re.sub('[^0-9a-zA-Z]+', '_', col)
                if type(val) is int:
                    coltype = 'BIGINT'
                    quote = ''
                elif type(val) is float:
                    coltype = 'FLOAT'
                    quote = ''
                else:
                    collen = dfcsv[col].map(len).max()
                    coltype = 'VARCHAR(%i)' %(collen+100)
                    quote = "'"
                if pd.isna(val):
                    val='NULL'
                    quote=''
                val = '%s%s%s' %(quote,val,quote)
                sql.append('%scast(%s as %s) as %s' %(delim,val,coltype,colnm))
                delim = ','
            sql.append('from (sel 1 one) i%i' %idx)
            if (idx+1) % maxrows == 0:
                if tblcreated:
                    sql.append(';\n')
                elif not tblcreated:
                    sql.append(') with data \n  no primary index \n  on commit preserve rows;\n\n')
                    sqlclosed = True
                    tblcreated = True
                sqlprefix = 'insert into "%s" \nselect ' %tbl
            else:
                sqlprefix = 'union all \nselect '

        if not sqlclosed and not tblcreated:
            sql.append(') with data \n  no primary index \n  on commit preserve rows;\n\n')
        else:
            sql.append(';\n\n')

        self.log('sql built for', tbl)
        rtn =  '\n'.join(sql)
        rtn = rtn + '\n\n'
        return rtn






    def substitute(self, string_content='', dict_replace={}, subname='', skipkeys=[]):
        rtn = str(string_content)
        self.log('    performing substitution', subname)
        for n,v in dict_replace.items():
            if n not in skipkeys:
                if str('{%s}' %n) in rtn:
                    rtn = rtn.replace('{%s}' %n, str(v))
                    self.log('     {%s}' %n, str(v))
        return str(rtn)



    def format_sql(self, sqltext):
        sql = str(sqltext).strip().split(';')[0]
        while '\n\n' in sql:
            sql = sql.replace('\n\n','\n').strip()
        sql = sql.strip() + '\n;\n\n'

        if sql.replace(';','').strip() == '':
            sql=''
        else:
            # replce --comments with /* comments */
            newsql = []
            for line in sql.split('\n'):
                if line.strip()[:2] != '/*': #skip comment-starting  lines
                    lineparts = line.split('--')
                    if len(lineparts) !=1:
                        firstpart = lineparts[0].strip()
                        secondpart = line[len(firstpart)+2:].strip()
                        newsql.append('%s /* %s */' %(firstpart, secondpart))
                    else:
                        newsql.append(line)
                else:
                    newsql.append(line)
            sql = '\n'.join(newsql).strip()

        return sql



    def log(self, msgleft='', msgright='', header=False, error=False, warning=False):
        delim = ':'
        if  msgright=='': delim=''
        msg = '%s%s' %(str(msgleft+delim).ljust(self.logspace), msgright)

        if error: msg='%s\nERROR:\n%s\n%s' %('-='*self.logspace, msg, '-='*self.logspace)
        if warning: msg='%s\nWARNING:\n%s\n%s' %('-'*self.logspace, msg, '-'*self.logspace)
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
                # self.__writelog(msg)
                with open(self.logpath,'a') as logfile:
                    logfile.write(msg + '\n')
            else:
                if self.printlog: print(msg)
                for log in self.logs:
                    #self.__writelog(log)
                    with open(self.logpath,'a') as logfile:
                        logfile.write(log + '\n')
                    self.logs=[]
                #self.__writelog(msg)
                with open(self.logpath,'a') as logfile:
                    logfile.write(msg + '\n')


    def check_setting(self, settings_dict={}, required_item_list=[], defaults=[]):
        i=-1
        for itm in required_item_list:
            i+=1
            if str(itm) not in settings_dict:
                if defaults != [] and defaults[i]!='':
                    settings_dict[str(itm)]=defaults[i]
                    msgsuffix = 'Substituting missing value', '%s=%s' %(str(itm),settings_dict[str(itm)])
                else:
                    msgsuffix = 'No default provided, leaving as empty-string'
                    settings_dict[str(itm)]=''

                msg = '%s\n      %s\n%s\n%s\n%s' %('Required Config name/value pair MISSING:',
                        str(itm),
                        '(note: names are case-sensitive)',
                        'Some functionality may not work until this is added and you reload_config()',
                        msgsuffix)
                self.log(msg, warning=True)



    def get_special_commands(self, sql, replace_with='', keys_to_skip=[]):
        cmdstart = '/*{{'
        cmdend = '}}*/'
        cmds = {}
        sqltext = sql
        if replace_with !='': replace_with = '/* %s */' %replace_with
        self.log('  parsing for special sql commands')

        # first, get a unique dict of sql commands to iterate:
        while cmdstart in sqltext and cmdend in sqltext:
            pos1 = sqltext.find(cmdstart)
            pos2 = sqltext.find(cmdend)
            cmdstr = sqltext[pos1:pos2+len(cmdend)]
            cmdlst = cmdstr.replace(cmdstart,'').replace(cmdend,'').split(':')
            cmdkey = cmdlst[0].strip()
            if len(cmdlst) == 2:
                cmdval = cmdlst[1].strip()
            else:
                cmdval = ''

            self.log('   special command found', '%s = %s' %(cmdkey,cmdval))

            cmds[cmdkey] = {}
            cmds[cmdkey]['name'] = cmdkey
            cmds[cmdkey]['value'] = cmdval
            cmds[cmdkey]['find'] = cmdstr
            cmds[cmdkey]['replace'] = replace_with.replace('{cmdname}', cmdkey).replace('{cmdkey}', cmdkey).replace('{cmdvalue}', cmdval)
            cmds[cmdkey]['pos1'] = pos1
            cmds[cmdkey]['pos2'] = pos2
            if cmdkey in keys_to_skip:
                cmds[cmdkey]['skip'] = 'True'
            else:
                cmds[cmdkey]['skip'] = 'False'
                self.log('   %s found in keys_to_skip, skipping...' %cmdkey)

            sqltext = sqltext.replace(cmdstr,'')

        # now we have a unique list of candidates, build return object:
        finalsql = sql
        rtn = {}
        for cmd, cmdobj in cmds.items():

            if cmdobj['skip']=='False':
                rtn[cmd] = cmdobj['value']
                finalsql = finalsql.replace(cmdobj['find'],cmdobj['replace'])

        rtn['sql'] = finalsql
        return rtn


    def dict_active(self, dictTarget={}, dictName='', also_contains_key=''):
        if 'active' not in dictTarget:
            self.log('!! dictionary missing "active" flag, assuming "True"')
            dictTarget['active'] = 'True'
        if str(dictTarget['active']).lower() == 'true':
            if also_contains_key=='' or (also_contains_key != '' and also_contains_key in dictTarget):
                self.log('  active dictionary', dictName)
                return True
        self.log('  INACTIVE dictionary',dictName)
        return False



    def recursively_delete_subfolders(self, parentpath):
        self.bufferlogs=True
        self.log('purge all subfolders',parentpath)
        for itm in os.listdir(parentpath):
            if os.path.isdir(os.path.join(parentpath,itm)):
                self.recursive_delete(os.path.join(parentpath, itm))
        self.bufferlogs = False


    def recursive_delete(self, delpath):
        if os.path.isdir(delpath):
            self.log(' recursively deleting', delpath)
            shutil.rmtree(delpath)
        else:
            self.log(' path not found', delpath)



    def recursive_copy(self, sourcepath, destpath, replace_existing=False, skippattern='' ):
        self.log(' recursive_copyfolder source', sourcepath)

        if not os.path.isdir(sourcepath):
            self.log('  ERROR: source path does not exist', sourcepath)
        else:
            if not os.path.exists(destpath):
                self.log('    destination folder absent, creating', destpath)
                os.mkdir(destpath)
            for itm in os.listdir(sourcepath):
                srcpath = os.path.join(sourcepath,itm)
                dstpath = os.path.join(destpath,itm)

                if skippattern != '' and skippattern in itm:
                    self.log('    skip: matched skip-pattern', srcparh)
                else:
                    if os.path.exists(dstpath) and not replace_existing:
                        self.log('    skip: replace_existing=False', dstpath)
                    else:
                        if os.path.exists(dstpath) and replace_existing:
                            self.log(' replace_existing=True')
                            self.recursive_delete(dstpath)

                        if os.path.isfile(srcpath):
                            self.log('    file copied', dstpath)
                            shutil.copyfile(srcpath, dstpath)
                        elif os.path.isdir(os.path.join(sourcepath,itm)):
                            self.log('    folder copied', dstpath)
                            os.mkdir(dstpath)
                            self.recursive_copy(srcpath, dstpath, replace_existing, skippattern)
                        else:
                            self.log('    um... unknown filetype: %s' %srcpath)



    def close_connection(self, connobject, skip=False): # TODO
        self.log('CLOSE_CONNECTION called',  str(dt.datetime.now()))
        self.log('*** THIS FUNCTION IS NOT YET IMPLEMENTED ***')
        conntype = connobject['type']
        conn = connobject['connection']
        self.log('  connection type', conntype)

        if skip:
            self.log('skip dbs setting is true, emulating closure...')

        else:
            # ------------------------------------
            if conntype == 'teradataml':
                pass

            # ------------------------------------
            elif conntype == 'sqlalchemy':
                pass

            # ------------------------------------
            else:  # assume odbc connect
                pass

        self.log('connection closed', str(dt.datetime.now()))
        return True




    def open_connection(self, conntype, host='', logmech='', username='', password='', system={}, skip=False):
        self.log('OPEN_CONNECTION started',  str(dt.datetime.now()))
        self.log('  connection type', conntype)
        # check all variables... use system{} as default, individual variables as overrides
        host=host.strip().lower()
        logmech=logmech.strip().lower()
        conntype=conntype.strip().lower()

        if host=='': host=system['host']
        if logmech=='': logmech=system['logmech']
        if username=='': username=system['username']
        if password=='': password=system['password']

        self.log('  host', host)
        self.log('  logmech', logmech)
        self.log('  username', username)
        self.log('  password', password)

        connObject ={'type':conntype, 'skip':skip, 'connection':None, 'components':{}}
        connObject['components']={'host':host, 'logmech':logmech,
                                  'username':username, 'password':password}

        self.log('connecting...')

        if skip:
            self.log('skip dbs setting is true, emulating connection...')

        else:
            # ------------------------------------
            if conntype == 'teradataml':
                if logmech=='': logmech='TD2'
                connObject['connection'] = tdml_context.create_context(
                                            host=host,
                                            logmech=logmech,
                                            username = username,
                                            password = password)

            # ------------------------------------
            elif conntype == 'sqlalchemy':
                if logmech.strip() != '': logmech = '/?logmech=%s' %logmech
                connstring = 'teradatasql://%s:%s@%s%s' %(username,password,host,logmech)
                connObject['connection'] = sqlalchemy.create_engine(connstring)

            # ------------------------------------
            else:  # assume odbc connect
                self.log('  (odbc driver)')
                udaExec = teradata.UdaExec(appName='tdcoa',
                                           version=self.version,
                                           logConsole=False)
                connObject['connection'] = udaExec.connect(method = 'odbc',
                                           system = host,
                                           username = username,
                                           password = password,
                                           driver = conntype)

        self.log('connected!', str(dt.datetime.now()))
        return connObject




    def open_sql(self, connobject, sql, skip=False ):
        import pandas as pd
        conntype = connobject['type']
        conn = connobject['connection']

        self.log('connection type', conntype)
        self.log('sql, first 100 characters:\n  %s' %sql[:100].replace('\n',' ').strip() + '...')
        self.log('sql submitted', str(dt.datetime.now()))

        if self.show_full_sql:
            self.log('full sql:', '\n%s\n' %sql)

        if skip:
            self.log('skip dbs setting is true, emulating execution...')
            df = pd.DataFrame(columns=list('ABCD'))

        else:
            # ------------------------------------
            if conntype == 'teradataml':
                df = tdml_df.DataFrame.from_query(sql)
                df = df.to_pandas()

            # ------------------------------------
            elif conntype == 'sqlalchemy':
                df = pd.read_sql(sql, conn)

            # ------------------------------------
            else:  # assume odbc connect
                df = pd.read_sql(sql, conn)

        self.log('sql completed', str(dt.datetime.now()))
        self.log('record count', str(len(df)))
        return df



# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# -=-=  end utilities
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=





    def reload_config(self, configpath='', secretpath=''):
        """Reloads configuration YAML files (config & secrets) used as
        process driver.  This will also perform any local environment checks,
        such as creating missing folders (download|sql|run|output), change
        siteid and transcend definitions, change gitfile and host pointers,
        change runlog.txt location, etc.  This process is called once during
        class instance initialization phase (i.e., "coa = tdcoa()" )

        Parameters:
          config ==  Relative file path for the config.yaml file.
                     Default is ./config.yaml, created if missing
          secrets == Relative file path for the secrets.yaml file.
                     Default is ./secrets.yaml, created if missing

        Examples:
          from tdcsm.tdcoa import tdcoa
          coa = tdcoa()

          coa.reload_config() # reloads default config.yaml
          coa.reload_config( config='./config_customerABC.yaml')
          coa.reload_config( config='../configs/config_mthly_dbql.yaml')
          coa.reload_config( config='./myconfig.yaml', secrets='../passwords.yaml')

          # you can also specify config/secrets in class instantiation:

          from tdcsm.tdcoa import tdcoa
          coa = tdcoa( config='configABC.yaml', secrets='passwords.yaml')
        """
        # dictionaries
        self.secrets = {}
        self.filesets = {}
        self.systems = {}
        self.folders = {}
        self.substitutions = {}
        self.transcend = {}
        self.settings = {}

        if configpath=='': configpath=self.configpath
        if secretpath=='': secretpath=self.secretpath

        self.bufferlogs = True
        self.log('reload_config started', header=True)
        self.log('time',str(dt.datetime.now()))
        self.log('tdcoa version',  self.version)

        # load secrets
        self.log('loading secrets', os.path.basename(self.secretpath))
        with open(secretpath, 'r') as fh:
            secretstr = fh.read()
        self.secrets = yaml.load(secretstr)['secrets']

        # load config
        self.log('loading config', os.path.basename(self.configpath))
        with open(configpath, 'r') as fh:
            configstr = fh.read()
        configyaml = yaml.load(configstr)
        configstr = self.substitute(configstr, self.secrets, 'secrets')
        configstr = self.substitute(configstr, configyaml['substitutions'], 'config:substitutions')
        configstr = self.substitute(configstr, configyaml['folders'], 'config:folders')
        configstr = self.substitute(configstr, configyaml['settings'], 'config:settings')
        configstr = self.substitute(configstr, configyaml['transcend'], 'config:transcend')
        configyaml = yaml.load(configstr)

        # load substitutions
        self.log('loading dictionary', 'substitutions')
        self.substitutions = configyaml['substitutions']

        # check and set Transcend connection information
        self.log('loading dictionary', 'transcend')
        self.transcend = configyaml['transcend']
        self.check_setting(self.transcend,
                           required_item_list=['username','password','host','logmech','db_coa','db_region'],
                           defaults=['{td_quicklook}','{td_password}','tdprdcop3.td.teradata.com','TD2','adlste_coa','adlste_westcomm'])
        self.transcend['connectionstring'] = 'teradatasql://%s:%s@%s/?logmech=%s' %(
                                                    self.transcend['username'],
                                                    self.transcend['password'],
                                                    self.transcend['host'],
                                                    self.transcend['logmech'])

        # check and set required Folders
        self.log('loading dictionary', 'folders')
        self.folders = configyaml['folders']
        self.check_setting(self.folders, required_item_list=['download','sql','run','output','override'],
                           defaults=['1_download','2_sql_store','3_ready_to_run','4_output','0_override'])

        # check and set required Settings
        self.log('loading dictionary', 'settings')
        self.settings = configyaml['settings']
        self.check_setting(self.settings,
                           required_item_list=['githost','gitfileset','gitmotd','localfilesets','skip_git','skip_dbs'
                                              ,'run_non_fileset_folders','customer_connection_type'],
                           defaults=['https://raw.githubusercontent.com/tdcoa/sql/master/'
                                    ,'filesets.yaml'
                                    ,'motd.txt'
                                    ,'{download}/filesets.yaml'
                                    ,'False'
                                    ,'False'
                                    ,'True'
                                    ,'sqlalchemy'])
        self.filesetpath = self.settings['localfilesets']

        if 'skip_git' in self.settings and str(self.settings['skip_git']).lower() in['true']:
            self.log('setting flag skip_git active, skipping all git pulls')
            self.skipgit = True
        else:
            self.skipgit = False

        if 'skip_dbs' in self.settings and str(self.settings['skip_dbs']).lower() in['true']:
            self.log('setting flag skip_dbs active, skipping all database pulls')
            self.skipdbs = True
        else:
            self.skipdbs = False

        # create missing folders
        for nm, subfo in self.folders.items():
            fopath = os.path.join(self.approot, subfo)
            # core folders
            if not os.path.exists(fopath):
                self.log('creating missing folder', fopath)
                os.mkdir(fopath)

        # unbuffer logs once we have a valid "run" folder
        self.logpath = os.path.join(self.approot, self.folders['run'], 'runlog.txt')
        #if os.path.isfile(self.logpath): os.remove(self.logpath)
        self.bufferlogs = False
        self.log('unbuffering log to "run" folder')

        # setup filesets.yaml
        if 'localfilesets' not in self.settings:
            self.settings['localfilesets'] = os.path.join(self.folders['download'], 'filesets.yaml')
        self.filesetpath = os.path.join(self.approot, self.settings['localfilesets'])

        # create missing filesets.yaml (now that download folder exists)
        if not os.path.isfile(self.filesetpath):
            self.log('missing filesets.yaml', self.filesetpath)
            self.yaml_filesets(writefile=True)
        else:
            self.log('found filesets.yaml', self.filesetpath)

        # load filesets dictionary (active only)
        self.log('loading dictionary', 'filesets (active only)')
        with open(self.filesetpath, 'r') as fh:
            filesetstr = fh.read()
        filesetyaml = yaml.load(filesetstr)
        if filesetyaml == None:
            msg = 'filesets.yaml appears empty, please make sure it contains valid yaml configuration.\n'
            msg = msg + 'when in doubt: delete the existing filesets.yaml file from the "download" folder,\n'
            msg = msg + 'and run the process again.  When missing, it will create a default file of\n'
            msg = msg + 'the correct format.  When executing the "download_sql" command, the program\n'
            msg = msg + 'will also re-download the latest filesets.yaml from github.'
            self.log(msg, error=True)
            raise IOError(msg)
        for setname, setobject in filesetyaml.items():
            if str(setobject['active']).strip().lower() == 'true':
                self.filesets.update({setname: setobject})

        # load systems (active only)
        self.log('loading system dictionaries (active only)')
        for sysname, sysobject in configyaml['systems'].items():
            if self.dict_active(sysobject, sysname):
                self.systems.update({sysname: sysobject})

                self.check_setting(self.systems[sysname],
                           required_item_list=['active','siteid','use','host','username','password','logmech'],
                           defaults=['True', 'siteid123', 'unknown', 'customer.host.missing.com', 'username_missing', 'password_missing', ''] )

                if 'connectionstring' not in sysobject:
                    if sysobject['logmech'].strip() == '':
                        logmech = ''
                    else:
                        logmech = '/?logmech=%s' %sysobject['logmech']
                    sysobject['connectionstring'] = 'teradatasql://%s:%s@%s%s' %(sysobject['username'],
                                                                                sysobject['password'],
                                                                                sysobject['host'],
                                                                                logmech)

        self.log('done!')
        self.log('time', str(dt.datetime.now()))







    def download_files(self):
        self.log('download_files started', header=True)
        self.log('time',str(dt.datetime.now()))
        githost = self.settings['githost']
        if githost[-1:]!='/': githost = githost+'/'
        self.log('githost',githost)

        filesetcontent = ''

        # download any control files first (filesets.yaml, motd.txt, etc.)
        # motd
        giturl = githost + self.settings['gitmotd']
        self.log('downloading "motd.txt" from github')
        self.log('  requesting url', giturl)
        if self.skipgit:
            self.log('  setting: skip_git = True', 'skipping download')
            filecontent = 'default message'
        else:
            filecontent = requests.get(giturl).text
        with open(os.path.join(self.approot,'motd.txt'), 'w') as fh:
            fh.write(filecontent)

        # filesets
        giturl = githost + self.settings['gitfileset']
        self.log('downloading "filesets.yaml" from github')
        self.log('  requesting url', giturl)
        if self.skipgit:
            self.log('  setting: skip_git = True', 'skipping download')
            filecontent = self.yaml_filesets()
        else:
            filecontent = requests.get(giturl).content.decode('utf-8')
        savepath = os.path.join(self.approot,self.settings['localfilesets'])
        self.log('saving filesets.yaml', savepath)
        with open(savepath, 'w') as fh:
            fh.write(filecontent)
        filesetstr = filecontent


        # reload configs with newly downloaded filesets.yaml
        self.reload_config()

        # delete all pre-existing download folders
        self.recursively_delete_subfolders(os.path.join(self.approot, self.folders['download']))

        # set proper githost for filesets
        githost = githost + 'sets/'

        # iterate all active systems.filesets:
        for sysname, sysobject in self.systems.items():
            if self.dict_active(sysobject, sysname, also_contains_key='filesets'):
                self.log('\nINTERROGATING SYSTEM', sysname)

                # get all filesets as defined in each system:
                for sys_setname, sys_setobject in sysobject['filesets'].items():
                    if self.dict_active(sys_setobject, sys_setname):
                        self.log('  found fileset', sys_setname)
                        self.log('  cross-referencing with filesets.yaml...')

                        # cross-reference to filesets in filesets.yaml
                        if sys_setname not in self.filesets:
                            self.log(' not found in filesets.yaml', sys_setname)
                        else: # found
                            setname = sys_setname
                            setobject = self.filesets[setname]

                            if self.dict_active(setobject,  setname, also_contains_key='files'):
                                self.log('FILE SET FOUND', setname)
                                self.log('    file count', len(setobject['files']))
                                savepath = os.path.join(self.approot,self.folders['download'], setname)
                                if not os.path.exists(savepath):
                                    os.mkdir(savepath)

                                for file in setobject['files']:
                                    self.log(' downloading file', file)
                                    giturl = githost + file
                                    self.log('  %s' %giturl)
                                    if not self.skipgit:
                                        filecontent = requests.get(giturl).text
                                    else:
                                        self.log('  setting: skip_git = True', 'skipping download')
                                        self.log('  generating sql from internal default')
                                        filecontent = "select '{account}' as Account_Name, '{siteid}' as SiteID, d.* \nfrom dbc.dbcinfo as d where DATE-5 between {startdate} and {enddate};"
                                    savefile = os.path.join(savepath, file.split('/')[-1])
                                    self.log('  saving file to', savefile)
                                    with open(savefile, 'w') as fh:
                                        fh.write(filecontent)
        self.log('\ndone!')
        self.log('time', str(dt.datetime.now()))

        self.copy_download_to_sql()





    def copy_download_to_sql(self, overwrite=False):
        self.log('copy_download_to_sql started', header=True)
        self.log('copy files from download folder (by fileset) to sql folder (by system)')
        self.log('time',str(dt.datetime.now()))
        downloadpath = os.path.join(self.approot, self.folders['download'])
        sqlpath = os.path.join(self.approot, self.folders['sql'])

        self.recursively_delete_subfolders(sqlpath)

        for sysname, sysobject in self.systems.items():
            self.log('processing system', sysname)
            if self.dict_active(sysobject, also_contains_key='filesets'):
                for setname, setobject in sysobject['filesets'].items():
                    self.log('processing fileset', setname)
                    if self.dict_active(setobject):

                        #define paths:
                        srcpath = os.path.join(self.approot, self.folders['download'], setname)
                        dstpath = os.path.join(self.approot, self.folders['sql'], sysname)
                        if not os.path.exists(dstpath): os.mkdir(dstpath)
                        dstpath = os.path.join(dstpath, setname)

                        # purge existing, and copy over
                        if overwrite: self.recursive_delete(dstpath)
                        self.recursive_copy(srcpath, dstpath)

        self.log('\ndone!')
        self.log('time', str(dt.datetime.now()))




    def apply_override(self, override_folder='', target_folder=''):
        self.log('applying file override')

        # apply folder default locations
        if override_folder=='': override_folder = self.folders['override']
        override_folder = os.path.join(self.approot, override_folder)

        if target_folder=='': target_folder = self.folders['sql']
        target_folder = os.path.join(self.approot, target_folder)

        self.log(' override folder', override_folder)
        self.log(' target_folder', target_folder)

        copyops={}
        allfiles = []

        # map files found in override folder
        logdone=False
        reloadconfig=False
        for fo, subfos, files in os.walk(override_folder):
            if fo == override_folder:
                self.log('\nprocessing files found in override root')
                self.log('these files replace any matching filename, regardless of subfolder location')
                for file in files:
                    if file=='config.yaml' or file=='secrets.yaml':
                        copyops[os.path.join(self.approot,file)] = os.path.join(override_folder, file)
                        reloadconfig=True
                        self.log('  config file found, reload imminent',file)
                    elif file[:1] != '.':
                        allfiles.append(file)
                        self.log('  root file found',file)
            else:
                if os.path.basename(fo)[:1] != '.':
                    if not logdone:
                        logdone=True
                        self.log('\nprocessing files found in override subfolders')
                        self.log('these files only replace filenames found in matching subfolders (and overrides root files)')

                    for file in files:
                        if file[:1] != '.':
                            specfile = os.path.join(fo, file).replace(override_folder,'.')
                            keydestfile = os.path.join(target_folder, specfile)
                            keydestfo = os.path.dirname(keydestfile)
                            if os.path.exists(keydestfo):
                                copyops[keydestfile] = os.path.join(override_folder, specfile)
                                self.log('  subfolder file found', specfile)
                            else:
                                self.log('target folder does not exist',keydestfo,warning=True)

        # search for matching allfiles by crawling the target_folder
        for fo, subfos, files in os.walk(target_folder):
            for file in files:
                keydestfile = os.path.join(fo, file)
                if file in allfiles:
                    copyops[keydestfile] = os.path.join(override_folder, file)

        # perform final copy:
        self.log('\nperform override file copy:')
        for dstpath, srcpath in copyops.items():
            self.log(' source:  %s' %srcpath)
            self.log(' target:  %s' %dstpath)
            shutil.copyfile(srcpath, dstpath)

        if reloadconfig:  self.reload_config()
        self.log('\napply override complete!')




    def prepare_sql(self, sqlfolder='', override_folder=''):
        self.log('prepare_sql started', header=True)
        self.log('time',str(dt.datetime.now()))

        if sqlfolder != '':
            self.log('sql folder', sqlfolder)
            self.folders['sql'] = sqlfolder
        self.log(' sql folder', self.folders['sql'])
        self.log(' run folder', self.folders['run'])

        self.apply_override(target_folder = sqlfolder, override_folder=override_folder)

        # clear pre-existing subfolders in "run" directory (file sets)
        self.log('empty run folder entirely')
        self.recursively_delete_subfolders(os.path.join(self.approot, self.folders['run']))

        # iterate all system level folders in "run" folder...
        for sysfolder in os.listdir(os.path.join(self.approot, self.folders['sql'])):
            if os.path.isdir(os.path.join(self.approot, self.folders['sql'])):
                self.log('\n' + '-'*self.logspace)
                self.log('SYSTEM FOLDER FOUND', sysfolder)

                if sysfolder not in self.systems or self.dict_active(self.systems[sysfolder])==False:
                    self.log('folder not defined as an active system, skipping...')
                else:

                    # iterate all fileset subfolders in system folder...
                    for setfolder in os.listdir(os.path.join(self.approot, self.folders['sql'], sysfolder)):
                        if os.path.isdir(os.path.join(self.approot, self.folders['sql'], sysfolder, setfolder)):
                            self.log('FILESET FOLDER FOUND', setfolder)

                            # what to do with non-fileset folders?  well, depends:
                            _continue = False
                            if setfolder not in self.filesets:
                                self.log('  folder does NOT MATCH a defined fileset name', setfolder)
                                if self.settings['run_non_fileset_folders'].strip().lower() == 'true':
                                    self.log('  however setting: "run_non_fileset_folders" equals "true", continuing...')
                                    _continue = True
                                else:
                                    self.log('  and setting: "run_non_fileset_folders" not equal "true", skipping...')
                                    _continue = False
                            else: # setfolder in self.filesets
                                self.log('  folder MATCHES a defined fileset name', setfolder)
                                if self.dict_active(self.systems[sysfolder]['filesets'][setfolder])==False:
                                    self.log("  however the system's fileset-override is marked as in-active, skipping...")
                                    _continue = False
                                elif self.dict_active(self.filesets[setfolder])==False:
                                    self.log('  however fileset itself is marked as in-active, skipping...')
                                    _continue = False
                                else:
                                    self.log('  and fileset record is active, continuing...')
                                    _continue = True

                            if  _continue:

                                # define paths
                                sqlpath = os.path.join(self.approot, self.folders['sql'], sysfolder, setfolder)
                                runpath = os.path.join(self.approot, self.folders['run'], sysfolder)
                                if not os.path.isdir(runpath):
                                    self.log('  creating system folder', runpath)
                                    os.mkdir(runpath)
                                runpath = os.path.join(self.approot, self.folders['run'], sysfolder, setfolder)
                                if not os.path.isdir(runpath):
                                    self.log('  creating fileset folder', runpath)
                                    os.mkdir(runpath)

                                self.recursive_copy(sqlpath, runpath, replace_existing=True)

                                # iterate all .sql.coa files in the fileset subfolder...
                                for runfile in os.listdir(runpath):
                                    runfilepath = os.path.join(runpath, runfile)
                                    if os.path.isfile(runfilepath) and runfile[-8:]=='.coa.sql':


                                        # if .coa.sql file, read into memory
                                        self.log('\n  PROCESSING COA.SQL FILE', runfile)
                                        with open(runfilepath, 'r') as fh:
                                            runfiletext = fh.read()
                                            self.log('  characters in file', str(len(runfiletext)))


                                        # SUBSTITUTE values for:  system-fileset override
                                        if setfolder in self.systems[sysfolder]['filesets']:
                                            sub_dict = self.systems[sysfolder]['filesets'][setfolder]
                                            if self.dict_active(sub_dict, 'system-fileset overrides'):
                                                runfiletext = self.substitute(runfiletext, sub_dict, subname='system-fileset overrides (highest priority)')

                                        # SUBSTITUTE values for:  system-defaults
                                        sub_dict = self.systems[sysfolder]
                                        if self.dict_active(sub_dict, 'system defaults'):
                                            runfiletext = self.substitute(runfiletext, sub_dict, skipkeys=['filesets'], subname='system defaults')

                                        # SUBSTITUTE values for:   overall application defaults (never inactive)
                                        self.log('  always use dictionary')
                                        runfiletext = self.substitute(runfiletext, self.substitutions, subname='overall app defaults (config.substitutions)')

                                        # SUBSTITUTE values for: TRANSCEND (mostly for db_coa and db_region)
                                        runfiletext = self.substitute(runfiletext, self.transcend, subname='overall transcend database defaults (db_coa and db_region)',
                                                                      skipkeys=['host','username','password','logmech'])

                                        # SUBSTITUTE values for:   fileset defaults
                                        if setfolder in self.filesets:
                                            sub_dict = self.filesets[setfolder]
                                            if self.dict_active(sub_dict, 'fileset defaults'):
                                                runfiletext = self.substitute(runfiletext, sub_dict, skipkeys=['files'], subname='fileset defaults (lowest priority)')


                                        # split sql file into many sql statements
                                        sqls_raw = runfiletext.split(';')
                                        self.log('  sql statements in file', str(len(sqls_raw)-1))
                                        sqls_done = []
                                        i = 0


                                        # loop thru individual sql statements within file
                                        for sql_raw in sqls_raw:

                                            # light formatting...
                                            sql = self.format_sql(sql_raw)

                                            if sql != '':
                                                i+=1
                                                self.log('  SQL %i' %i, '%s...' %sql[:50].replace('\n',' '))


                                                # Get SPECIAL COMMANDS
                                                cmds = self.get_special_commands(sql, '{{replaceMe:{cmdname}}}', keys_to_skip=['save','load','call'])
                                                sql = cmds['sql'] # sql stripped of commands (now in dict)

                                                self.log('  processing special commands')
                                                for cmdname, cmdvalue in cmds.items():


                                                    # --> FILE <--: replace with local sql file
                                                    if str(cmdname[:4]).lower() == 'file':
                                                        self.log('   replace variable with a local sql file')

                                                        if not os.path.isfile(os.path.join(runpath, cmdvalue)):
                                                            self.log('custom file missing', os.path.join(runpath, cmdvalue), warning=True)
                                                            self.log('   This may be by design, consult CSM for details.')
                                                            # raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), os.path.join(runpath, cmdvalue))
                                                        else:
                                                            self.log('   specified file found', cmdvalue)
                                                            with open(os.path.join(runpath, cmdvalue), 'r') as fh:
                                                                tempsql = fh.read()
                                                            sqls_done.append('/* BEGIN file insert: %s */ \n%s' %(cmdvalue,tempsql))
                                                            sql = sql.replace('{{replaceMe:%s}}' %cmdname, 'END file insert: %s' %(cmdvalue), 1)


                                                    # --> TEMP <--: load temp file from .csv
                                                    if str(cmdname[:4]).lower() == 'temp':
                                                        self.log('   create temp (volatile) table from .csv')

                                                        if not os.path.isfile(os.path.join(runpath, cmdvalue)):
                                                            self.log('csv file missing!!!', os.path.join(runpath, cmdvalue), error=True)
                                                            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), os.path.join(runpath, cmdvalue))
                                                        else:
                                                            self.log('   csv file found', cmdvalue)
                                                            tempsql = self.sql_create_temp_from_csv(os.path.join(runpath, cmdvalue))
                                                            sqls_done.append(tempsql)
                                                            sql = sql.replace('{{replaceMe:%s}}' %cmdname,'above volatile table create script for %s' %cmdvalue, 1)


                                                    # --> LOOP <--: loop thru csv and generate one sql per csv row, with substitutions
                                                    if str(cmdname[:4]).lower() == 'loop':
                                                        self.log('   loop sql once per row in .csv, with substitutions')

                                                        # can we find the file?
                                                        if not os.path.isfile(os.path.join(runpath, cmdvalue)):
                                                            self.log('csv file missing!!!', os.path.join(runpath, cmdvalue), warning=True)
                                                        else:
                                                            self.log('   file found!')
                                                            df = pd.read_csv(os.path.join(runpath, cmdvalue))
                                                            self.log('   rows in file', str(len(df)))

                                                            # perform csv substitutions
                                                            self.log('   perform csv file substitutions (find: {column_name}, replace: row value)')
                                                            for index, row in df.iterrows():
                                                                tempsql = sql
                                                                for col in df.columns:
                                                                    tempsql = tempsql.replace(str('{%s}' %col), str(row[col]))
                                                                tempsql = tempsql.replace('{{replaceMe:%s}}' %cmdname,' csv row %i out of %i ' %(index+1, len(df)))
                                                                self.log('   sql generated from row data', 'character length = %i' %len(tempsql))
                                                                sqls_done.append(tempsql)
                                                            sql = '' # don't append original sql again - it is only a template


                                                    # --> others, append special command back to the SQL for processing in the run phase
                                                    #if str(cmdname[:4]).lower() in ['save','load','call']:
                                                    #    sql = sql.replace('/* {{replaceMe:%s}} */' %cmdname,'/*{{%s:%s}}*/' %(cmdname, cmdvalue), 1)


                                            # after all special commands, append the original sql
                                            sqls_done.append(sql)

                                        # write out new finalized file content:
                                        self.log('  writing out final sql')
                                        with open(runfilepath,'w') as fh:
                                            fh.write( '\n\n'.join(sqls_done) )

        self.log('done!')
        self.log('time', str(dt.datetime.now()))


    def archive_prepared_sql(self, name=''):
        """Manually archives (moves) all folders / files in the 'run' folder, where
        prepared sql is stored after the prepare_sql() function.  This includes the
        runlog.txt.  All  files are moved, leaving the 'run' folder empty after the
        operation. The destination folder is a new time-stamped output folder (with
        optional name).  Useful when you don't have access to execute_run() (the
        process that normally archives collateral) against customer system directly,
        but still want to keep a record of that 'run'.   For example, if you need to
        prepare sql to send to a customer DBA for execution - you cannot execute_run()
        yourself, but still want to keep a record of what was sent.

        USAGE:
        tdcoa.archive_prepared_sql(name = 'name_of_archive')
          - name: optional string to append to folder name (after timestamp)
                  all non-alphanumeric characters in name are replaced with underscore

        EXAMPLE:
        from tdcsm.tdcoa import tdcoa
        coa = tdcoa()             # instantiates objects
        coa.download_files()      # get some collateral to move
        coa.copy_download_to_sql  # move download to sql store
        coa.prepare_sql()         # prepare sql for execution

        # now we should have some files worth archiving:
        coa.archive_prepared_sql ('march run for CustABC')
        """
        self.log('archive_prepared_sql started', header=True)
        self.log('time',str(dt.datetime.now()))
        outputpath = self.make_output_folder(name)
        runpath = os.path.join(self.approot, self.folders['run'])
        self.log('created output folder', outputpath)
        self.log('moving all content from',runpath )
        self.recursive_copy(runpath, outputpath)
        self.logpath = os.path.join(outputpath, 'runlog.txt')
        self.recursive_delete(os.path.join(self.approot, self.folders['run']))
        os.mkdir(os.path.join(self.approot, self.folders['run']))
        self.log('done!')
        self.log('time', str(dt.datetime.now()))



    def make_output_folder(self, name=''):
        outputpath = os.path.join(self.approot, self.folders['output'], str(dt.datetime.now())[:-7].replace(' ','_').replace(':',''))
        if name.strip() !='': name = '-%s' %str(re.sub('[^0-9a-zA-Z]+', '_', name.strip()))
        outputpath = outputpath + name
        os.makedirs(outputpath)
        return outputpath



    def execute_run(self, name=''):
        self.log('execute_run started', header=True)
        self.log('time',str(dt.datetime.now()))

        # TODO: paramterize final database lcoation (adlste_wetcomm should be {}
        #  when building out the upload_manifest.json, so EMEA and APAC can use

        # at this point, we make the assumption that everything in the "run" directory is valid

        # make output directory for execution output and other collateral
        runpath = os.path.join(self.approot,self.folders['run'])
        outputpath = self.make_output_folder(name)

        # create hidden file containing last run's output -- to remain in the root folder
        with open(os.path.join(self.approot, '.last_run_output_path.txt'), 'w') as lastoutput:
            lastoutput.write(outputpath)
        self.log('save location of last-run output folder to hidden file')
        self.log('last-run output', outputpath)


        for sysname in os.listdir(runpath):
            sysfolder = os.path.join(runpath, sysname)
            if os.path.isdir(sysfolder):

                # iterate system folders  -- must exist in config.yaml!
                if sysname not in self.systems:
                    self.log('SYSTEM NOT FOUND IN CONFIG.YAML', sysname, warning=True)
                else:

                    # iterate file set folders -- ok to NOT exist, depending on setting
                    for setname in os.listdir(sysfolder):
                        setfolder = os.path.join(sysfolder, setname)
                        if os.path.isdir(setfolder):

                            if setname not in self.systems[sysname]['filesets'] and str(self.settings['run_non_fileset_folders']).strip().lower() != 'true':
                                self.log('-'* self.logspace)
                                self.log('WARNING!!!\nfileset does not exist', setname)
                                self.log(' AND setting "run_non_fileset_folders" is not "True"')
                                self.log(' Skipping folder', setname)
                            else:

                                self.log('SYSTEM:  %s   FILESET:  %s' %(sysname,setname), header=True)
                                workpath = setfolder
                                outputfo = os.path.join(outputpath, sysname, setname)

                                self.log('work (sql) path', workpath)
                                self.log('output path', outputfo)

                                # collect all prepared sql files, place in alpha order
                                coasqlfiles=[]
                                for coafile in os.listdir(workpath):
                                    if coafile[:1] != '.' and coafile[-8:]=='.coa.sql' :
                                        self.log('found prepared sql file', coafile )
                                        coasqlfiles.append(coafile)
                                coasqlfiles.sort()

                                if len(coasqlfiles)==0:
                                    self.log('no .coa.sql files found in\n  %s' %workpath, warning=True)
                                else:

                                    self.log('all sql files alpha-sorted for exeuction consistency')
                                    self.log('sql files found', len(coasqlfiles))

                                    # create output folder:
                                    self.log('output folder', outputfo)
                                    if not os.path.exists(outputfo):
                                        os.makedirs(outputfo)
                                    self.outputpath = outputfo

                                    # create our upload-manifest:
                                    self.log('creating upload manifest file')
                                    with open(os.path.join(outputfo,'upload-manifest.json'),'w') as manifest:
                                        manifest.write('{"entries":[ ')
                                    manifestdelim='\n '

                                    # connect to customer system:
                                    conn = self.open_connection(
                                                    conntype = self.settings['customer_connection_type'],
                                                    skip = self.skipdbs,
                                                    system = self.systems[sysname]) # <------------------------------- Connect to the database
                                    # loop thru all sql files:
                                    for coasqlfile in sorted(coasqlfiles):
                                        self.log('\nOPENING SQL FILE', coasqlfile)
                                        with open(os.path.join(workpath, coasqlfile), 'r') as coasqlfilehdlr:
                                            sqls = coasqlfilehdlr.read()

                                        sqlcnt = 0
                                        for sql in sqls.split(';'):  # loop thru the sql in the files
                                            sqlcnt +=1

                                            if sql.strip() == '':
                                                self.log('null statement, skipping')
                                            else:

                                                self.log('\n---- SQL #%i' %sqlcnt)

                                                # pull out any embedded SQLcommands:
                                                sqlcmd = self.get_special_commands(sql)
                                                sql = sqlcmd.pop('sql','')

                                                df = self.open_sql(conn, sql, skip=self.skipdbs)# <------------------------------- Run SQL

                                                if len(df) != 0:  # Save non-empty returns to .csv

                                                    if len(sqlcmd)==0:
                                                        self.log('no special commands found')

                                                    if 'save' not in sqlcmd:
                                                        sqlcmd['save'] = '%s.%s--%s' %(sysname, setname, coasqlfile) + '%04d' %sqlcnt + '.csv'

                                                    # once built, append output folder, SiteID on the front, iterative counter if duplicates
                                                    #csvfile = os.path.join(outputfo, sqlcmd['save'])
                                                    csvfile = os.path.join(workpath, sqlcmd['save'])
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

                                        # archive file we just processed (for re-run-ability)
                                        self.log('Moving coa.sql file to Output folder', coasqlfile)
                                        src = os.path.join(workpath, coasqlfile)
                                        dst = os.path.join(outputfo, coasqlfile)
                                        shutil.move(src, dst)
                                        self.log('')

                                # close JSON object
                                with open(os.path.join(outputfo,'upload-manifest.json'),'a') as manifest:
                                    manifest.write("\n  ]}")
                                    self.log('closing out upload-manifest.json')

                                # Move all files from run folder to output, for posterity:
                                self.log('moving all other run artifacts to output folder, for archiving')
                                self.recursive_copy(workpath, outputfo, replace_existing=False)
                                self.recursive_delete(workpath)


        # also COPY a few other operational files to output folder, for ease of use:
        self.log('-'*self.logspace)
        self.log('post-processing')
        for srcpath in [os.path.join(self.approot, '.last_run_output_path.txt'),
                     self.configpath, self.filesetpath]:
            self.log('copy to output folder root, for ease of use: \n  %s' %srcpath)
            dstpath = os.path.join(outputpath, os.path.basename(srcpath))
            shutil.copyfile(srcpath, dstpath)

        self.log('\ndone!')
        self.log('time', str(dt.datetime.now()))

        # after logging is done, move the log file too...
        runlogsrc = os.path.join(self.approot, self.folders['run'], 'runlog.txt')
        runlogdst = os.path.join(outputpath, 'runlog.txt')
        shutil.move(runlogsrc, runlogdst)




    def upload_to_transcend(self, _outputpath=''):
        self.bufferlogs = True
        self.log('upload_to_transcend started', header=True)
        self.log('time',str(dt.datetime.now()))

        # process 3 ways to get output path
        if _outputpath !='':   # supplied parameter
            outputpath = _outputpath
            self.log('output folder = manual param', outputpath)
        elif os.path.isfile(os.path.join(self.approot, '.last_run_output_path.txt')):
            # .last_run_output_path.txt  in approot
            with open(os.path.join(self.approot, '.last_run_output_path.txt'),'r') as fh:
                outputpath = fh.read().strip().split('\n')[0]
            self.log('output folder= .last_run_output_path.txt', outputpath)
        elif self.outputpath !='':
            # local variable set
            outputpath = self.outputpath
            self.log('output folder = class variable: coa.outputpath', outputpath)
        else:
            outputpath =''
            self.log('no output path defined')


        # now that outputfo is defined, let's make sure the dir actually exists:
        if not os.path.isdir(outputpath):
            self.log('\nERROR = invalid path', outputpath)
            raise NotADirectoryError('Invalid Path: %s' %outputpath)
            exit()
        else:
            self.outputpath = outputpath

        # update log file to correct location
        self.log('updating runlog.txt location')
        self.logpath = os.path.join(outputpath, 'runlog.txt')
        self.bufferlogs = False
        self.log('unbuffer logs')


        # connect to Transcend using TeradataML lib, for fast bulk uploads
        self.log('connecting to transcend')
        self.log('    host', self.transcend['host'])
        self.log('    logmech', self.transcend['logmech'])
        self.log('    username', self.transcend['username'])
        self.log('    password', self.transcend['password'])
        self.log('    db_coa', self.transcend['db_coa'])
        self.log("\nNOTE:  if you happen to see a scary WARNING below, DON'T PANIC!")
        self.log("       it just means you already had an active connection that was replaced.\n")

        transcend = self.open_connection(
                        'teradataml',
                        system = self.transcend,
                        skip = self.skipdbs)   # <--------------------------------- Connect

        # Walk the directory structure looking for upload_manifest.json files
        for workpath, subfo, files in os.walk(outputpath):
            self.log('\nexamining folder', str(workpath).strip())
            workname = os.path.split(workpath)[1]
            if str(workname)[:1] != '.': # no hidden folders
                if 'upload-manifest.json' in files:

                    self.log('FOUND upload-manifest.json')
                    with open(os.path.join(workpath, 'upload-manifest.json'),'r') as fh:
                        manifestjson = fh.read()
                    manifest = json.loads(manifestjson)
                    self.log('upload count found', str(len(manifest['entries'])) )
                    self.log('manifest file','\n%s' %str(manifest))

                    if len(manifest['entries']) == 0:
                        self.log('nothing to upload, skipping', workpath)
                    else:

                        for entry in manifest['entries']:

                            # define database and table names
                            if '.' in entry['table']:
                                entry['schema'] = entry['table'].split('.')[0]
                                entry['table'] = entry['table'].split('.')[1]
                            else:
                                entry['schema'] = 'adlste_coa'

                            self.log('\nPROCESSING NEW ENTRY')
                            self.log('  load file', entry['file'])
                            self.log('  into table', entry['table'])
                            self.log('  of schema', entry['schema'])
                            self.log('  then call', entry['call'])
                            self.log('-'*10)

                            # open CSV and prepare for appending
                            csvfilepath = os.path.join(workpath, entry['file'])
                            self.log('opening csv', csvfilepath)
                            dfcsv = pd.read_csv(csvfilepath)
                            dfcsv = dfcsv.where(pd.notnull(dfcsv), None)
                            self.log('records found', str(len(dfcsv)))

                            # strip out any unnamed columns
                            for col in dfcsv.columns:
                                if col[:8] == 'Unnamed:':
                                    self.log('unnamed column dropped', col)
                                    self.log('  (usually the pandas index as a column, "Unnamed: 0")')
                                    dfcsv = dfcsv.drop(columns=[col])
                            self.log('final column count', str(len(dfcsv.columns)))

                            # APPEND data to database, via teradataml bulk upload:
                            self.log('uploading', str(dt.datetime.now()))
                            if self.skipdbs:
                                self.log('skipdbs = True', 'emulating upload')
                            else:
                                try:
                                    copy_to_sql(dfcsv, entry['table'], entry['schema'], if_exists = 'append')
                                except Exception as err:
                                    self.log('\n\nERROR during UPLOAD', error=True)
                                    self.log(str(err))
                                    self.log('   (error repeated below)')
                                    self.log('\n    first 10 records of what was being uploaded (dataframe):')
                                    self.log(dfcsv[0:10])
                                    self.log('')
                                    sql =["Select ColumnName, ColumnType, ColumnFormat, ColumnLength, ColumnId"]
                                    sql.append("from dbc.columns ")
                                    sql.append("where databasename = '%s' " %entry['schema'])
                                    sql.append("  and tablename = '%s' " %entry['table'])
                                    #sql.append("order by ColumnID;")
                                    sql = '\n'.join(sql)
                                    self.log(sql)
                                    df = DataFrame.from_query(sql)
                                    df = df.sort(['ColumnId'], ascending=[True])
                                    self.log('\n\n    structure of destination table:')
                                    print(df)
                                    self.log('')
                                    raise err


                            self.log('complete', str(dt.datetime.now()))

                            # CALL any specified SPs:
                            if str(entry['call']).strip() != "":
                                self.log('Stored Proc', str(dt.datetime.now()) )
                                if self.skipdbs:
                                    self.log('skipdbs = True', 'emulating call')
                                else:
                                    transcend['connection'].execute('call %s ;' %str(entry['call']) )
                                self.log('complete', str(dt.datetime.now()))

        self.log('\ndone!')
        self.log('time', str(dt.datetime.now()))
