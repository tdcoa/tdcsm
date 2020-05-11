import datetime as dt
import os
import shutil

import pandas as pd
import sqlalchemy
# --  Teradata Drivers:
import teradata  # odbc driver
from teradataml.context import context as tdml_context
from teradataml.dataframe import dataframe as tdml_df


class Utils:

    def __init__(self, logger, version):
        self.logger = logger  # logger object
        self.version = version

    # todo remove and replace with default file?
    def yaml_filesets(self, filesetpath, writefile=False):
        self.logger.log('generating filesets.yaml from internal default')
        cy = []
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
            with open(os.path.join(filesetpath), 'w') as fh:
                fh.write(rtn)
            self.logger.log('  saving file', filesetpath)
        return rtn

    def sql_create_temp_from_csv(self, csvfilepath, rowsperchunk=100):
        tbl = os.path.basename(csvfilepath)
        self.logger.log('    transcribing sql', tbl)

        # open csv
        self.logger.log('    open csv', tbl)
        dfcsv = pd.read_csv(csvfilepath)
        rowcount = len(dfcsv)
        self.logger.log('    rows in file', str(rowcount))

        coldefs = {}
        chunknumber = 1
        rowstart = 0
        rowend = rowsperchunk
        self.logger.log('    rows per chunk', str(rowsperchunk))

        # iterrate in chunks of records, as defined above
        sql = []
        while rowstart <= rowcount - 1:
            df = dfcsv[rowstart:rowend]
            rowend = rowstart + len(df)
            delim = '('
            self.logger.log('building chunk %i containing rows %i thru %i' % (chunknumber, rowstart, rowend))

            # use first chunk to define data types and CREATE TABLE
            if chunknumber == 1:
                sql.append('CREATE MULTISET VOLATILE TABLE "%s"' % tbl)

                for colname, colpytype in dfcsv.dtypes.items():
                    coltype = ''
                    if str(colpytype) == 'object':
                        collen = dfcsv[colname].map(str).map(len).max() + 100
                        coltype = 'VARCHAR(%i)  CHARACTER SET UNICODE ' % collen
                        coldefs[colname] = {'type': 'varchar(%i)' % collen, 'len': collen, 'quote': "'", 'null': "''"}

                    if str(colpytype)[:3] == 'int':
                        coltype = 'BIGINT'
                        coldefs[colname] = {'type': 'bigint', 'len': 0, 'quote': '', 'null': 'NULL'}

                    tmp = '"%s"' % str(colname)
                    sql.append('%s%s%s' % (delim, tmp.ljust(30), coltype))
                    delim = ','

                sql.append(') NO PRIMARY INDEX\nON COMMIT PRESERVE ROWS;\n')

            # for each chunk now build INSERT STATEMENT
            sql.append('INSERT INTO "%s"' % tbl)
            for idx, row in df.iterrows():
                sql.append('SELECT')
                delim = '  '

                for col, val in row.items():
                    quote = coldefs[col]['quote']
                    if pd.isna(val):
                        val = coldefs[col]['null']
                        quote = ''
                    sql.append('%scast(%s%s%s as %s)' % (delim, quote, val, quote, coldefs[col]['type']))
                    delim = ' ,'

                union = 'UNION ALL'
                if idx == rowend - 1:
                    union = ';\n'
                sql.append('from (sel 1 one) i%i    %s' % (idx, union))

            chunknumber += 1
            rowstart = (chunknumber - 1) * rowsperchunk
            rowend = chunknumber * rowsperchunk

        self.logger.log('sql built for', tbl)
        return '\n'.join(sql)

    def substitute(self, string_content='', dict_replace=None, subname='', skipkeys=None):
        if dict_replace is None:
            dict_replace = {}
        if skipkeys is None:
            skipkeys = []

        rtn = str(string_content)
        self.logger.log('    performing substitution', subname)
        for n, v in dict_replace.items():
            if n not in skipkeys:
                if str('{%s}' % n) in rtn:
                    rtn = rtn.replace('{%s}' % n, str(v))
                    self.logger.log('     {%s}' % n, str(v))
        return str(rtn)

    @staticmethod
    def format_sql(sqltext):
        sql = str(sqltext).strip().split(';')[0]
        while '\n\n' in sql:
            sql = sql.replace('\n\n', '\n').strip()

        sql = sql.strip() + '\n;\n\n'

        if sql.replace(';', '').strip() == '':
            sql = ''

        else:
            # replace --comments with /* comments */
            newsql = []
            for line in sql.split('\n'):
                if line.strip()[:2] != '/*':  # skip comment-starting  lines
                    lineparts = line.split('--')
                    if len(lineparts) != 1:
                        firstpart = lineparts[0].strip()
                        secondpart = line[len(firstpart) + 2:].strip()
                        newsql.append('%s /* %s */' % (firstpart, secondpart))
                    else:
                        newsql.append(line)
                else:
                    newsql.append(line)
            sql = '\n'.join(newsql).strip()

        return sql

    def check_setting(self, settings_dict=None, required_item_list=None, defaults=None):
        if settings_dict is None:
            settings_dict = {}
        if required_item_list is None:
            required_item_list = []
        if defaults is None:
            defaults = []
        i = -1
        for itm in required_item_list:
            i += 1
            if str(itm) not in settings_dict:
                if defaults != [] and defaults[i] != '':
                    settings_dict[str(itm)] = defaults[i]
                    msgsuffix = 'Substituting missing value', '%s=%s' % (str(itm), settings_dict[str(itm)])
                else:
                    msgsuffix = 'No default provided, leaving as empty-string'
                    settings_dict[str(itm)] = ''

                msg = '%s\n      %s\n%s\n%s\n%s' % ('Required Config name/value pair MISSING:',
                                                    str(itm),
                                                    '(note: names are case-sensitive)',
                                                    'Some functionality may not work until this is added and you reload_config()',
                                                    msgsuffix)
                self.logger.log(msg, warning=True)

    def get_special_commands(self, sql, replace_with='', keys_to_skip=None):
        if keys_to_skip is None:
            keys_to_skip = []

        cmdstart = '/*{{'
        cmdend = '}}*/'
        cmds = {}
        sqltext = sql
        replace_with = '/* %s */' % replace_with if replace_with != '' else replace_with

        self.logger.log('  parsing for special sql commands')

        # first, get a unique dict of sql commands to iterate:
        while cmdstart in sqltext and cmdend in sqltext:
            pos1 = sqltext.find(cmdstart)
            pos2 = sqltext.find(cmdend)
            cmdstr = sqltext[pos1:pos2 + len(cmdend)]
            cmdlst = cmdstr.replace(cmdstart, '').replace(cmdend, '').split(':')
            cmdkey = cmdlst[0].strip()
            if len(cmdlst) == 2:
                cmdval = cmdlst[1].strip()
            else:
                cmdval = ''

            self.logger.log('   special command found', '%s = %s' % (cmdkey, cmdval))

            cmds[cmdkey] = {}
            cmds[cmdkey]['name'] = cmdkey
            cmds[cmdkey]['value'] = cmdval
            cmds[cmdkey]['find'] = cmdstr
            cmds[cmdkey]['replace'] = replace_with.replace('{cmdname}', cmdkey).replace('{cmdkey}', cmdkey).replace(
                '{cmdvalue}', cmdval)
            cmds[cmdkey]['pos1'] = pos1
            cmds[cmdkey]['pos2'] = pos2

            if cmdkey in keys_to_skip:
                cmds[cmdkey]['skip'] = True

            else:
                cmds[cmdkey]['skip'] = False
                self.logger.log('   %s found in keys_to_skip, skipping...' % cmdkey)

            sqltext = sqltext.replace(cmdstr, '')

        # now we have a unique list of candidates, build return object:
        finalsql = sql
        rtn = {}
        for cmd, cmdobj in cmds.items():

            # add non-skipped special cmds
            if not cmdobj['skip']:
                rtn[cmd] = cmdobj['value']
                finalsql = finalsql.replace(cmdobj['find'], cmdobj['replace'])

        rtn['sql'] = finalsql

        return rtn

    def dict_active(self, dictTarget=None, dictName='', also_contains_key=''):
        if dictTarget is None:
            dictTarget = {}

        if 'active' not in dictTarget:
            self.logger.log('!! dictionary missing "active" flag, assuming "True"')
            dictTarget['active'] = 'True'

        if str(dictTarget['active']).lower() == 'true':
            if also_contains_key == '' or (also_contains_key != '' and also_contains_key in dictTarget):
                self.logger.log('  active dictionary', dictName)
                return True

        self.logger.log('  INACTIVE dictionary', dictName)

        return False

    def recursively_delete_subfolders(self, parentpath):
        self.logger.bufferlogs = True
        self.logger.log('purge all subfolders', parentpath)
        for itm in os.listdir(parentpath):
            if os.path.isdir(os.path.join(parentpath, itm)):
                self.recursive_delete(os.path.join(parentpath, itm))
        self.logger.bufferlogs = False

    def recursive_delete(self, delpath):
        if os.path.isdir(delpath):
            self.logger.log(' recursively deleting', delpath)
            shutil.rmtree(delpath)
        else:
            self.logger.log(' path not found', delpath)

    def recursive_copy(self, sourcepath, destpath, replace_existing=False, skippattern=''):
        self.logger.log(' recursive_copyfolder source', sourcepath)

        if not os.path.isdir(sourcepath):
            self.logger.log('  ERROR: source path does not exist', sourcepath)
        else:
            if not os.path.exists(destpath):
                self.logger.log('    destination folder absent, creating', destpath)
                os.mkdir(destpath)
            for itm in os.listdir(sourcepath):
                srcpath = os.path.join(sourcepath, itm)
                dstpath = os.path.join(destpath, itm)

                if skippattern != '' and skippattern in itm:
                    self.logger.log('    skip: matched skip-pattern', srcpath)
                else:
                    if os.path.exists(dstpath) and not replace_existing:
                        self.logger.log('    skip: replace_existing=False', dstpath)
                    else:
                        if os.path.exists(dstpath) and replace_existing:
                            self.logger.log(' replace_existing=True')
                            self.recursive_delete(dstpath)

                        if os.path.isfile(srcpath):
                            self.logger.log('    file copied', dstpath)
                            shutil.copyfile(srcpath, dstpath)
                        elif os.path.isdir(os.path.join(sourcepath, itm)):
                            self.logger.log('    folder copied', dstpath)
                            os.mkdir(dstpath)
                            self.recursive_copy(srcpath, dstpath, replace_existing, skippattern)
                        else:
                            self.logger.log('    um... unknown filetype: %s' % srcpath)

    def close_connection(self, connobject, skip=False):  # TODO
        self.logger.log('CLOSE_CONNECTION called', str(dt.datetime.now()))
        self.logger.log('*** THIS FUNCTION IS NOT YET IMPLEMENTED ***')
        conntype = connobject['type']
        conn = connobject['connection']
        self.logger.log('  connection type', conntype)

        if skip:
            self.logger.log('skip dbs setting is true, emulating closure...')

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

        self.logger.log('connection closed', str(dt.datetime.now()))
        return True

    def open_connection(self, conntype, host='', logmech='', username='', password='', system=None, skip=False):
        if system is None:
            system = {}
        self.logger.log('OPEN_CONNECTION started', str(dt.datetime.now()))
        self.logger.log('  connection type', conntype)
        # check all variables... use system{} as default, individual variables as overrides
        host = host.strip().lower()
        logmech = logmech.strip().lower()
        conntype = conntype.strip().lower()

        host = system['host'] if host == '' else host
        logmech = system['logmech'] if logmech == '' else logmech
        username = system['username'] if username == '' else username
        password = system['password'] if password == '' else password

        self.logger.log('  host', host)
        self.logger.log('  logmech', logmech)
        self.logger.log('  username', username)
        self.logger.log('  password', password)

        connObject = {
            'type': conntype,
            'skip': skip,
            'connection': None,
            'components': {
                'host': host,
                'logmech': logmech,
                'username': username,
                'password': password
            }
        }

        self.logger.log('connecting...')

        if skip:
            self.logger.log('skip dbs setting is true, emulating connection...')

        else:
            # ------------------------------------
            if conntype == 'teradataml':
                logmech = 'TD2' if logmech == '' else logmech

                connObject['connection'] = tdml_context.create_context(
                    host=host,
                    logmech=logmech,
                    username=username,
                    password=password
                )

            # ------------------------------------
            elif conntype == 'sqlalchemy':
                if logmech.strip() != '':
                    logmech = '/?logmech=%s' % logmech

                connstring = 'teradatasql://%s:%s@%s%s' % (username, password, host, logmech)
                connObject['connection'] = sqlalchemy.create_engine(connstring)

            # ------------------------------------
            else:  # assume odbc connect
                self.logger.log('  (odbc driver)')
                udaExec = teradata.UdaExec(appName='tdcoa',
                                           version=self.version,
                                           logConsole=False)
                connObject['connection'] = udaExec.connect(method='odbc',
                                                           system=host,
                                                           username=username,
                                                           password=password,
                                                           driver=conntype)

        self.logger.log('connected!', str(dt.datetime.now()))
        return connObject

    def open_sql(self, connobject, sql, skip=False):
        conntype = connobject['type']
        conn = connobject['connection']

        self.logger.log('connection type', conntype)
        self.logger.log('sql, first 100 characters:\n  %s' % sql[:100].replace('\n', ' ').strip() + '...')
        self.logger.log('sql submitted', str(dt.datetime.now()))

        if self.logger.show_full_sql:
            self.logger.log('full sql:', '\n%s\n' % sql)

        if skip:
            self.logger.log('skip dbs setting is true, emulating execution...')
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

        self.logger.log('sql completed', str(dt.datetime.now()))
        self.logger.log('record count', str(len(df)))
        return df