import datetime as dt
import os
import shutil
import numpy

import pandas as pd
import sqlalchemy
# --  Teradata Drivers:
import teradata  # odbc driver
from teradataml.context import context as tdml_context
from teradataml.dataframe import dataframe as tdml_df
from tdcsm.logging import Logger
from pptx import Presentation
from pptx.util import Inches, Pt
import textwrap


class Utils(Logger):

    def __init__(self, version):
        super().__init__()  # inherits Logger class
        self.version = version

    def sql_create_temp_from_csv(self, csvfilepath, rowsperchunk=100):
        tbl = os.path.basename(csvfilepath)
        self.log('    transcribing sql', tbl)

        # open csv
        self.log('    open csv', tbl)
        dfcsv = pd.read_csv(csvfilepath)
        rowcount = len(dfcsv)
        self.log('    rows in file', str(rowcount))

        coldefs = {}
        chunknumber = 1
        rowstart = 0
        rowend = rowsperchunk
        self.log('    rows per chunk', str(rowsperchunk))

        # iterrate in chunks of records, as defined above
        sql = []
        while rowstart <= rowcount - 1:
            df = dfcsv[rowstart:rowend]
            rowend = rowstart + len(df)
            delim = '('
            self.log('building chunk %i containing rows %i thru %i' % (chunknumber, rowstart, rowend))

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

        self.log('sql built for', tbl)
        return '\n'.join(sql)

    def substitute(self, string_content='', dict_replace=None, subname='', skipkeys=None):
        if dict_replace is None:
            dict_replace = {}
        if skipkeys is None:
            skipkeys = []

        rtn = str(string_content)
        self.log('    performing substitution', subname)
        for n, v in dict_replace.items():
            if n not in skipkeys:
                if str('{%s}' % n) in rtn:
                    rtn = rtn.replace('{%s}' % n, str(v))
                    self.log('     {%s}' % n, str(v))
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

    def check_setting(self, settings_dict=None, required_item_list=None, defaults=None, printwarning=True):
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
                if printwarning: self.log(msg, warning=True)

    def get_special_commands(self, sql, replace_with='', keys_to_skip=None):
        if keys_to_skip is None:
            keys_to_skip = []

        cmdstart = '/*{{'
        cmdend = '}}*/'
        cmds = {}
        sqltext = sql
        replace_with = '/* %s */' % replace_with if replace_with != '' else replace_with

        self.log('  parsing for special sql commands')

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

            self.log('   special command found', '%s = %s' % (cmdkey, cmdval))

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
                self.log('   %s found in keys_to_skip, skipping...' % cmdkey)

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
            self.log('!! dictionary missing "active" flag, assuming "True"')
            dictTarget['active'] = 'True'

        if str(dictTarget['active']).lower() == 'true':
            if also_contains_key == '' or (also_contains_key != '' and also_contains_key in dictTarget):
                self.log('  active dictionary', dictName)
                return True

        self.log('  INACTIVE dictionary', dictName)

        return False

    def recursively_delete_subfolders(self, parentpath):
        self.bufferlogs = True
        self.log('purge all subfolders', parentpath)
        for itm in os.listdir(parentpath):
            if os.path.isdir(os.path.join(parentpath, itm)):
                self.recursive_delete(os.path.join(parentpath, itm))
        self.bufferlogs = False

    def recursive_delete(self, delpath):
        if os.path.isdir(delpath):
            self.log(' recursively deleting', delpath)
            shutil.rmtree(delpath,ignore_errors=True) # Set ignore error as true to avoid failure because of read only files not being deleted.
        else:
            self.log(' path not found', delpath)

    def recursive_copy(self, sourcepath, destpath, replace_existing=False, skippattern=''):
        self.log(' recursive_copyfolder source', sourcepath)

        if not os.path.isdir(sourcepath):
            self.log('  ERROR: source path does not exist', sourcepath)
        else:
            if not os.path.exists(destpath):
                self.log('    destination folder absent, creating', destpath)
                os.mkdir(destpath)
            for itm in os.listdir(sourcepath):
                srcpath = os.path.join(sourcepath, itm)
                dstpath = os.path.join(destpath, itm)

                if skippattern != '' and skippattern in itm:
                    self.log('    skip: matched skip-pattern', srcpath)
                else:
                    if os.path.exists(dstpath) and not replace_existing:
                        self.log('    skip: replace_existing=False', dstpath)
                    else:
                        if os.path.exists(dstpath) and replace_existing:
                            self.log(' replace_existing=True')
                            ## uncomment below line --> Kailash
                            self.recursive_delete(dstpath)

                        if os.path.isfile(srcpath):
                            self.log('    file copied', dstpath)
                            shutil.copyfile(srcpath, dstpath)
                        elif os.path.isdir(os.path.join(sourcepath, itm)):
                            self.log('    folder copied', dstpath)
                            os.mkdir(dstpath)
                            self.recursive_copy(srcpath, dstpath, replace_existing, skippattern)
                        else:
                            self.log('    um... unknown filetype: %s' % srcpath)

    def close_connection(self, connobject, skip=False):  # TODO
        self.log('CLOSE_CONNECTION called', str(dt.datetime.now()))
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

    def open_connection(self, conntype, host='', logmech='', encryption='', username='', password='', system=None, skip=False):
        if system is None:
            system = {}
        self.log('OPEN_CONNECTION started', str(dt.datetime.now()))
        self.log('  connection type', conntype)
        # check all variables... use system{} as default, individual variables as overrides
        host = host.strip().lower()
        logmech = logmech.strip().lower()
        encryption = encryption.strip().lower()
        conntype = conntype.strip()
        if conntype.lower() in ('sqlalchemy', 'teradataml'):
            conntype = conntype.lower().strip()

        host = system['host'] if host == '' else host
        logmech = system['logmech'] if logmech == '' else logmech
        username = system['username'] if username == '' else username
        password = system['password'] if password == '' else password

        self.log('  host', host)
        self.log('  logmech', logmech)
        self.log('  username', username)
        self.log('  password', password)

        connObject = {
            'type': conntype,
            'skip': skip,
            'connection': None,
            'components': {
                'host': host,
                'logmech': logmech,
                'username': username,
                'password': password,
                'encryption': encryption
            }
        }

        self.log('connecting...')

        if skip:
            self.log('skip dbs setting is true, emulating connection...')

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

                # todo investigate why this fails when 'false'
                if encryption.strip().lower() == 'true':
                    if logmech.strip() == '':
                        encryption = '/?encryption=%s' % encryption
                    else:
                        encryption = '?encryption=%s' % encryption

                    connstring = 'teradatasql://%s:%s@%s%s%s' % (username, password, host, logmech, encryption)

                else:
                    connstring = 'teradatasql://%s:%s@%s%s' % (username, password, host, logmech)

                connObject['connection'] = sqlalchemy.create_engine(connstring)

            # ------------------------------------
            else:  # assume odbc connect
                self.log('  (odbc driver)')
                udaExec = teradata.UdaExec(appName='tdcoa',
                                           version=self.version,
                                           logConsole=False)
                connObject['connection'] = udaExec.connect(method='odbc',
                                                           system=host,
                                                           username=username,
                                                           password=password,
                                                           driver=conntype,
                                                           authentication=logmech,
                                                           encryptdata=encryption,
                                                           column_name='true')

        self.log('connected!', str(dt.datetime.now()))
        return connObject

    def open_sql(self, connobject, sql, skip=False, columns=False):
        conntype = connobject['type']
        conn = connobject['connection']

        self.log('connection type', conntype)
        self.log('sql, first 100 characters:\n  %s' % sql[:100].replace('\n', ' ').strip() + '...')
        self.log('sql submitted', str(dt.datetime.now()))

        if self.show_full_sql:
            self.log('full sql:', '\n%s\n' % sql)

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

                if not columns:
                    try:
                        df = pd.DataFrame(conn.execute(sql))
                    except Exception as e:
                        df = []

                # get column names
                # df.read_sql does not work properly for odbc connections.
                # Can directly execute using odbc connection but then there are no column names
                # This code block retrieves the column names before saving the csv file
                # Col names will be merged upon save
                else:
                    try:
                        df = []
                        for row in conn.execute(sql).description:
                            df.append(row[0])
                    except Exception as e:
                        df = []

        self.log('sql completed', str(dt.datetime.now()))
        self.log('record count', str(len(df)))
        return df

    @staticmethod
    def get_cell_value_from_table(cell_content):
        cell_value = ''

        if '{{col:' in cell_content:
            pass
        elif '{{val:' in cell_content:
            pass
        elif '{{pic:' in cell_content:
            pass
        else:
            pass

    @staticmethod
    def pad(l, content, width):
        l.extend([content] * (width - len(l)))
        return l

    @staticmethod
    def insert_to_pptx(pptx_path, workpath):
        prs = Presentation(pptx_path)

        for slide in prs.slides:  # loop through all slides
            for shape in slide.shapes:  # loop through all shape objects in a slide

                # shape_type 19 = table which does not have text field
                # if shape.shape_type not in (13, 19) and '{{' in shape.text and '}}' in shape.text:  # search for special command
                if shape.shape_type in (
                1, 14) and '{{' in shape.text and '}}' in shape.text:  # search for special command
                    # insert image
                    if '.png' in shape.text:
                        # img_name = shape.text.replace('{{pic:', '').replace('}}', '')  # get img file name to be inserted
                        # slide.shapes.add_picture(os.path.join(workpath, img_name),
                        #                          left=shape.left,
                        #                          top=shape.top,
                        #                          height=shape.height,
                        #                          width=shape.width)  # insert with same dimensions as placeholder
                        #
                        # # remove placeholder shape
                        # shape_to_remove = shape._element
                        # shape_to_remove.getparent().remove(shape_to_remove)

                        try:
                            img_name = shape.text.replace('{{pic:', '').replace('}}', '').replace('**',
                                                                                                  '')  # get img file name to be inserted
                            slide.shapes.add_picture(os.path.join(workpath, img_name),
                                                     left=shape.left,
                                                     top=shape.top,
                                                     height=shape.height,
                                                     width=shape.width)  # insert with same dimensions as placeholder

                            # remove placeholder shape
                            shape_to_remove = shape._element
                            shape_to_remove.getparent().remove(shape_to_remove)

                        except OSError as e:
                            text_ori = shape.text.replace('**', '')
                            text_frame = shape.text_frame
                            text_frame.clear()  # not necessary for newly-created shape

                            p = text_frame.paragraphs[0]
                            run = p.add_run()
                            run.text = text_ori + '**'

                            font = run.font
                            #                         font.name = 'Calibri'
                            #                         font.size = Pt(18)
                            #                         font.bold = True
                            #                         font.color.rgb = RGBColor(0xFF, 0x7F, 0x50)

                            print("File Not Found!!")
                    else:
                        # csv_name = shape.text[shape.text.find("{{")+2 : shape.text.find("}}")]
                        file_not_found = False
                        index = 0
                        csv_name_list = []
                        text = shape.text

                        while index < len(text):
                            index = text.find('{{val:', index)
                            if index == -1:
                                break
                            print('{{ found at', index)

                            index += 6  # +2 because len('ll') == 2
                            start_index = index

                            index = text.find('}}', index)
                            if index == -1:
                                break
                            print('}} found at', index)

                            end_index = index
                            index += 2  # +2 because len('ll') == 2

                            csv_name = text[start_index:end_index]
                            csv_name_list.append(csv_name)

                        csv_name_value = []

                        for csv_name in csv_name_list:
                            df_name_cell = csv_name.split('.csv')
                            df_name = df_name_cell[0]
                            df_cell = df_name_cell[1]
                            # df_csv = pd.read_csv(os.path.join(workpath, df_name + '.csv'))
                            # # df_value = df_csv[df_cell[1]:df_cell[3]]
                            # df_value = df_csv.iloc[int(df_cell[1]) - 2, int(df_cell[3]) - 1]
                            # csv_name_value.append(df_value)

                            try:
                                df_csv = pd.read_csv(os.path.join(workpath, df_name + '.csv'))
                                # df_value = df_csv[df_cell[1]:df_cell[3]]
                                df_value = df_csv.iloc[int(df_cell[1]) - 2, int(df_cell[3]) - 1]
                                csv_name_value.append(df_value)
                            except OSError as e:

                                csv_name_value.append(csv_name)
                                file_not_found = True

                        text_2 = ''
                        i = 0
                        index = 0
                        text_start = 0

                        while index < len(text):
                            index = text.find('{{val:', index)
                            if index == -1:
                                break
                            print('{{ found at', index)

                            index += 6  # +2 because len('ll') == 2
                            start_index = index

                            index = text.find('}}', index)
                            if index == -1:
                                break
                            print('}} found at', index)

                            end_index = index
                            index += 2  # +2 because len('ll') == 2
                            #     text_start = index

                            print('text_2:', text_2)
                            print('text:', text)
                            print('text_start:', text_start)
                            print('start_index:', start_index)
                            print('text[text_start:start_index - 6]:', text[text_start:start_index - 6])
                            print('csv_name_list[i]:', csv_name_list[i])

                            try:
                                text_2 = text_2 + text[text_start:start_index - 6] + csv_name_value[i]
                            except:
                                # text_2 = text_2 + text[text_start:start_index - 6] + csv_name_value[i].astype(numpy.str)
                                try:
                                    text_2 = text_2 + text[text_start:start_index - 6] + csv_name_value[i].astype(
                                        numpy.str)
                                except:
                                    file_not_found = True
                            i += 1

                            text_start = index

                        text_2 = text_2 + text[end_index + 2:]

                        # #     csv_name = text[start_index:end_index]
                        # #     csv_name_list.append(csv_name)
                        #
                        # # new_text = shape.text[0:shape.text.find("{{")] + df_value + " " + shape.text[shape.text.find("}}") + 2 : ]
                        # shape.text = text_2

                        if file_not_found == True:
                            shape.text = text_ori.replace('**', '') + "**"
                        else:
                            shape.text = text_2.replace('**', '')

                elif shape.shape_type == 19:
                    num_of_columns_in_ppt = len(shape.table.columns)
                    num_of_rows_in_ppt = len(shape.table.rows)

                    df_name = ''

                    index = 0

                    print('\n')

                    print('New Table: ')
                    print('total number of cols: ', num_of_columns_in_ppt)
                    print('total number of rows: ', num_of_rows_in_ppt)

                    i_col_counter_in_ppt = 0
                    is_entire_column = False

                    df_length = -1

                    column_name_list = []
                    columns_list = []

                    skip_column_list = []

                    while i_col_counter_in_ppt < num_of_columns_in_ppt:

                        j_row_counter_in_ppt = 0

                        single_column_list = []

                        while j_row_counter_in_ppt < num_of_rows_in_ppt:

                            col_text = shape.table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text

                            print('row: ', j_row_counter_in_ppt)
                            print('col: ', i_col_counter_in_ppt)
                            print('cell text:', col_text)

                            if j_row_counter_in_ppt == 0:
                                is_column = True
                            else:
                                is_column = False

                            if is_column:

                                if '{{col:' in col_text:
                                    print('inside col')

                                    cell_text = shape.table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text

                                    #                                 '''

                                    # df_name = cell_text[cell_text.find('{{col:') + 6: cell_text.find('.csv') + 4]
                                    # print('df_name: ', df_name)
                                    # # df = pd.read_csv(workpath + '\\' + df_name)
                                    # df = pd.read_csv(os.path.join(workpath, df_name))
                                    #
                                    # df_columns = df.columns
                                    # print('df_columns:', df_columns)
                                    #
                                    # df_length_new = len(df.index)
                                    #
                                    # if df_length == -1 or df_length < df_length_new:
                                    #     df_length = df_length_new
                                    #
                                    # if len(df_name) > 0:
                                    #     cell_text_index = int(cell_text[cell_text.find('[') + 1:cell_text.find(']')])
                                    #     print('cell_text_index:', cell_text_index)
                                    #     column_name = df_columns[cell_text_index - 1]
                                    #     print('column_name:', column_name)
                                    # else:
                                    #     column_name = ''
                                    #
                                    # column_name_list.append(column_name)
                                    #
                                    # single_column_list = list(df[column_name].values)
                                    #
                                    # skip_column_list.append(i_col_counter_in_ppt)
                                    #                                 '''

                                    try:

                                        df_name = cell_text[cell_text.find('{{col:') + 6: cell_text.find('.csv') + 4]
                                        print('df_name: ', df_name)
                                        # df = pd.read_csv(workpath + '\\' + df_name)
                                        df = pd.read_csv(os.path.join(workpath, df_name))

                                        df_columns = df.columns
                                        print('df_columns:', df_columns)

                                        df_length_new = len(df.index)

                                        if df_length == -1 or df_length < df_length_new:
                                            df_length = df_length_new

                                        if len(df_name) > 0:
                                            cell_text_index = int(
                                                cell_text[cell_text.find('[') + 1:cell_text.find(']')])
                                            print('cell_text_index:', cell_text_index)
                                            column_name = df_columns[cell_text_index - 1]
                                            print('column_name:', column_name)
                                        else:
                                            column_name = ''

                                        column_name_list.append(column_name)

                                        single_column_list = list(df[column_name].values)

                                        skip_column_list.append(i_col_counter_in_ppt)

                                    except:
                                        column_name = cell_text.replace('**', '') + "**"
                                        column_name_list.append(column_name)


                                else:

                                    if i_col_counter_in_ppt in skip_column_list:
                                        j_row_counter_in_ppt += 1
                                        break

                                    cell_text = shape.table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text
                                    print('column cell_text:', cell_text)
                                    column_name_list.append(cell_text)

                            else:
                                if i_col_counter_in_ppt in skip_column_list:
                                    j_row_counter_in_ppt += 1
                                    break

                                cell_text = shape.table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text
                                #
                                if '{{val:' in cell_text:
                                    print('yes, value')

                                    #                                 '''

                                    #                             if df_name == '':
                                    # df_name = cell_text[cell_text.find('{{val:') + 6: cell_text.find('.csv') + 4]
                                    # print('df_name: ', df_name)
                                    # # df = pd.read_csv(workpath + '\\' + df_name)
                                    # df = pd.read_csv(os.path.join(workpath, df_name))
                                    #
                                    # if ':' in cell_text:
                                    #     cell_text_index = cell_text[cell_text.find('[') + 1:cell_text.find(']')].split(
                                    #         ':')
                                    #     print('cell_text_index:', cell_text_index)
                                    #
                                    #     # cell_value = df.iloc[int(cell_text_index[1]) - 2, int(cell_text_index[0]) -1]
                                    #     cell_value = df.iloc[int(cell_text_index[0]) - 2, int(cell_text_index[1]) -1]

                                    # else:
                                    #     cell_value = cell_text
                                    #
                                    # print('cell_value:', cell_value)

                                    #                                 '''

                                    try:
                                        df_name = cell_text[cell_text.find('{{val:') + 6: cell_text.find('.csv') + 4]
                                        print('df_name: ', df_name)
                                        # df = pd.read_csv(workpath + '\\' + df_name)
                                        df = pd.read_csv(os.path.join(workpath, df_name))

                                        if ':' in cell_text:
                                            cell_text_index = cell_text[
                                                              cell_text.find('[') + 1:cell_text.find(']')].split(
                                                ':')
                                            print('cell_text_index:', cell_text_index)

                                            # cell_value = df.iloc[int(cell_text_index[1]) - 2, int(cell_text_index[0]) -1]
                                            cell_value = df.iloc[
                                                int(cell_text_index[0]) - 2, int(cell_text_index[1]) - 1]

                                        else:
                                            cell_value = cell_text

                                        print('cell_value:', cell_value)
                                    except:
                                        cell_value = cell_text.replace('**', '') + "**"

                                else:
                                    cell_value = cell_text

                                single_column_list.append(cell_value)

                            #                             '''

                            #                         print('After Modification cell text:', cell_text)

                            #                         if '{{col:' in cell_text:
                            #                             print('yes')

                            #                             if df_name == '':
                            #                             df_name = cell_text[cell_text.find('{{col:') + 6 : cell_text.find('.csv') + 4]
                            #                             print('df_name: ', df_name)
                            #                             df = pd.read_csv(r'C:\Users\KT250034\Pictures\customer_success\\' + df_name)

                            #                             df_columns = df.columns
                            #                             print('df_columns:', df_columns)

                            #                             df_length_new = len(df.index)

                            #                             if df_length == -1 or df_length < df_length_new:
                            #                                 df_length = df_length_new

                            #                             if len(df_name)>0:
                            #                                 cell_text_index = int(cell_text[cell_text.find('[') + 1:cell_text.find(']')])
                            #                                 print('cell_text_index:',cell_text_index)
                            #                                 cell_value = df_columns[cell_text_index]
                            #                                 print('cell_value:',cell_value)

                            #                             else:
                            #                                 cell_value = ''

                            #                         write column headings
                            #                         try:
                            #                             table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text = str(cell_value)
                            #                         except:
                            #                             table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text = cell_value.astype(numpy.str)
                            #                         '''

                            j_row_counter_in_ppt += 1

                        single_column_list = Utils.pad(single_column_list, '', df_length)

                        columns_list.append(single_column_list)

                        i_col_counter_in_ppt += 1

                    print('column_name_list: ', column_name_list)
                    print('columns_list: ', columns_list)

                    #                 '''

                    #                 df_test = pd.DataFrame(list(zip(lst1, lst2, lst3)),
                    #                 columns=['lst1_title','lst2_title', 'lst3_title'])

                    #                 '''

                    # if len(columns_list[0][0]) > 0:
                    if len(columns_list) > 0:
                        df_new = pd.DataFrame(columns_list, index=None)
                        df_new = df_new.transpose()
                        print(df_new)

                        df_new.columns = column_name_list

                        rows = df_new.shape[0] + 1
                        cols = df_new.shape[1]
                        left = top = Inches(2.0)
                        width = Inches(6.0)

                        rows = num_of_rows_in_ppt
                        cols = num_of_columns_in_ppt

                        #                     num_of_columns_in_ppt = len(shape.table.columns)
                        #                 num_of_rows_in_ppt = len(shape.table.rows)

                        #                     '''
                        #                 height_inch = 0.6 * rows
                        # height = Inches(height_inch)

                        #                 if is_entire_column:
                        #                     height = Inches(height_inch)
                        #                 else:
                        #                     height = shape.height
                        #                     '''

                        height = shape.height

                        #                     table = slide.shapes.add_table(rows, cols, left=shape.left, top=shape.top, width=shape.width,
                        #                                                    height=height).table

                        #                     '''
                        #                 table = slide.shapes.add_table(rows, cols, left=shape.left, top=shape.top, width=shape.width
                        #                                               ).table

                        #                 # set column widths
                        #                 table.columns[0].width = Inches(2.0)
                        #                 table.columns[1].width = Inches(4.0)

                        #                     table.rows[0].width = Inches(2.0)
                        #                     table.rows[1].width = Inches(4.0)

                        #                     write column headings
                        #                     '''

                        r = 0

                        for c in range(0, len(column_name_list)):
                            cell_value = column_name_list[c]
                            try:
                                shape.table.cell(0, c).text = str(cell_value)
                            except:
                                shape.table.cell(0, c).text = cell_value.astype(numpy.str)

                        while r < rows - 1:
                            c = 0
                            while c < cols:
                                #                                 table.cell(r, c).text = df_new.iloc[r, c]
                                # print('r: ', r)
                                # print('c: ', c)
                                cell_value = df_new.iloc[r, c]

                                try:
                                    shape.table.cell(r + 1, c).text = str(cell_value)
                                except:
                                    shape.table.cell(r + 1, c).text = cell_value.astype(numpy.str)

                                c += 1
                            r += 1

                        # remove placeholder shape
        #                     shape_to_remove = shape._element
        #                     shape_to_remove.getparent().remove(shape_to_remove)

        prs.save(pptx_path)  # save updated pptx

    @staticmethod
    def insert_to_pptx_old(pptx_path, workpath):
        prs = Presentation(pptx_path)

        for slide in prs.slides:  # loop through all slides
            for shape in slide.shapes:  # loop through all shape objects in a slide

                # shape_type 19 = table which does not have text field
                # if shape.shape_type not in (13, 19) and '{{' in shape.text and '}}' in shape.text:  # search for special command
                if shape.shape_type in (1, 14) and '{{' in shape.text and '}}' in shape.text:  # search for special command
                    # insert image
                    if '.png' in shape.text:
                        # img_name = shape.text.replace('{{pic:', '').replace('}}', '')  # get img file name to be inserted
                        # slide.shapes.add_picture(os.path.join(workpath, img_name),
                        #                          left=shape.left,
                        #                          top=shape.top,
                        #                          height=shape.height,
                        #                          width=shape.width)  # insert with same dimensions as placeholder
                        #
                        # # remove placeholder shape
                        # shape_to_remove = shape._element
                        # shape_to_remove.getparent().remove(shape_to_remove)

                        try:
                            img_name = shape.text.replace('{{pic:', '').replace('}}', '').replace('**',
                                                                                                  '')  # get img file name to be inserted
                            slide.shapes.add_picture(os.path.join(workpath, img_name),
                                                     left=shape.left,
                                                     top=shape.top,
                                                     height=shape.height,
                                                     width=shape.width)  # insert with same dimensions as placeholder

                            # remove placeholder shape
                            shape_to_remove = shape._element
                            shape_to_remove.getparent().remove(shape_to_remove)

                        except OSError as e:
                            text_ori = shape.text.replace('**', '')
                            text_frame = shape.text_frame
                            text_frame.clear()  # not necessary for newly-created shape

                            p = text_frame.paragraphs[0]
                            run = p.add_run()
                            run.text = text_ori + '**'

                            font = run.font
                            #                         font.name = 'Calibri'
                            #                         font.size = Pt(18)
                            #                         font.bold = True
                            #                         font.color.rgb = RGBColor(0xFF, 0x7F, 0x50)

                            print("File Not Found!!")
                    else:
                        # csv_name = shape.text[shape.text.find("{{")+2 : shape.text.find("}}")]
                        file_not_found = False
                        index = 0
                        csv_name_list = []
                        text = shape.text

                        while index < len(text):
                            index = text.find('{{val:', index)
                            if index == -1:
                                break
                            print('{{ found at', index)

                            index += 6  # +2 because len('ll') == 2
                            start_index = index

                            index = text.find('}}', index)
                            if index == -1:
                                break
                            print('}} found at', index)

                            end_index = index
                            index += 2  # +2 because len('ll') == 2

                            csv_name = text[start_index:end_index]
                            csv_name_list.append(csv_name)

                        csv_name_value = []

                        for csv_name in csv_name_list:
                            df_name_cell = csv_name.split('.csv')
                            df_name = df_name_cell[0]
                            df_cell = df_name_cell[1]
                            # df_csv = pd.read_csv(os.path.join(workpath, df_name + '.csv'))
                            # # df_value = df_csv[df_cell[1]:df_cell[3]]
                            # df_value = df_csv.iloc[int(df_cell[1]) - 2, int(df_cell[3]) - 1]
                            # csv_name_value.append(df_value)

                            try:
                                df_csv = pd.read_csv(os.path.join(workpath, df_name + '.csv'))
                                # df_value = df_csv[df_cell[1]:df_cell[3]]
                                df_value = df_csv.iloc[int(df_cell[1]) - 2, int(df_cell[3]) - 1]
                                csv_name_value.append(df_value)
                            except OSError as e:

                                csv_name_value.append(csv_name)
                                file_not_found = True

                        text_2 = ''
                        i = 0
                        index = 0
                        text_start = 0

                        while index < len(text):
                            index = text.find('{{val:', index)
                            if index == -1:
                                break
                            print('{{ found at', index)

                            index += 6  # +2 because len('ll') == 2
                            start_index = index

                            index = text.find('}}', index)
                            if index == -1:
                                break
                            print('}} found at', index)

                            end_index = index
                            index += 2  # +2 because len('ll') == 2
                            #     text_start = index

                            print('text_2:', text_2)
                            print('text:', text)
                            print('text_start:', text_start)
                            print('start_index:', start_index)
                            print('text[text_start:start_index - 6]:', text[text_start:start_index - 6])
                            print('csv_name_list[i]:', csv_name_list[i])

                            try:
                                text_2 = text_2 + text[text_start:start_index - 6] + csv_name_value[i]
                            except:
                                # text_2 = text_2 + text[text_start:start_index - 6] + csv_name_value[i].astype(numpy.str)
                                try:
                                    text_2 = text_2 + text[text_start:start_index - 6] + csv_name_value[i].astype(
                                        numpy.str)
                                except:
                                    file_not_found = True
                            i += 1

                            text_start = index

                        text_2 = text_2 + text[end_index + 2:]

                        # #     csv_name = text[start_index:end_index]
                        # #     csv_name_list.append(csv_name)
                        #
                        # # new_text = shape.text[0:shape.text.find("{{")] + df_value + " " + shape.text[shape.text.find("}}") + 2 : ]
                        # shape.text = text_2

                        if file_not_found == True:
                            shape.text = text_ori.replace('**', '') + "**"
                        else:
                            shape.text = text_2.replace('**', '')

                elif shape.shape_type == 19:
                    num_of_columns_in_ppt = len(shape.table.columns)
                    num_of_rows_in_ppt = len(shape.table.rows)

                    df_name = ''

                    index = 0

                    print('\n')

                    print('New Table: ')
                    print('total number of cols: ', num_of_columns_in_ppt)
                    print('total number of rows: ', num_of_rows_in_ppt)

                    i_col_counter_in_ppt = 0
                    is_entire_column = False

                    df_length = -1

                    column_name_list = []
                    columns_list = []

                    skip_column_list = []

                    while i_col_counter_in_ppt < num_of_columns_in_ppt:

                        j_row_counter_in_ppt = 0

                        single_column_list = []

                        while j_row_counter_in_ppt < num_of_rows_in_ppt:

                            col_text = shape.table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text

                            print('row: ', j_row_counter_in_ppt)
                            print('col: ', i_col_counter_in_ppt)
                            print('cell text:', col_text)

                            if j_row_counter_in_ppt == 0:
                                is_column = True
                            else:
                                is_column = False

                            if is_column:

                                if '{{col:' in col_text:
                                    print('inside col')

                                    cell_text = shape.table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text

                                    # df_name = cell_text[cell_text.find('{{col:') + 6: cell_text.find('.csv') + 4]
                                    # print('df_name: ', df_name)
                                    # # df = pd.read_csv(workpath + '\\' + df_name)
                                    # df = pd.read_csv(os.path.join(workpath, df_name))
                                    #
                                    # df_columns = df.columns
                                    # print('df_columns:', df_columns)
                                    #
                                    # df_length_new = len(df.index)
                                    #
                                    # if df_length == -1 or df_length < df_length_new:
                                    #     df_length = df_length_new
                                    #
                                    # if len(df_name) > 0:
                                    #     cell_text_index = int(cell_text[cell_text.find('[') + 1:cell_text.find(']')])
                                    #     print('cell_text_index:', cell_text_index)
                                    #     column_name = df_columns[cell_text_index - 1]
                                    #     print('column_name:', column_name)
                                    # else:
                                    #     column_name = ''
                                    #
                                    # column_name_list.append(column_name)
                                    #
                                    # single_column_list = list(df[column_name].values)
                                    #
                                    # skip_column_list.append(i_col_counter_in_ppt)

                                    try:

                                        df_name = cell_text[cell_text.find('{{col:') + 6: cell_text.find('.csv') + 4]
                                        print('df_name: ', df_name)
                                        # df = pd.read_csv(workpath + '\\' + df_name)
                                        df = pd.read_csv(os.path.join(workpath, df_name))

                                        df_columns = df.columns
                                        print('df_columns:', df_columns)

                                        df_length_new = len(df.index)

                                        if df_length == -1 or df_length < df_length_new:
                                            df_length = df_length_new

                                        if len(df_name) > 0:
                                            cell_text_index = int(
                                                cell_text[cell_text.find('[') + 1:cell_text.find(']')])
                                            print('cell_text_index:', cell_text_index)
                                            column_name = df_columns[cell_text_index - 1]
                                            print('column_name:', column_name)
                                        else:
                                            column_name = ''

                                        column_name_list.append(column_name)

                                        single_column_list = list(df[column_name].values)

                                        skip_column_list.append(i_col_counter_in_ppt)

                                    except:
                                        column_name = cell_text.replace('**', '') + "**"
                                        column_name_list.append(column_name)


                                else:

                                    if i_col_counter_in_ppt in skip_column_list:
                                        j_row_counter_in_ppt += 1
                                        break

                                    cell_text = shape.table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text
                                    print('column cell_text:', cell_text)
                                    column_name_list.append(cell_text)

                            else:
                                if i_col_counter_in_ppt in skip_column_list:
                                    j_row_counter_in_ppt += 1
                                    break

                                cell_text = shape.table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text
                                #
                                if '{{val:' in cell_text:
                                    print('yes, value')

                                    #                             if df_name == '':
                                    # df_name = cell_text[cell_text.find('{{val:') + 6: cell_text.find('.csv') + 4]
                                    # print('df_name: ', df_name)
                                    # # df = pd.read_csv(workpath + '\\' + df_name)
                                    # df = pd.read_csv(os.path.join(workpath, df_name))
                                    #
                                    # if ':' in cell_text:
                                    #     cell_text_index = cell_text[cell_text.find('[') + 1:cell_text.find(']')].split(
                                    #         ':')
                                    #     print('cell_text_index:', cell_text_index)
                                    #
                                    #     # cell_value = df.iloc[int(cell_text_index[1]) - 2, int(cell_text_index[0]) -1]
                                    #     cell_value = df.iloc[int(cell_text_index[0]) - 2, int(cell_text_index[1]) -1]


                                    # else:
                                    #     cell_value = cell_text
                                    #
                                    # print('cell_value:', cell_value)

                                    try:
                                        df_name = cell_text[cell_text.find('{{val:') + 6: cell_text.find('.csv') + 4]
                                        print('df_name: ', df_name)
                                        # df = pd.read_csv(workpath + '\\' + df_name)
                                        df = pd.read_csv(os.path.join(workpath, df_name))

                                        if ':' in cell_text:
                                            cell_text_index = cell_text[
                                                              cell_text.find('[') + 1:cell_text.find(']')].split(
                                                ':')
                                            print('cell_text_index:', cell_text_index)

                                            # cell_value = df.iloc[int(cell_text_index[1]) - 2, int(cell_text_index[0]) -1]
                                            cell_value = df.iloc[
                                                int(cell_text_index[0]) - 2, int(cell_text_index[1]) - 1]

                                        else:
                                            cell_value = cell_text

                                        print('cell_value:', cell_value)
                                    except:
                                        cell_value = cell_text.replace('**', '') + "**"

                                else:
                                    cell_value = cell_text

                                single_column_list.append(cell_value)

                            #                         print('After Modification cell text:', cell_text)

                            #                         if '{{col:' in cell_text:
                            #                             print('yes')

                            #                             if df_name == '':
                            #                             df_name = cell_text[cell_text.find('{{col:') + 6 : cell_text.find('.csv') + 4]
                            #                             print('df_name: ', df_name)
                            #                             df = pd.read_csv(r'C:\Users\KT250034\Pictures\customer_success\\' + df_name)

                            #                             df_columns = df.columns
                            #                             print('df_columns:', df_columns)

                            #                             df_length_new = len(df.index)

                            #                             if df_length == -1 or df_length < df_length_new:
                            #                                 df_length = df_length_new

                            #                             if len(df_name)>0:
                            #                                 cell_text_index = int(cell_text[cell_text.find('[') + 1:cell_text.find(']')])
                            #                                 print('cell_text_index:',cell_text_index)
                            #                                 cell_value = df_columns[cell_text_index]
                            #                                 print('cell_value:',cell_value)

                            #                             else:
                            #                                 cell_value = ''

                            #                         write column headings
                            #                         try:
                            #                             table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text = str(cell_value)
                            #                         except:
                            #                             table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text = cell_value.astype(numpy.str)

                            j_row_counter_in_ppt += 1

                        single_column_list = Utils.pad(single_column_list, '', df_length)

                        columns_list.append(single_column_list)

                        i_col_counter_in_ppt += 1



                    print('column_name_list: ', column_name_list)
                    print('columns_list: ', columns_list)

                    #                 df_test = pd.DataFrame(list(zip(lst1, lst2, lst3)),
                    #                 columns=['lst1_title','lst2_title', 'lst3_title'])

                    # if len(columns_list[0][0]) > 0:
                    if len(columns_list) > 0:
                        df_new = pd.DataFrame(columns_list, index=None)
                        df_new = df_new.transpose()
                        print(df_new)

                        df_new.columns = column_name_list

                        rows = df_new.shape[0] + 1
                        cols = df_new.shape[1]
                        left = top = Inches(2.0)
                        width = Inches(6.0)
                        #                 height_inch = 0.6 * rows
                        # height = Inches(height_inch)

                        #                 if is_entire_column:
                        #                     height = Inches(height_inch)
                        #                 else:
                        #                     height = shape.height

                        height = shape.height

                        table = slide.shapes.add_table(rows, cols, left=shape.left, top=shape.top, width=shape.width,
                                                       height=height).table
                        #                 table = slide.shapes.add_table(rows, cols, left=shape.left, top=shape.top, width=shape.width
                        #                                               ).table

                        #                 # set column widths
                        #                 table.columns[0].width = Inches(2.0)
                        #                 table.columns[1].width = Inches(4.0)

                        #                     table.rows[0].width = Inches(2.0)
                        #                     table.rows[1].width = Inches(4.0)

                        #                     write column headings

                        r = 0

                        for c in range(0, len(column_name_list)):
                            cell_value = column_name_list[c]
                            try:
                                table.cell(0, c).text = str(cell_value)
                            except:
                                table.cell(0, c).text = cell_value.astype(numpy.str)

                        while r < rows - 1:
                            c = 0
                            while c < cols:
                                #                                 table.cell(r, c).text = df_new.iloc[r, c]
                                # print('r: ', r)
                                # print('c: ', c)
                                cell_value = df_new.iloc[r, c]

                                try:
                                    table.cell(r + 1, c).text = str(cell_value)
                                except:
                                    table.cell(r + 1, c).text = cell_value.astype(numpy.str)

                                c += 1
                            r += 1

                        # remove placeholder shape
                        shape_to_remove = shape._element
                        shape_to_remove.getparent().remove(shape_to_remove)

        prs.save(pptx_path)  # save updated pptx
