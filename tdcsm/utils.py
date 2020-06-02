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


class Utils(Logger):

    def __init__(self, version):
        super().__init__()  # inherits Logger class
        self.version = version

    # todo remove and replace with default file?
    def yaml_filesets(self, filesetpath, writefile=False):
        self.log('generating filesets.yaml from internal default')
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
            self.log('  saving file', filesetpath)
        return rtn

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
                self.log(msg, warning=True)

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
            shutil.rmtree(delpath)
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

    def open_connection(self, conntype, host='', logmech='', username='', password='', system=None, skip=False):
        if system is None:
            system = {}
        self.log('OPEN_CONNECTION started', str(dt.datetime.now()))
        self.log('  connection type', conntype)
        # check all variables... use system{} as default, individual variables as overrides
        host = host.strip().lower()
        logmech = logmech.strip().lower()
        conntype = conntype.strip().lower()

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
                'password': password
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
                                                           driver=conntype)

        self.log('connected!', str(dt.datetime.now()))
        return connObject

    def open_sql(self, connobject, sql, skip=False):
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
                df = pd.read_sql(sql, conn)

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
    def insert_to_pptx(pptx_path, workpath):
        prs = Presentation(pptx_path)

        for slide in prs.slides:  # loop through all slides
            for shape in slide.shapes:  # loop through all shape objects in a slide

                # shape_type 19 = table which does not have text field
                if shape.shape_type != 19 and '{{' in shape.text and '}}' in shape.text:  # search for special command

                    # insert image
                    if '.png' in shape.text:
                        img_name = shape.text.replace('{{pic:', '').replace('}}', '')  # get img file name to be inserted
                        slide.shapes.add_picture(os.path.join(workpath, img_name),
                                                 left=shape.left,
                                                 top=shape.top,
                                                 height=shape.height,
                                                 width=shape.width)  # insert with same dimensions as placeholder

                        # remove placeholder shape
                        shape_to_remove = shape._element
                        shape_to_remove.getparent().remove(shape_to_remove)
                    else:
                        # csv_name = shape.text[shape.text.find("{{")+2 : shape.text.find("}}")]

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
                            df_csv = pd.read_csv(os.path.join(workpath, df_name + '.csv'))
                            # df_value = df_csv[df_cell[1]:df_cell[3]]
                            df_value = df_csv.iloc[int(df_cell[1]) - 1, int(df_cell[3])]
                            csv_name_value.append(df_value)

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
                                text_2 = text_2 + text[text_start:start_index - 6] + csv_name_value[i].astype(numpy.str)

                            i += 1

                            text_start = index

                        text_2 = text_2 + text[end_index + 2:]

                        #     csv_name = text[start_index:end_index]
                        #     csv_name_list.append(csv_name)

                        # new_text = shape.text[0:shape.text.find("{{")] + df_value + " " + shape.text[shape.text.find("}}") + 2 : ]
                        shape.text = text_2

                # elif shape.shape_type == 19:
                #     num_of_columns_in_ppt = len(shape.table.columns) # - 1 # Leaving out last column for Notes
                #     num_of_rows_in_ppt = len(shape.table.rows)
                #
                #     # shape.table.cell(row_no, col_no).text
                #
                #     # Checking 1
                #     # text in column headers
                #     i_col_counter_in_ppt = 0
                #     j_row_counter_in_ppt = 0
                #     is_entire_column = False
                #     index = 0
                #
                #     special_character_columns_index_in_ppt = []
                #
                #     # cell_text =
                #     df_name = ''
                #
                #     rows = num_of_rows_in_ppt
                #     cols = num_of_columns_in_ppt
                #     left = top = Inches(2.0)
                #     width = Inches(6.0)
                #     height_inch = 0.6 * rows
                #     # height = Inches(height_inch)
                #
                #     if is_entire_column:
                #         height = Inches(height_inch)
                #     else:
                #         height = shape.height
                #
                #     table = slide.shapes.add_table(rows, cols, left=shape.left, top=shape.top, width=shape.width,
                #                                    height=height).table
                #
                #     # set column widths
                #     table.columns[0].width = Inches(2.0)
                #     table.columns[1].width = Inches(4.0)
                #
                #     while j_row_counter_in_ppt < num_of_rows_in_ppt:
                #
                #         while i_col_counter_in_ppt < num_of_columns_in_ppt:
                #
                #             if j_row_counter_in_ppt == 0:
                #                 is_column = True
                #             else:
                #                 is_column = False
                #
                #             if is_column:
                #                 col_text = shape.table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text
                #
                #                 if '{{col:' in col_text:
                #                     for row_number in range(0, rows):
                #                         row_text = col_text.replace('col', 'val')
                #
                #                         index = row_text.find(']', 0)
                #                         row_text_new = row_text[0:index] + ':' + str(row_number) + row_text[index:]
                #                         shape.table.cell(row_number, i_col_counter_in_ppt).text = row_text_new
                #
                #                 cell_text = shape.table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text
                #
                #             else:
                #                 cell_text = shape.table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text
                #
                #             if df_name == '':
                #                 df_name = cell_text[cell_text.find('{{val:') + 6 : cell_text.find('.csv') + 4]
                #                 if df_name == '':
                #                     break
                #                 df = pd.read_csv(r'C:\Users\KT250034\Pictures\customer_success\\' + df_name)
                #
                #             cell_text_index = cell_text[cell_text.find('[') + 1:cell_text.find(']')].split(':')
                #             if len(cell_text_index) == 1:
                #                 cell_value = cell_text_index[0]
                #             else:
                #                 cell_value = df.iloc[int(cell_text_index[1]),int(cell_text_index[0])]
                #
                #             # write column headings
                #             # table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text = cell_value.astype(numpy.int32)
                #             try:
                #                 table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text = str(cell_value)
                #             except:
                #                 table.cell(j_row_counter_in_ppt, i_col_counter_in_ppt).text = cell_value.astype(numpy.str)
                #
                #
                #
                #             i_col_counter_in_ppt += 1
                #
                #         j_row_counter_in_ppt += 1
                #
                #
                #
                #
                #
                #     # remove placeholder shape
                #     shape_to_remove = shape._element
                #     shape_to_remove.getparent().remove(shape_to_remove)
                #
                #
                #
                #
                #
                #
                #     cell_0_0_text = shape.table.cell(0,0).text
                #     # cell_0_1_text = shape.table.cell(0,1).text()
                #     # cell_0_2_text = shape.table.cell(0,2).text()
                #     # cell_0_3_text = shape.table.cell(0,3).text()
                #     #
                #     # print(cell_0_0_text)
                #
                #     # w = shape.width
                #     pass

        prs.save(pptx_path)  # save updated pptx

