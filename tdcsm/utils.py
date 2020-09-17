import datetime as dt
import os
import shutil
import numpy

import pandas as pd
from .logging import Logger
from pptx import Presentation
from pptx.util import Inches, Pt
import textwrap

## import statements for visualizations

# import os
# import numpy as np
# import pandas as pd
# from pandas.plotting import andrews_curves
# from pandas.plotting import parallel_coordinates
# import seaborn as sns
# matplotlib and related imports
# import matplotlib.pyplot as plt
# from matplotlib.path import Path
# from matplotlib.patches import PathPatch
# from matplotlib.patches import Patch
# import matplotlib.patches as patches
# import datetime
# import matplotlib.dates as mdates
# import matplotlib.style as style
# style.available
# import matplotlib.ticker as tick
# from matplotlib.lines import Line2D


import matplotlib
from mpl_toolkits.axes_grid1 import make_axes_locatable
import matplotlib.ticker as ticker
import os
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")
import matplotlib.font_manager
import seaborn as sns
import numpy as np
import pandas as pd
import matplotlib.markers as mrkrs

sns.set(font_scale=1.4)
sns.set(rc={'figure.figsize': (20, 12)})


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
                    self.log('column: %s is python type: %s' %(colname, colpytype))

                    if str(colpytype)[:3] == 'int':
                        coltype = 'BIGINT'
                        coldefs[colname] = {'type': 'bigint', 'len': 0, 'quote': '', 'null': 'NULL'}
                    if str(colpytype)[:5] == 'float':
                        coltype = 'DECIMAL(32,10)'
                        coldefs[colname] = {'type': 'decimal(32,10)', 'len': 32, 'quote': '', 'null': 'NULL'}
                    else: # str(colpytype) == 'object':
                        collen = dfcsv[colname].map(str).map(len).max() + 100
                        coltype = 'VARCHAR(%i)  CHARACTER SET UNICODE ' % collen
                        coldefs[colname] = {'type': 'varchar(%i)' % collen, 'len': collen, 'quote': "'", 'null': "''"}

                    self.log('    translated to db type: %s' %coltype)
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
    def validate_boolean(sbool, returntype = 'string'):
        """accepts a string-as-boolean, and depending on returntype[:3], returns
          - 'str'  == well-formed bool as string (default)
          - 'boo' == boolean value itself
        Special note: this is the first step in transitioning YAML from string-as-boolean to pure boolean: isolate the change as-is.
        Once we've wrapped all instances here, we can change the default behavior once and test results."""
        if str(sbool).strip().lower() == 'true':
            if returntype[:3] == 'str': return 'True'
            if returntype[:3] == 'boo': return  True
        elif str(sbool).strip().lower() == 'false':
            if returntype[:3] == 'str': return 'False'
            if returntype[:3] == 'boo': return  False

    @staticmethod
    def format_sql(sqltext):
        sql = str(sqltext).strip().split(';')[0]
        while '\n\n' in sql:
            sql = sql.replace('\n\n', '\n').strip()

        sql = sql.strip() + '\n;\n\n'

        if sql.replace(';', '').strip() == '':
            sql = ''

        # else:
        #     # replace --comments with /* comments */
        #     newsql = []
        #     for line in sql.split('\n'):
        #         if line.strip()[:2] != '/*':  # skip comment-starting  lines
        #             lineparts = line.split('--')
        #             if len(lineparts) != 1:
        #                 firstpart = lineparts[0].strip()
        #                 secondpart = line[len(firstpart) + 2:].strip()
        #                 newsql.append('%s /* %s */' % (firstpart, secondpart))
        #             else:
        #                 newsql.append(line)
        #         else:
        #             newsql.append(line)
        #     sql = '\n'.join(newsql).strip()

        return sql

    def validate_all_filepaths(self, filepaths=[], mustbe_file=False, throw_errors=False):
        rtn = True
        self.log('validating %i filepaths' %len(filepaths))
        for filepath in filepaths:
            if not os.path.exists(filepath):
                rtn = False
                break
            if mustbe_file and not os.path.isfile(filepath):
                rtn = False
                break
        if throw_errors and rtn==False:
            msg = "not all filepaths provided are valid"
            self.log(msg, error=True)
            raise ValueError(msg)
        return rtn

    def cast_list(self, parm, dict_convert='keys'):
        """Ensures the parm is a list. If a string, turn into a list.
        If a dict, choose whether to convert [keys|values|both|none].
        In all cases, return a list."""
        if type(parm)==str:
            rtn.append(parm)
        elif type(parm)==list:
            rtn = parm
        elif type(parm)==dict:
            rtn=[] # none returns empty list
            if dict_convert=='keys':   rtn = list(parm)
            if dict_convert=='values': rtn = list(parm.values())
            if dict_convert=='both':
                for n,v in parm.items():
                    rtn.append(n)
                    rtn.append(v)
        else:
            msg = "must be list or string,\n you supplied %s" %type(filepaths)
            self.log(msg, error=True)
            raise ValueError(msg)
        return rtn

    def cast_dict(self, parm):
        """Ensures the parm is a dict.  If it comes in as a string or list,
        turn into a dict where key = value.  Always returns a dict."""
        if type(parm)==str:
            rtn={parm:parm}
        elif type(parm)==list:
            r={}
            for p in parm:
                r[p]=p
        elif type(parm)==dict:
            rtn = parm
        else:
            msg = "must be dict, list, or string,\n you supplied %s" %type(filepaths)
            self.log(msg, error=True)
            raise ValueError(msg)
        return rtn

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

    def get_special_commands(self, sql, replace_with='', keys_to_skip=None, indent=0):
        if keys_to_skip is None:
            keys_to_skip = []

        cmdstart = '/*{{'
        cmdend = '}}*/'
        cmds = {}
        sqltext = sql
        replace_with = '/* %s */' % replace_with if replace_with != '' else replace_with
        self.log('  parsing for special sql commands', indent=indent)

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

            self.log('   special command found', '%s = %s' % (cmdkey, cmdval), indent=indent+2)
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
                self.log('   %s found in keys_to_skip, skipping...' % cmdkey, indent=indent+2)
            else:
                cmds[cmdkey]['skip'] = False

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
                self.log('  ACTIVE dictionary', dictName)
                return True

        self.log('  Inactive dictionary', dictName)

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

        if skip:
            self.log('skip dbs setting is true, emulating closure...')

        else:
            connobject['connection'].close()
            connobject['connection'] = None

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
            from .dbutil import connect
            connObject['connection'] = connect(**connObject['components'])

        self.log('connected!', str(dt.datetime.now()))
        return connObject

    def open_sql(self, connobject, sql, skip=False, columns=False):
        self.log('sql, first 100 characters:\n  %s' % sql[:100].replace('\n', ' ').strip() + '...')
        self.log('sql submitted', str(dt.datetime.now()))

        if self.show_full_sql:
            self.log('full sql:', '\n%s\n' % sql)

        if skip:
            self.log('skip dbs setting is true, emulating execution...')
            df = pd.DataFrame(columns=list('ABCD'))

        else:
            from .dbutil import sql_to_df
            df = sql_to_df(connobject['connection'], sql)

        self.log('sql completed', str(dt.datetime.now()))
        self.log('record count', str(len(df)))
        return df

    @staticmethod
    def set_plot_sizes(plt, small = 20, medium = 30, big = 40):
        """
        This function sets the sizes of font, axes title, axes label, xtick, ytick, legend and figure title.
        Sizes could be either small, medium or big.

        :param plt: input matplotlib plot object. Required
        :param small: Integer value for small size. Default set as 20
        :param medium: Interger value for medium size
        :param big: Integer value for big size
        :return: output matplotlib plot object with sizes set as per the parameters

        """
        SMALL_SIZE = small
        MEDIUM_SIZE = medium
        BIGGER_SIZE = big

        plt.rc('font', size=SMALL_SIZE)  # controls default text sizes
        plt.rc('axes', titlesize=MEDIUM_SIZE)  # fontsize of the axes title
        plt.rc('axes', labelsize=MEDIUM_SIZE)  # fontsize of the x and y labels
        plt.rc('xtick', labelsize=SMALL_SIZE)  # fontsize of the tick labels
        plt.rc('ytick', labelsize=SMALL_SIZE)  # fontsize of the tick labels
        plt.rc('legend', fontsize=SMALL_SIZE)  # legend fontsize
        plt.rc('figure', titlesize=MEDIUM_SIZE)  # fontsize of the figure title

        return plt

    @staticmethod
    def human_readable_names(input_str_with_underscore):

        names_dict = {}
        names_dict['Cnt'] = 'Count'
        names_dict['Sec'] = 'Seconds'
        names_dict['Avg'] = 'Average'
        names_dict['80Pctl'] = '80th Percentile'
        names_dict['95Pctl'] = '95th Percentile'
        names_dict['Concurrency'] = 'Concurrency,'




        out_list = []
        name_list = input_str_with_underscore.split('_')

        for name in name_list:
            if name in list(names_dict.keys()):
                out_list.append(names_dict[name])
            else:
                out_list.append(name)

        output_str = ' '.join(out_list)
        return output_str

    @staticmethod
    def scatter_plot(df_with_selected_cols, style_column, factor_x, factor_y, hue_column, markers, bucket_unique_list):
        scatterplot = sns.scatterplot(data=df_with_selected_cols, style=Utils.human_readable_names(style_column),
                                      alpha=.8, s=200, palette="muted", x=Utils.human_readable_names(factor_x),
                                      y=Utils.human_readable_names(factor_y), hue=Utils.human_readable_names(hue_column),
                                      markers=markers)

        lgnd = plt.legend(loc="upper left", bbox_to_anchor=(1, 1), scatterpoints=1, fontsize=20)

        legend_handle_counter = 1
        for cat in bucket_unique_list:
            lgnd.legendHandles[legend_handle_counter]._sizes = [200]
            legend_handle_counter += 1

        xlabels = []
        for x in scatterplot.get_xticks():
            if x >= 1000000000:
                xlabels.append('{:,.1f}'.format(x / 1000000000) + ' B')
            elif x >= 1000000:
                xlabels.append('{:,.1f}'.format(x / 1000000) + ' M')
            elif x >= 1000:
                xlabels.append('{:,.1f}'.format(x / 1000) + ' k')
            elif x >= 0:
                xlabels.append('{:,.0f}'.format(x))
            else:
                xlabels.append(x)

        ylabels = []
        for y in scatterplot.get_yticks():
            if y >= 1000000000:
                ylabels.append('{:,.1f}'.format(y / 1000000000) + ' B')
            elif y >= 1000000:
                ylabels.append('{:,.1f}'.format(y / 1000000) + ' M')
            elif y >= 1000:
                ylabels.append('{:,.1f}'.format(y / 1000) + ' k')
            elif y >= 0:
                ylabels.append('{:,.0f}'.format(y))
            else:
                ylabels.append(y)

        scatterplot.set_xticklabels(xlabels)
        scatterplot.set_yticklabels(ylabels)

        scatterplot.set_title(Utils.human_readable_names(factor_x) + " vs " + Utils.human_readable_names(factor_y), fontsize=42)
        scatterplot.title.set_position([.5, 1.05])
        # sc_pl_user_buckets.xaxis.labelpad = 5

        # print(scatterplot._legend_data.keys())

        plt.tight_layout()
        fig = scatterplot.get_figure()
        return fig

    @staticmethod
    def bar_chart(df_with_selected_cols, groupBy_column, factor):
        group_by_bucket_mean = df_with_selected_cols.groupby(
            [Utils.human_readable_names(groupBy_column)]).mean()

        group_by_bucket_mean.sort_values(Utils.human_readable_names(factor), inplace=True)
        group_by_bucket_mean = group_by_bucket_mean.round(0).astype(int)

        # fitler x and y
        x = group_by_bucket_mean.index
        y = group_by_bucket_mean[Utils.human_readable_names(factor)]

        # ----------------------------------------------------------------------------------------------------
        # instanciate the figure
        fig = plt.figure(figsize=(20, 12))
        ax = fig.add_subplot()

        # ----------------------------------------------------------------------------------------------------
        # plot the data
        for x_, y_ in zip(x, y):
            # this is very cool, since we can pass a function to matplotlib
            # and it will plot the color based on the result of the evaluation
            ax.bar(x_, y_, color="red" if y_ < y.mean() else "green", alpha=0.3)

            # add some text
            ax.text(x_, y_ + 0.3, round(y_, 1), horizontalalignment='center')

        # rotate the x ticks 90 degrees
        #         ax.set_xticklabels(x, rotation=45)

        # add an y label
        ax.set_ylabel("Average " + Utils.human_readable_names(factor))

        # add an x label
        ax.set_xlabel(Utils.human_readable_names(groupBy_column))

        # set a title/
        ax_title = "Average " + Utils.human_readable_names(factor) + " filtered by " + Utils.human_readable_names(
            groupBy_column)
        ax.set_title(ax_title)

        ax.get_xaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())
        ax.get_yaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())

        #         ax.grid(b=True, which='major', color='w', linewidth=1.5)
        #         ax.grid(b=True, which='minor', color='w', linewidth=0.75)

        plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')

        plt.tight_layout()

        return fig

    @staticmethod
    def bar_chart_simple(df_with_selected_cols, groupBy_column_x, factor_y, horizontal_bar = False):
        # instanciate the figure
        fig = plt.figure(figsize=(20, 12))
        # ax = fig.add_subplot()

        if horizontal_bar == False:
            x_col = Utils.human_readable_names(groupBy_column_x)
            y_col = Utils.human_readable_names(factor_y)
        else:
            x_col = Utils.human_readable_names(factor_y)
            y_col = Utils.human_readable_names(groupBy_column_x)

        ax = sns.barplot(x=x_col, y=y_col, data=df_with_selected_cols)
        # ax.set_xticklabels(ax.get_xticklabels(),rotation=30)
        ax.get_xaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())
        ax.get_yaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())

        if horizontal_bar == False:
            ylabels = []
            for y in ax.get_yticks():
                if y >= 1000000000:
                    ylabels.append('{:,.1f}'.format(y / 1000000000) + ' B')
                elif y >= 1000000:
                    ylabels.append('{:,.1f}'.format(y / 1000000) + ' M')
                elif y >= 1000:
                    ylabels.append('{:,.1f}'.format(y / 1000) + ' k')
                elif y >= 0:
                    ylabels.append('{:,.0f}'.format(y))
                else:
                    ylabels.append(y)


            ax.set_yticklabels(ylabels)
        else:
            xlabels = []
            for x in ax.get_xticks():
                if x >= 1000000000:
                    xlabels.append('{:,.1f}'.format(x / 1000000000) + ' B')
                elif x >= 1000000:
                    xlabels.append('{:,.1f}'.format(x / 1000000) + ' M')
                elif x >= 1000:
                    xlabels.append('{:,.1f}'.format(x / 1000) + ' k')
                elif x >= 0:
                    xlabels.append('{:,.0f}'.format(x))
                else:
                    xlabels.append(x)

            ax.set_xticklabels(xlabels)

        if horizontal_bar == False:
            plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')

        plt.tight_layout()

        return fig

    @staticmethod
    def bar_chart_stacked(df_with_selected_cols, groupBy_column_x, factor_y, horizontal_bar=False, hue_column='Month_name'):
        fig = plt.figure(figsize=(20, 12))
        # ax = fig.add_subplot()

        if horizontal_bar == False:
            x_col = Utils.human_readable_names(groupBy_column_x)
            y_col = Utils.human_readable_names(factor_y)
        else:
            x_col = Utils.human_readable_names(factor_y)
            y_col = Utils.human_readable_names(groupBy_column_x)

        hue_column = Utils.human_readable_names(hue_column)

        ax = sns.barplot(x=x_col, y=y_col, data=df_with_selected_cols, hue=hue_column)
        # ax.set_xticklabels(ax.get_xticklabels(),rotation=30)
        ax.get_xaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())
        ax.get_yaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())

        lgnd = plt.legend(loc="upper left", bbox_to_anchor=(1, 1), scatterpoints=1, fontsize=20)

        if horizontal_bar == False:
            ylabels = []
            for y in ax.get_yticks():
                if y >= 1000000000:
                    ylabels.append('{:,.1f}'.format(y / 1000000000) + ' B')
                elif y >= 1000000:
                    ylabels.append('{:,.1f}'.format(y / 1000000) + ' M')
                elif y >= 1000:
                    ylabels.append('{:,.1f}'.format(y / 1000) + ' k')
                elif y >= 0:
                    ylabels.append('{:,.0f}'.format(y))
                else:
                    ylabels.append(y)

            ax.set_yticklabels(ylabels)
        else:
            xlabels = []
            for x in ax.get_xticks():
                if x >= 1000000000:
                    xlabels.append('{:,.1f}'.format(x / 1000000000) + ' B')
                elif x >= 1000000:
                    xlabels.append('{:,.1f}'.format(x / 1000000) + ' M')
                elif x >= 1000:
                    xlabels.append('{:,.1f}'.format(x / 1000) + ' k')
                elif x >= 0:
                    xlabels.append('{:,.0f}'.format(x))
                else:
                    xlabels.append(x)

            ax.set_xticklabels(xlabels)

        if horizontal_bar == False:
            plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')

        plt.tight_layout()

        return fig

    @staticmethod
    def violin_chart(df_with_selected_cols, groupBy_column, factor):
        scatterplot = sns.violinplot(x=Utils.human_readable_names(groupBy_column),
                                     y=Utils.human_readable_names(factor),
                                     data=df_with_selected_cols,
                                     scale='width',
                                     inner='quartile'
                                     )

        # ----------------------------------------------------------------------------------------------------
        # prettify the plot

        # instanciate the figure
        # fig = plt.figure(figsize=(20, 12))
        # ax = fig.add_subplot()

        # get the current figure
        ax = plt.gca()
        # get the xticks to iterate over
        xticks = ax.get_xticks()

        # iterate over every xtick and add a vertical line
        # to separate different classes
        for tick in xticks:
            ax.vlines(tick + 0.5, 0, np.max(df_with_selected_cols[Utils.human_readable_names(factor)]), color="grey",
                      alpha=.1)

        # rotate the x and y ticks
        ax.tick_params(axis='x', labelrotation=45, labelsize=20)
        ax.tick_params(axis='y', labelsize=20)

        ylabels = []
        for y in scatterplot.get_yticks():
            if y >= 1000000000:
                ylabels.append('{:,.1f}'.format(y / 1000000000) + ' B')
            elif y >= 1000000:
                ylabels.append('{:,.1f}'.format(y / 1000000) + ' M')
            elif y >= 1000:
                ylabels.append('{:,.1f}'.format(y / 1000) + ' k')
            elif y >= 0:
                ylabels.append('{:,.0f}'.format(y))
            else:
                ylabels.append(y)

        # scatterplot.set_xticklabels(xlabels)
        scatterplot.set_yticklabels(ylabels)

        # # add x and y label
        # ax.set_xlabel(human_readable_names("User_Bucket"), fontsize = 14)
        # ax.set_ylabel(human_readable_names("CPU_Sec"), fontsize = 14)

        # # set title
        # ax.set_title("Violinplot", fontsize = 14);

        # add an y label
        ax.set_ylabel("Sum of " + Utils.human_readable_names(factor))

        # add an x label
        ax.set_xlabel(Utils.human_readable_names(groupBy_column))

        # set a title/
        ax_title = "" + Utils.human_readable_names(factor) + " across different " + Utils.human_readable_names(groupBy_column)
        ax.set_title(ax_title)

        ax.get_xaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())
        ax.get_yaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())

        # ax.grid(b=True, which='major', color='w', linewidth=1.5)
        # ax.grid(b=True, which='minor', color='w', linewidth=0.75)

        plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')

        plt.tight_layout()

        fig = scatterplot.get_figure()

        return fig

    @staticmethod
    def stacked_histogram(df_with_selected_cols, groupBy_column, factor):
        gb_df_selected_cols_complete_data = df_with_selected_cols[
            [Utils.human_readable_names(groupBy_column), Utils.human_readable_names(factor)]].groupby(
            Utils.human_readable_names(groupBy_column))
        lx = []
        ln = []

        # handpicked colors
        # colors = ["#543005", "#8c510a", "#bf812d", "#80cdc1", "#35978f", "#01665e", "#003c30","#643005", "#9c510a", "#cf812d", "#90cdc1", "#45978f", "#11665e", "#203c30", "#303c30"]
        colors = ['#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4', '#46f0f0', '#f032e6', '#bcf60c',
                  '#fabebe', '#008080', '#e6beff', '#9a6324', '#fffac8', '#800000', '#aaffc3', '#808000', '#ffd8b1',
                  '#000075', '#808080', '#ffffff', '#000000']

        # iterate over very groupby group and
        # append their values as a list
        # THIS IS A CRUCIAL STEP
        for _, df_ in gb_df_selected_cols_complete_data:
            lx.append(df_[Utils.human_readable_names(factor)].values.tolist())
            ln.append(list(set(df_[Utils.human_readable_names(groupBy_column)].values.tolist()))[0])

        colors = colors[0:len(ln)]

        # ----------------------------------------------------------------------------------------------------
        # instanciate the figure
        fig = plt.figure(figsize=(20, 12))
        ax = fig.add_subplot()

        # ----------------------------------------------------------------------------------------------------
        # plot the data

        # hist returns a tuple of 3 values
        # let's unpack it
        n, bins, patches = ax.hist(lx, bins=50, stacked=True, density=False, color=colors)

        # ----------------------------------------------------------------------------------------------------
        # prettify the plot

        # change x lim
        # ax.set_ylim(0, 5)
        ax.set_yscale('log')

        # set the xticks to reflect every third value
        ax.set_xticks(bins[::3])

        xlabels = []
        for x in ax.get_xticks():
            if x >= 1000000000:
                xlabels.append('{:,.1f}'.format(x / 1000000000) + ' B')
            elif x >= 1000000:
                xlabels.append('{:,.1f}'.format(x / 1000000) + ' M')
            elif x >= 1000:
                xlabels.append('{:,.1f}'.format(x / 1000) + ' k')
            elif x >= 0:
                xlabels.append('{:,.0f}'.format(x))
            else:
                xlabels.append(x)

        ylabels = []
        for y in ax.get_yticks():
            if y >= 1000000000:
                ylabels.append('{:,.1f}'.format(y / 1000000000) + ' B')
            elif y >= 1000000:
                ylabels.append('{:,.1f}'.format(y / 1000000) + ' M')
            elif y >= 1000:
                ylabels.append('{:,.1f}'.format(y / 1000) + ' k')
            elif y >= 0:
                ylabels.append('{:,.0f}'.format(y))
            else:
                ylabels.append(y)

        ax.set_xticklabels(xlabels)
        ax.set_yticklabels(ylabels)

        # set a title
        ax_title = "Stacked Histogram of " + Utils.human_readable_names(factor) + " colored by " + Utils.human_readable_names(
            groupBy_column)
        ax.set_title(ax_title)

        plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')

        # add a custom legend wit class and color
        # you have to pass a dict
        ax.legend({class_: color for class_, color in zip(ln, colors)}, loc="upper left", bbox_to_anchor=(1, 1),
                  scatterpoints=1, fontsize=20)

        # set the y label
        ax.set_ylabel("Frequency");

        # set the x label
        ax.set_xlabel(Utils.human_readable_names(factor))

        plt.tight_layout()

        return fig

    @staticmethod
    def heatmap(data, row_labels, col_labels, ax=None,
                cbar_kw={}, cbarlabel="", **kwargs):
        """
        Create a heatmap from a numpy array and two lists of labels.

        Parameters
        ----------
        data
            A 2D numpy array of shape (N, M).
        row_labels
            A list or array of length N with the labels for the rows.
        col_labels
            A list or array of length M with the labels for the columns.
        ax
            A `matplotlib.axes.Axes` instance to which the heatmap is plotted.  If
            not provided, use current axes or create a new one.  Optional.
        cbar_kw
            A dictionary with arguments to `matplotlib.Figure.colorbar`.  Optional.
        cbarlabel
            The label for the colorbar.  Optional.
        **kwargs
            All other arguments are forwarded to `imshow`.
        """

        if not ax:
            ax = plt.gca()

        # Plot the heatmap
        im = ax.imshow(data, **kwargs)

        # Create colorbar
        #     cbar = ax.figure.colorbar(im, ax=ax, **cbar_kw)
        #     cbar.ax.set_ylabel(cbarlabel, rotation=0, va="bottom")

        # We want to show all ticks...
        ax.set_xticks(np.arange(data.shape[1]))
        ax.set_yticks(np.arange(data.shape[0]))
        # ... and label them with the respective list entries.
        ax.set_xticklabels(col_labels)
        ax.set_yticklabels(row_labels)

        # Let the horizontal axes labeling appear on top.
        ax.tick_params(top=True, bottom=False,
                       labeltop=True, labelbottom=False)

        # Rotate the tick labels and set their alignment.
        plt.setp(ax.get_xticklabels(), rotation=0, ha="right",
                 rotation_mode="anchor")

        plt.setp(ax.get_yticklabels(), rotation=0, ha="right",
                 rotation_mode="anchor")

        # Turn spines off and create white grid.
        for edge, spine in ax.spines.items():
            spine.set_visible(False)

        ax.set_xticks(np.arange(data.shape[1] + 1) - .5, minor=True)
        ax.set_yticks(np.arange(data.shape[0] + 1) - .5, minor=True)
        ax.grid(which="minor", color="w", linestyle='-', linewidth=3)
        ax.tick_params(which="minor", bottom=False, left=False)

        #     return im, cbar
        return im

    @staticmethod
    def annotate_heatmap(im, data=None, valfmt="{x:.2f}",
                         textcolors=["black", "white"],
                         threshold=None, **textkw):
        """
        A function to annotate a heatmap.

        Parameters
        ----------
        im
            The AxesImage to be labeled.
        data
            Data used to annotate.  If None, the image's data is used.  Optional.
        valfmt
            The format of the annotations inside the heatmap.  This should either
            use the string format method, e.g. "$ {x:.2f}", or be a
            `matplotlib.ticker.Formatter`.  Optional.
        textcolors
            A list or array of two color specifications.  The first is used for
            values below a threshold, the second for those above.  Optional.
        threshold
            Value in data units according to which the colors from textcolors are
            applied.  If None (the default) uses the middle of the colormap as
            separation.  Optional.
        **kwargs
            All other arguments are forwarded to each call to `text` used to create
            the text labels.
        """

        if not isinstance(data, (list, np.ndarray)):
            data = im.get_array()

        # Normalize the threshold to the images color range.
        if threshold is not None:
            threshold = im.norm(threshold)
        else:
            threshold = im.norm(data.max()) / 2.

        # Set default alignment to center, but allow it to be
        # overwritten by textkw.
        kw = dict(horizontalalignment="center",
                  verticalalignment="center")
        kw.update(textkw)

        # Get the formatter in case a string is supplied
        if isinstance(valfmt, str):
            valfmt = matplotlib.ticker.StrMethodFormatter(valfmt)

        # Loop over the data and create a `Text` for each "pixel".
        # Change the text's color depending on the data.
        texts = []
        for i in range(data.shape[0]):
            for j in range(data.shape[1]):
                kw.update(color=textcolors[int(im.norm(data[i, j]) > threshold)])
                text = im.axes.text(j, i, valfmt(data[i, j], None), **kw)
                type(text)
                texts.append(text)

        return texts

    @staticmethod
    def human_readable_heatmap_title(input_text):
        output_text = ''
        if input_text == 'Concurrency, Average':
            output_text = 'Concurrency, Average'
        elif input_text == 'Concurrency_80Pctl':
            output_text = 'Concurrency, 80th Percentile'
        elif input_text == 'Concurrency_95Pctl':
            output_text = 'Concurrency, 95th Percentile'
        elif input_text == 'Concurrency_Peak':
            output_text = 'Concurrency, Peak'
        else:
            output_text = 'Concurrency'

        return output_text

    @staticmethod
    def heatmap_chart(df_concurrency, concurrency_col):
        df = df_concurrency[['LogDate', 'LogHour', concurrency_col]]

        # df['LogDate'] = df['LogDate'].dt.date

        df_pivot = df.pivot(index='LogDate', columns='LogHour', values=concurrency_col)

        log_date = df_pivot.index

        log_hour = df_pivot.columns

        Concurrency_values = df_pivot.values

        fig, ax = plt.subplots(figsize=(30, 30))
        # plt.figure(figsize=(1,1))

        # SMALL_SIZE = 20
        # MEDIUM_SIZE = 20
        # BIGGER_SIZE = 20
        #
        # plt.rc('font', size=SMALL_SIZE)  # controls default text sizes
        # plt.rc('axes', titlesize=SMALL_SIZE)  # fontsize of the axes title
        # plt.rc('axes', labelsize=MEDIUM_SIZE)  # fontsize of the x and y labels
        # plt.rc('xtick', labelsize=SMALL_SIZE)  # fontsize of the tick labels
        # plt.rc('ytick', labelsize=SMALL_SIZE)  # fontsize of the tick labels
        # plt.rc('legend', fontsize=40)  # legend fontsize
        # plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

        Utils.set_plot_sizes(plt, small=20, medium=20, big=20)

        im = Utils.heatmap(Concurrency_values, log_date, log_hour, ax=ax,
                     cmap="YlOrRd", cbarlabel=concurrency_col)

        # legend
        # cbar.set_label('Concurrency Peak', rotation=270, size=35)

        # create an axes on the right side of ax. The width of cax will be 5%
        # of ax and the padding between cax and ax will be fixed at 0.05 inch.
        divider = make_axes_locatable(ax)
        cax = divider.append_axes("right", size="5%", pad=0.75)
        # cax.set_label('Concurrency Peak')

        plt.colorbar(im, cax=cax)
        # plt.title("Concurrency_Peak")
        #     ax.set_ylabel('Date',size = 30,labelpad =10)
        ax.set_xlabel('Hour', size=30, labelpad=10)
        ax.xaxis.set_label_position('top')
        size = fig.get_size_inches() * fig.dpi
        #     ax.set_title('Heatmap showing ' + concurrency_list[i], y=-0.05,size=36, pad=20)
        # ax_title = Utils.human_readable_heatmap_title(concurrency_col)
        ax_title = concurrency_col
        ax.set_title(ax_title, loc='left', pad=5, size=36)

        texts = Utils.annotate_heatmap(im, valfmt="{x}")

        plt.tight_layout()

        return fig

    @staticmethod
    def comparative_line_trend_graph(df_daily_max_modified, date_col, parameter_col, value_col):
        fig, ax = plt.subplots(figsize=(30, 20))
        g = sns.lineplot(x=df_daily_max_modified[date_col], y=value_col, data=df_daily_max_modified, hue=parameter_col,
                         sort=True, linewidth=3)
        x_dates = df_daily_max_modified[date_col].sort_values().unique()
        plt.xticks(plt.xticks()[0], x_dates, rotation=30)

        ax.xaxis.set_major_locator(ticker.MultipleLocator(1))

        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles=handles[1:], labels=labels[1:], loc="upper left", bbox_to_anchor=(1, 1), scatterpoints=1,
                  fontsize=20)

        ax.set_title('Comparative Line Trend Graph', y=-0.15, size=36, pad=20)

        # ax.legend(loc="upper left",bbox_to_anchor=(1, 1), scatterpoints=1, fontsize=20)

        plt.tight_layout()

        return fig

    @staticmethod
    def weekly_mean_comparative_line_trend_graph(df_weekly_mean_modified, date_col, parameter_col, value_col):

        fig, ax = plt.subplots(figsize=(30, 20))
        g = sns.lineplot(x=df_weekly_mean_modified[date_col], y=value_col, data=df_weekly_mean_modified, hue=parameter_col,
                         sort=True, linewidth=3)
        x_dates = df_weekly_mean_modified[date_col].unique()
        plt.xticks(plt.xticks()[0], x_dates, rotation=30)

        ax.xaxis.set_major_locator(ticker.MultipleLocator(7))

        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles=handles[1:], labels=labels[1:])

        ax.set_title('Weekly Comparative Line Trend Graph', y=-0.15, size=36, pad=20)

        lgnd = plt.legend(loc="upper left", bbox_to_anchor=(1, 1), scatterpoints=1, fontsize=20)

        plt.tight_layout()

        return fig

    @staticmethod
    def weekday_comparative_line_trend_graph(df_daily_mean_modified, weekday_col, parameter_col, value_col):
        fig, ax = plt.subplots(figsize=(30, 20))
        g = sns.lineplot(x=df_daily_mean_modified[weekday_col], y=value_col, data=df_daily_mean_modified,
                         hue=parameter_col, sort=True, linewidth=3)
        x_dates = df_daily_mean_modified[weekday_col].unique()
        plt.xticks(plt.xticks()[0], x_dates, rotation=30)

        ax.xaxis.set_major_locator(ticker.MultipleLocator(1))

        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles=handles[1:], labels=labels[1:], loc="upper left", bbox_to_anchor=(1, 1), scatterpoints=1,
                  fontsize=20)

        ax.set_title('Week-day Comparative Line Trend Graph', y=-0.15, size=36, pad=20)

        plt.tight_layout()

        return fig


    @staticmethod
    def box_plot(df, data_columns_list, x_axis, y_axis):
        fig, axes = plt.subplots(len(data_columns_list), 1, figsize=(11, 10), sharex=True)
        for name, ax in zip(data_columns_list, axes):
            sns.boxplot(data=df, x=x_axis, y=name, ax=ax)
            ax.set_ylabel(y_axis)
            ax.set_title(name)
            # Remove the automatic x-axis label from all but the bottom subplot
            if ax != axes[-1]:
                ax.set_xlabel('')

        plt.tight_layout()

        return fig

    @staticmethod
    def slope_chart(df_daily_mean_modified, user_category_column, feature_column, month_1, month_2,
                    month_column='Month name', ):

        user_category_column = Utils.human_readable_names(user_category_column)
        feature_column = Utils.human_readable_names(feature_column)

        df_input_df = df_daily_mean_modified[[month_column, user_category_column, feature_column]]

        df_input_df_pivot = df_input_df.pivot_table(feature_column, [user_category_column], 'Month name')
        df_input_df_pivot = df_input_df_pivot.fillna(0)

        #     unique_month_list = list(set(list(df_input_df['Month name'])))

        #     unique_month_list = sorted(unique_month_list, key=lambda m: dt.strptime(m, "%B"))

        #     print('unique_month_list: ', unique_month_list)

        #     i = 0
        #     while i < len(unique_month_list) - 1:

        #         print('i: ', i)
        #         print('unique_month_list[i]: ',unique_month_list[i] )

        #         month_1 = unique_month_list[i]
        #         month_2 = unique_month_list[i+1]

        #         print('month_1: ', month_1)
        #         print('month_2: ', month_2)

        #     df_input_df_pivot = df_input_df_pivot.loc[df_input_df_pivot['Month name'].isin([])]

        month_1 = str(month_1).upper()
        month_2 = str(month_2).upper()

        #     print(df_input_df_pivot)

        df_month_1_rows = df_input_df.loc[df_input_df['Month name'] == month_1]
        df_month_1_rows_len = len(df_month_1_rows)

        df_month_2_rows = df_input_df.loc[df_input_df['Month name'] == month_2]
        df_month_2_rows_len = len(df_month_2_rows)

        if df_month_1_rows_len > 0 and df_month_2_rows_len > 0:

            df_input_df_pivot_1M_above = df_input_df_pivot.loc[
                (df_input_df_pivot[month_1] >= 1000000) & (df_input_df_pivot[month_2] >= 1000000)]

            df_input_df_pivot_1M_above["color"] = df_input_df_pivot_1M_above.apply(
                lambda row: "green" if row[month_2] >= row[month_1] else "red", axis=1)

            # ----------------------------------------------------------------------------------------------------
            # instanciate the figure
            fig = plt.figure(figsize=(12, 15))
            ax = fig.add_subplot()

            # ----------------------------------------------------------------------------------------------------
            # plot the data
            for cont in df_input_df_pivot_1M_above.index:
                # prepare the data for plotting
                # extract each point and the color
                x_start = df_input_df_pivot_1M_above.columns[1]
                x_finish = df_input_df_pivot_1M_above.columns[2]
                y_start = df_input_df_pivot_1M_above[df_input_df_pivot_1M_above.index == cont][month_1]
                y_finish = df_input_df_pivot_1M_above[df_input_df_pivot_1M_above.index == cont][month_2]
                color = df_input_df_pivot_1M_above[df_input_df_pivot_1M_above.index == cont]["color"]

                # plot eac point
                ax.scatter(x_start, y_start, color=color, s=200)
                ax.scatter(x_finish, y_finish, color=color, s=200 * (y_finish / y_start))

                # connect the starting point and the ending point with a line
                # check the bouns section for more
                ax.plot([x_start, x_finish], [float(y_start), float(y_finish)], linestyle="-", color=color.values[0])

                # annotate the value for each continent
                ax.text(ax.get_xlim()[0] - 0.05, y_start, r'{}: {:,.2f}M'.format(cont, int(y_start) / 1000000),
                        horizontalalignment='right', verticalalignment='center', fontdict={'size': 18})
                ax.text(ax.get_xlim()[1] + 0.05, y_finish, r'{}: {:,.2f}M'.format(cont, int(y_finish) / 1000000),
                        horizontalalignment='left', verticalalignment='center', fontdict={'size': 18})

            #     ax.text(ax.get_xlim()[0] - 0.05, y_start, r'{}:{}'.format(cont, y_start_plot), horizontalalignment = 'right', verticalalignment = 'center', fontdict = {'size':18})
            #     ax.text(ax.get_xlim()[1] + 0.05, y_finish, r'{}:{}'.format(cont, y_finish_plot), horizontalalignment = 'left', verticalalignment = 'center', fontdict = {'size':18})

            # ----------------------------------------------------------------------------------------------------
            # prettify the plot

            # get the x and y limits
            x_lims = ax.get_xlim()
            y_lims = ax.get_ylim()

            # change the x and y limits programmaticaly
            ax.set_xlim(x_lims[0] - 1, x_lims[1] + 1);

            # add 2 vertical lines
            ax.vlines(x_start, 0, y_lims[1], color="black", alpha=0.3, lw=0.7)
            ax.vlines(x_finish, 0, y_lims[1], color="black", alpha=0.3, lw=0.7)

            # for each vertical line, add text: BEFORE and AFTER to help understand the plot
            ax.text(x_lims[0], y_lims[1], "BEFORE", horizontalalignment='right', verticalalignment='center')
            ax.text(x_lims[1], y_lims[1], "AFTER", horizontalalignment='left', verticalalignment='center')

            # set and x and y label
            ax.set_xlabel("Months")
            ax.set_ylabel("CPU Usage")

            # add a title
            ax.set_title("CPU Usage - " + month_1 + " vs " + month_2)

            ylabels = []
            for y in ax.get_yticks():
                if y >= 1000000000:
                    ylabels.append('{:,.1f}'.format(y / 1000000000) + ' B')
                elif y >= 1000000:
                    ylabels.append('{:,.1f}'.format(y / 1000000) + ' M')
                elif y >= 1000:
                    ylabels.append('{:,.1f}'.format(y / 1000) + ' k')
                elif y >= 0:
                    ylabels.append('{:,.0f}'.format(y))
                else:
                    ylabels.append(y)

            ax.set_xticklabels([month_1, month_2])
            ax.set_yticklabels(ylabels)

            # ax.set_yscale('log')

            # remove all the spines of the axes
            ax.spines["left"].set_color("None")
            ax.spines["right"].set_color("None")
            ax.spines["top"].set_color("None")
            ax.spines["bottom"].set_color("None")

            #     i += 1

            plt.tight_layout()

            return fig

    @staticmethod
    def pie_chart(df_with_selected_cols, groupBy_column, factor, donut=False):

        df = df_with_selected_cols

        df = df[[groupBy_column, factor]]
        df = df.groupby([groupBy_column]).sum()
        df[groupBy_column] = df.index

        group_by_column = groupBy_column
        factor = factor

        labels = list(df[group_by_column])
        sizes = list(df[factor])

        colors = ['#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4', '#46f0f0', '#f032e6', '#bcf60c',
                  '#fabebe', '#008080', '#e6beff', '#9a6324', '#fffac8', '#800000', '#aaffc3', '#808000', '#ffd8b1',
                  '#000075', '#808080', '#ffffff', '#000000']
        # explsion
        explode = tuple([0.01] * len(list(df['User Department'])))

        # add colors
        # colors = ['#ff9999','#66b3ff','#99ff99','#ffcc99']

        fig = plt.figure(figsize=(20, 12))
        ax = fig.add_subplot()

        #     fig1, ax1 = plt.subplots()
        ax.pie(sizes, explode=explode, colors=colors,
               shadow=True, startangle=90, textprops={'fontsize': 24})

        if donut == True:
            # draw circle
            centre_circle = plt.Circle((0, 0), 0.70, fc='white')
            # fig = plt.figure(figsize=(20, 12))
            fig = plt.gcf()
            fig.gca().add_artist(centre_circle)
            # Equal aspect ratio ensures that pie is drawn as a circle
            ax.axis('equal')

            # Equal aspect ratio ensures that pie is drawn as a circle
        ax.axis('equal')

        lgnd = plt.legend(loc="upper left", bbox_to_anchor=(1, 1), scatterpoints=1, fontsize=20, labels=labels)

        plt.tight_layout()

        return fig

    @staticmethod
    def bar_compare_monthwise_weekwise(df_with_selected_cols, groupBy_column, groupBy_column_value, factor,
                                       fig_save_path):
        plt.figure(figsize=(20, 12))

        df_with_selected_cols = df_with_selected_cols[
            df_with_selected_cols[groupBy_column] == groupBy_column_value.upper()]

        g = sns.FacetGrid(df_with_selected_cols, col="Month name", size=10, height=4, aspect=.6)
        g.map(sns.barplot, "Week no", factor)

        plt.savefig(r'' + fig_save_path + '\\top_users.bar_compare_monthwise_weekwise.png',
                    dpi=100, bbox_inches='tight')

    @staticmethod
    def process_category_content(df, user_category_list):
        for user_category in user_category_list:
            df[user_category] = df[user_category].str.upper()
            df[user_category] = df[user_category].apply(lambda x: Utils.human_readable_names(x))
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
