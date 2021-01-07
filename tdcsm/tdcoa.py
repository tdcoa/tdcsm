import datetime as dt
import errno
import json
import os
import ntpath
import re
import shutil
import pandas as pd
import requests
import yaml
import csv
import sys
import subprocess
from teradatasql import OperationalError
from .dbutil import df_to_sql, sql_to_df
import webbrowser


import tdcsm
from pathlib import Path
from .utils import Utils  # includes Logger class


# todo create docstring for all methods


class tdcoa:

    # paths
    approot = '.'
    configpath = ''
    secretpath = ''
    systemspath = ''
    filesetpath = ''
    outputpath = ''
    version = "0.4.1.3"
    skip_dbs = False    # skip ALL dbs connections / executions
    manual_run = False  # skip dbs executions in execute_run() but not upload_to_transcend()
                        # also skips /*{{save:}}*/ special command

    # dictionaries
    secrets = {}
    filesets = {}
    files = {}
    systems = {}
    folders = {}
    substitutions = {}
    transcend = {}
    settings = {}

    def __init__(self, approot='.', printlog=True, config='config.yaml', secrets='secrets.yaml', filesets='filesets.yaml', systems='source_systems.yaml', refresh_defaults=False, skip_dbs=False):
        self.bufferlog = True
        self.printlog = printlog
        self.approot = os.path.join('.', approot)
        self.configpath = os.path.join(self.approot, config)
        self.secretpath = os.path.join(self.approot, secrets)
        self.systemspath = os.path.join(self.approot, systems)
        self.refresh_defaults = refresh_defaults

        self.utils = Utils(self.version)  # utilities class. inherits Logger class

        self.utils.log('tdcoa started', header=True)
        self.utils.log('time', str(dt.datetime.now()))
        self.utils.log('app root', self.approot)
        self.utils.log('config file', self.configpath)
        self.utils.log('source systems file', self.systemspath)
        self.utils.log('secrets file', self.secretpath)
        self.utils.log('tdcoa version', self.version)

        self.unique_id = dt.datetime.now().strftime("%m%d%Y%H%M")  # unique id to append to table
        self.motd_url = 'file://' + os.path.abspath(os.path.join(self.approot, 'motd.html'))

        # filesets.yaml is validated at download time
        self.reload_config(skip_dbs=skip_dbs)

    def add_filesets_to_systems(self):
        # read in fileset.yaml file to dictionary:
        self.utils.log('adding all filesets to all systems (in memory, not disk)')
        for sysname, sysobject in self.systems.items():  # iterate systems object...
            i = 0
            if 'filesets' not in sysobject or type(sysobject['filesets']) != dict: sysobject['filesets']={}
            for fsname, fsobject in self.filesets.items(): # iterate fileset master yaml...
                if fsname not in sysobject['filesets']:
                    sysobject['filesets'][fsname] = {'active':False}  # add if missing
                    i+=1
            self.utils.log('added %i new filesets to' %i, sysname)

    def reload_config(self, config='', secrets='', systems='', refresh_defaults=False, skip_dbs=False, skip_git=False):
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

        configpath = self.configpath if config == ''    else os.path.join(self.approot,  config)
        secretpath = self.secretpath if secrets == ''   else os.path.join(self.approot, secrets)
        systemspath = self.systemspath if systems == '' else os.path.join(self.approot, systems)
        self.refresh_defaults = refresh_defaults

        self.utils.bufferlogs = True
        self.utils.log('reload_config started', header=True)
        self.utils.log('time', str(dt.datetime.now()))
        self.utils.log('tdcoa version', self.version)

        # ensure all required configuration files are present:
        self.utils.log('checking core config files')
        # these all sit in approot by default, so these are both filenames AND relative paths:
        startfiles = ['secrets.yaml','config.yaml','source_systems.yaml','run_gui.py','run_gui']
        startfilecontent = ''
        for startfile in startfiles:
            startfile_src = os.path.join(os.path.dirname(tdcsm.__file__), startfile)
            startfile_ovr = os.path.join(self.approot,'0_override', startfile)


            # honor parameter overrides:
            if   startfile == 'secrets.yaml': startfile_dst = secretpath
            elif startfile == 'config.yaml': startfile_dst = configpath
            elif startfile == 'source_systems.yaml': startfile_dst = systemspath
            else: startfile_dst = os.path.join(self.approot, startfile)

            # remove files if "refresh defaults" is requested via __init__ param
            if self.refresh_defaults and os.path.isfile(startfile_dst) and startfiles != 'secrets.yaml':
                os.remove(startfile_dst)

            # if file is missing:
            if not os.path.isfile(startfile_dst):
                self.utils.log(' MISSING FILE', startfile)
                # check if the file is in the 0_override folder... if so, use that:
                if os.path.isfile(startfile_ovr):
                    self.utils.log('   Adding from 0_override')
                    with open(startfile_ovr) as f1:
                        startfilecontent = f1.read()
                # if no override, pull from package directory
                elif os.path.isfile(startfile_src):
                    self.utils.log('   Adding from installed package')
                    with open(startfile_src) as f1:
                        startfilecontent = f1.read()
                else:  # this is just-in-case, until I can be sure above logic is working.
                    if startfile == 'coa.py': startfilecontent = 'from tdcsm.tdgui import coa\nc=coa()'
                    if startfile == 'secrets.yaml': startfilecontent = 'secrets:\n  td_quicklook: "qlikuserid"\n  td_password:  "qlikpassword"'
                    if startfile == 'config.yaml': startfilecontent = self.yaml_config()
                    if startfile == 'source_systems.yaml': startfilecontent = self.yaml_systems()
                    self.utils.log('   Adding from internal string (should not happen)')
                with open(startfile_dst, 'w') as f2:
                    f2.write(startfilecontent)

        # load secrets.yaml
        with open(secretpath, 'r') as fh:
            secretstr = fh.read()
        self.secrets = yaml.load(secretstr, Loader=yaml.FullLoader)['secrets']
        self.utils.secrets = self.secrets  # update secrets attribute in logger

        # load config.yaml
        with open(configpath, 'r') as fh:
            configstr = fh.read()
        configyaml = yaml.load(configstr, Loader=yaml.FullLoader)
        configstr = self.utils.substitute(configstr, self.secrets, 'secrets')
        configstr = self.utils.substitute(configstr, configyaml['substitutions'], 'config:substitutions')
        configstr = self.utils.substitute(configstr, configyaml['folders'], 'config:folders')
        configstr = self.utils.substitute(configstr, configyaml['settings'], 'config:settings')
        configstr = self.utils.substitute(configstr, configyaml['transcend'], 'config:transcend')
        configyaml = yaml.load(configstr, Loader=yaml.FullLoader)

        # load substitutions
        self.utils.log('loading dictionary', 'substitutions')
        self.substitutions = configyaml['substitutions']
        with open(systemspath, 'r') as fh:
            systemsstr = fh.read()
        systemsstr = self.utils.substitute(systemsstr, self.secrets, 'secrets')
        systemsstr = self.utils.substitute(systemsstr, self.substitutions, 'systems:substitutions')
        systemsyaml = yaml.load(systemsstr, Loader=yaml.FullLoader)

        # check and set Transcend connection information
        self.utils.log('loading dictionary', 'transcend')
        self.transcend = configyaml['transcend']
        self.utils.check_setting(self.transcend,
                           required_item_list=['username', 'password', 'host', 'logmech', 'db_coa', 'db_region', 'db_stg'],
                           defaults=['{td_quicklook}', '{td_password}', 'tdprdcop3.td.teradata.com', 'TD2',
                                     'adlste_coa', 'adlste_westcomm', 'adlste_coa_stg'])
        self.transcend['connectionstring'] = 'teradatasql://%s:%s@%s/?logmech=%s' % (
            self.transcend['username'],
            self.transcend['password'],
            self.transcend['host'],
            self.transcend['logmech'])

        # check and set required Folders
        self.utils.log('loading dictionary', 'folders')
        self.folders = configyaml['folders']
        self.utils.check_setting(self.folders, required_item_list=['download', 'sql', 'run', 'output', 'override'],
                           defaults=['1_download', '2_sql_store', '3_ready_to_run', '4_output', '0_override'])

        # check and set required Settings
        self.utils.log('loading dictionary', 'settings')
        self.settings = configyaml['settings']
        if skip_dbs: self.settings['skip_dbs'] = 'True'
        self.utils.check_setting(self.settings,
                           required_item_list=['githost', 'gitfileset', 'gitmotd', 'localfilesets',
                                               'run_non_fileset_folders', 'gui_show_dev_filesets',
                                                'skip_dbs'],
                           defaults=['https://raw.githubusercontent.com/tdcoa/sql/master/',
                                     'filesets.yaml',
                                     'motd.txt',
                                     '{download}/filesets.yaml',
                                     'True',
                                     'False',
                                     'python','False'])

        if self.utils.validate_boolean(self.settings['skip_dbs'],'bool'):
            self.utils.log('SKIP_DBS == TRUE, emulating all database connections', warning=True)

        self.filesetpath = self.settings['localfilesets']

        # create missing folders
        for nm, subfo in self.folders.items():
            fopath = os.path.join(self.approot, subfo)
            # core folders
            if not os.path.exists(fopath):
                self.utils.log('creating missing folder', fopath)
                os.mkdir(fopath)

        # unbuffer logs once we have a valid "run" folder
        self.utils.logpath = os.path.join(self.approot, self.folders['run'], 'runlog.txt')
        # if os.path.isfile(self.logpath): os.remove(self.logpath)
        self.utils.bufferlogs = False
        self.utils.log('unbuffering log to "run" folder')

        # setup filesets.yaml
        if 'localfilesets' not in self.settings:
            self.settings['localfilesets'] = os.path.join(self.folders['download'], 'filesets.yaml')
        self.filesetpath = os.path.join(self.approot, self.settings['localfilesets'])

        githost = self.settings['githost']
        if githost[-1:] != '/':
            githost = githost + '/'
        self.utils.log('githost', githost)
        giturl = githost + self.settings['gitfileset']
        self.utils.log('downloading "filesets.yaml" from github')
        self.utils.log('  requesting url', giturl)

        # add skip_git check from config.settings
        if 'skip_git' in self.settings and self.settings['skip_git']=='True':
            skip_git = True
            self.utils.log('setting found in config.yaml', 'skip_git: "True"')

        # skip git download if requested
        if skip_git:
            self.utils.log('filesets.yaml download skipped, using cached local copy', warning=True)
        else:
            try:
                filecontent = requests.get(giturl).content.decode('utf-8')
                savepath = os.path.join(self.approot, self.settings['localfilesets'])
                self.utils.log('saving filesets.yaml', savepath)
                with open(savepath, 'w') as fh:
                    fh.write(filecontent)
                    self.utils.log('filesets.yaml saved')
            except Exception as ex:
                self.utils.log('filesets.yaml could not be downloaded, using cached local copy', warning=True)
                # self.utils.log('Error: %s' %str(ex), indent=2)

        # load filesets dictionary (active only)
        self.utils.log('loading dictionary', 'filesets (active only)')
        if not os.path.isfile(self.filesetpath):
            self.utils.log('the filesets.yaml file is not found at the expected location: \n\t%s\n' %self.filesetpath, error=True)
            self.utils.log('this might be caused by a network disallowing downloads from GitHub.com, or being offline entirely')
            self.utils.log('downloading the filesets.yaml is a HARD REQUIREMENT, as that file defines all filesets in real-time')
            self.utils.log('\nRecommended Actions:')
            self.utils.log('  1) manually download the fileset definition file here:\n    https://raw.githubusercontent.com/tdcoa/sql/master/filesets/filesets.yaml')
            self.utils.log('  2) save to your "1_download" folder as "filesets.yaml"')
            self.utils.log('  3) to prevent waiting for the connection timeout, open your "config.yaml" file and in the "settings" section add:\n    skip_git: "True" # (remember to match the indent)')
            self.utils.log('  4) click the "Reload Configs" button')
            self.utils.log('  5) plan to manually refresh the filesets.yaml file periodically\n\n')
            self.utils.log('Finally Note: all other fileset collateral is likewise downloaded from github, so you are likely to hit similar errors during the Download phase.\n\n')

        else:
            with open(self.filesetpath, 'r') as fh:
                filesetstr = fh.read()
            filesetyaml = yaml.load(filesetstr, Loader=yaml.FullLoader)
            if not filesetyaml:
                msg = 'filesets.yaml appears empty, please make sure it contains valid yaml configuration.\n'
                msg = msg + 'when in doubt: delete the existing filesets.yaml file from the "download" folder,\n'
                msg = msg + 'and run the process again.  When missing, it will create a default file of\n'
                msg = msg + 'the correct format.  When executing the "download_sql" command, the program\n'
                msg = msg + 'will also re-download the latest filesets.yaml from github.'
                self.utils.log(msg, error=True)
                raise IOError(msg)
            for setname, setobject in filesetyaml.items():
                if str(setobject['active']).strip().lower() == 'true':
                    self.filesets.update({setname: setobject})

        # load systems (no longer active only)
        self.utils.log('loading system dictionaries')
        for sysname, sysobject in systemsyaml['systems'].items():
            # if self.utils.dict_active(sysobject, sysname): #<--- no more, really messed up lots of UI work before
            self.systems[sysname] = sysobject
            self.utils.log('LOADING SYSTEM', sysname)

            # todo add default dbsversion and collection
            self.utils.check_setting(self.systems[sysname],
                               required_item_list=['active', 'siteid', 'use', 'host',
                                                   'username', 'password',
                                                   'logmech', 'driver', 'encryption','dbsversion','collection',
                                                   'filesets'],
                               defaults=['True', 'siteid123', 'unknown', 'customer.host.missing.com',
                                         'username_missing', 'password_missing',
                                         'TD2', 'sqlalchemy', 'False','16.20','pdcr',
                                         {}])

            if sysobject['logmech'].strip() == '':
                logmech = ''
            else:
                logmech = '/?logmech=%s' % sysobject['logmech']
            sysobject['connectionstring'] = 'teradatasql://%s:%s@%s%s' % (sysobject['username'],
                                                                          sysobject['password'],
                                                                          sysobject['host'],
                                                                          logmech)

        # add filesets to systems, in memory only:
        self.add_filesets_to_systems()

        # not sure this is ever explicitly re-set
        self.configpath = configpath
        self.secretpath = secretpath
        self.systemspath = systemspath

        self.bteq_delim = '|~|'
        bp=[]
        bp.append('---------------------------------------------------------------------')
        bp.append('--- add credentials below, all else should run & export automatically')
        bp.append('.SET MAXERROR 1')
        bp.append('.logmech TD2 --- example options: NTLM, KRB5, LDAP, TD2')
        bp.append('.LOGON host/username,password')
        bp.append('.TITLEDASHES off')
        bp.append(".SEPARATOR '%s'" %self.bteq_delim)
        bp.append(".SET NULL AS ''")
        bp.append('.WIDTH 1048575')
        bp.append('.RETLIMIT * *')
        bp.append('---------------------------------------------------------------------')
        self.bteq_prefix = '\n'.join(bp)

        self.substitutions['YYYYMMDD'] = dt.datetime.today().strftime('%Y%m%d')
        self.substitutions['YYYYMM'] = dt.datetime.today().strftime('%Y%m')

        self.utils.log('done!')
        self.utils.log('time', str(dt.datetime.now()))

    def download_files(self, motd=True):
        self.utils.log('download_files started', header=True)
        self.utils.log('time', str(dt.datetime.now()))
        githost = self.settings['githost']
        if githost[-1:] != '/':
            githost = githost + '/'
        self.utils.log('githost', githost)

        filesetcontent = ''

        # download any control files first (motd.html, etc.)
        # motd
        giturl = githost + self.settings['gitmotd']
        self.utils.log('downloading "motd.html" from github')
        self.utils.log('  requesting url', giturl)
        filecontent = requests.get(giturl).text
        with open(os.path.join(self.approot, 'motd.html'), 'w') as fh:
            fh.write(filecontent)

        # open motd.html in browser
        self.motd_url = 'file://' + os.path.abspath(os.path.join(self.approot, 'motd.html'))
        if motd: webbrowser.open(self.motd_url)

        # delete all pre-existing download folders
        # Commented the below code, in order to make the script download only if the files doesn't exist
        self.utils.recursively_delete_subfolders(os.path.join(self.approot, self.folders['download']))

        # set proper githost for filesets
        githost = githost + 'filesets/'

        # iterate all active systems.filesets:
        for sysname, sysobject in self.systems.items():
            if self.utils.dict_active(sysobject, sysname, also_contains_key='filesets'):  # must be ACTIVE (this test pre-dated systems.active change)
                self.utils.log('\nINTERROGATING SYSTEM', sysname)

                # get all filesets as defined in each system:
                for sys_setname, sys_setobject in sysobject['filesets'].items():
                    if self.utils.dict_active(sys_setobject, sys_setname):
                        self.utils.log('  found fileset', sys_setname)
                        self.utils.log('  cross-referencing with filesets.yaml...')

                        # cross-reference to filesets in filesets.yaml
                        if sys_setname in self.filesets:
                            setname = sys_setname
                            setobject = self.filesets[setname]

                            if self.utils.dict_active(setobject, setname, also_contains_key='files'):
                                self.utils.log('  FILE SET FOUND', setname + ' [' + str(len(setobject['files'])) + ']')
                                savepath = os.path.join(self.approot, self.folders['download'], setname)
                                if not os.path.exists(savepath):
                                    os.mkdir(savepath)

                                # download each file in the fileset
                                for file_key, file_dict in setobject['files'].items():
                                    self.utils.log('   ' + ('-' * 50))
                                    self.utils.log('   ' + file_key)

                                    # check for matching dbversion
                                    dbversion_match = True
                                    if 'dbsversion' in file_dict.keys():
                                        if sysobject['dbsversion'] in file_dict['dbsversion']:
                                            dbversion_match = True
                                        else:
                                            dbversion_match = False

                                    # check for matching collection
                                    collection_match = True
                                    if 'collection' in file_dict.keys():
                                        if sysobject['collection'] in file_dict['collection']:
                                            collection_match = True
                                        else:
                                            collection_match = False

                                    # only download file if dbsversion and collection match

                                    savefile = os.path.join(savepath, file_dict['gitfile'].split('/')[-1])  # save path
                                    # Skip download if file already exists
                                    file_exists = os.path.exists(savefile)
                                    print(str(savefile))
                                    if file_exists == True:
                                        self.utils.log('  File %s already exists in the download folder, so skipping download' % str(file_dict['gitfile'].split('/')[-1]))
                                        continue
                                    if dbversion_match and collection_match:
                                        self.utils.log('   downloading file', file_dict['gitfile'])
                                        giturl = githost + file_dict['gitfile']
                                        self.utils.log('    %s' % giturl)
                                        response = requests.get(giturl)
                                        if response.status_code == 200:
                                            filecontent = response.content
                                            self.utils.log('    saving file to', savefile)
                                            with open(savefile, 'wb') as fh:
                                                fh.write(filecontent)
                                        else:
                                            self.utils.log('Status Code: ' + str(
                                                response.status_code) + '\nText: ' + response.text, error=True)
                                            exit()

                                    else:
                                        self.utils.log('   diff dbsversion or collection, skipping')

                                self.utils.log('   ' + ('-' * 50) + '\n')

                        else:  # not found
                            self.utils.log(' not found in filesets.yaml', sys_setname)

        self.utils.log('\ndone!')
        self.utils.log('time', str(dt.datetime.now()))

    def copy_download_to_sql(self, overwrite=False):
        self.utils.log('copy_download_to_sql started', header=True)
        self.utils.log('copy files from download folder (by fileset) to sql folder (by system)')
        self.utils.log('time', str(dt.datetime.now()))
        downloadpath = os.path.join(self.approot, self.folders['download'])
        sqlpath = os.path.join(self.approot, self.folders['sql'])

        self.utils.recursively_delete_subfolders(sqlpath)

        for sysname, sysobject in self.systems.items():
            if self.utils.dict_active(sysobject, also_contains_key='filesets'):  # must be ACTIVE (this test pre-dated systems.active change)
                self.utils.log('processing system', sysname)  # just shuffled log inside of active test
                for setname, setobject in sysobject['filesets'].items():
                    self.utils.log('processing fileset', setname)
                    if self.utils.dict_active(setobject):

                        # define paths:
                        srcpath = os.path.join(self.approot, self.folders['download'], setname)
                        dstpath = os.path.join(self.approot, self.folders['sql'], sysname)
                        dstpath = os.path.join(dstpath, setname)
                        if not os.path.exists(dstpath):
                            os.makedirs(dstpath)

                        # purge existing, and copy over
                        if overwrite:
                            self.utils.recursive_delete(dstpath)

                        # loop through downloaded files and copy them to sql_store folder
                        # only copy if dbsversion and collection match
                        for downloaded_file in os.listdir(srcpath):
                            dbsversion_match = True
                            collection_match = True

                            # match downloaded file to fileset object so that we can compare collection & dbsversion
                            # note: fileset may not always have dbsversion or collection listed. always copy if thats true
                            for file_object, file_values in self.filesets['customer_data_space']['files'].items():
                                if downloaded_file == file_values['gitfile'].split('/')[-1]:
                                    if 'dbsversion' in file_values:
                                        if sysobject['dbsversion'] not in file_values['dbsversion']:
                                            dbsversion_match = False  # non-matching dbsversion: dont copy

                                    if 'collection' in file_values:
                                        if sysobject['collection'] not in file_values['collection']:
                                            collection_match = False  # non-matching collection: dont copy

                                    break

                            # only copy if dbsversion and collection match (if given)
                            # todo add logging regarding which files are being skipped / copied
                            if dbsversion_match and collection_match:
                                shutil.copyfile(os.path.join(srcpath, downloaded_file), os.path.join(dstpath, downloaded_file))

        self.utils.log('\ndone!')
        self.utils.log('time', str(dt.datetime.now()))

    def apply_override(self, override_folder='', target_folder=''):
        self.utils.log('applying file override')

        # apply folder default locations
        if override_folder == '':
            override_folder = self.folders['override']
        override_folder = os.path.join(self.approot, override_folder)

        if target_folder == '':
            target_folder = self.folders['sql']
        target_folder = os.path.join(self.approot, target_folder)

        self.utils.log(' override folder', override_folder)
        self.utils.log(' target_folder', target_folder)

        copyops = {}
        allfiles = []

        # map files found in override folder
        logdone = False
        reloadconfig = False
        for fo, subfos, files in os.walk(override_folder):
            if fo == override_folder:
                self.utils.log('\nprocessing files found in override root')
                self.utils.log('these files replace any matching filename, regardless of subfolder location')
                for file in files:
                    if file == 'config.yaml' or file == 'secrets.yaml':
                        # TODO: add  source_systems.yaml -- or, maybe can be removed?
                        copyops[os.path.join(self.approot, file)] = os.path.join(override_folder, file)
                        reloadconfig = True
                        self.utils.log('  config file found, reload imminent', file)
                    elif file[:1] != '.':
                        allfiles.append(file)
                        self.utils.log('  root file found', file)
            else:
                if os.path.basename(fo)[:1] != '.':
                    if not logdone:
                        logdone = True
                        self.utils.log('\nprocessing files found in override subfolders')
                        self.utils.log(
                            'these files only replace filenames found in matching subfolders (and overrides root files)')

                    for file in files:
                        if file[:1] != '.':
                            specfile = os.path.join(fo, file).replace(override_folder, '.')
                            keydestfile = os.path.join(target_folder, specfile)
                            keydestfo = os.path.dirname(keydestfile)
                            if os.path.exists(keydestfo):
                                copyops[keydestfile] = os.path.join(override_folder, specfile)
                                self.utils.log('  subfolder file found', specfile)
                            else:
                                self.utils.log('target folder does not exist', keydestfo, warning=True)

        # search for matching allfiles by crawling the target_folder
        for fo, subfos, files in os.walk(target_folder):
            for file in files:
                keydestfile = os.path.join(fo, file)
                if file in allfiles:
                    copyops[keydestfile] = os.path.join(override_folder, file)

        # perform final copy:
        self.utils.log('\nperform override file copy:')
        for dstpath, srcpath in copyops.items():
            self.utils.log(' source:  %s' % srcpath)
            self.utils.log(' target:  %s' % dstpath)
            shutil.copyfile(srcpath, dstpath)

        if reloadconfig:
            self.reload_config()
        self.utils.log('\napply override complete!')

    def prepare_sql(self, sqlfolder='', override_folder=''):
        self.copy_download_to_sql()  # moved from end of download_files() to here

        self.utils.log('prepare_sql started', header=True)
        self.utils.log('time', str(dt.datetime.now()))

        if sqlfolder != '':
            self.utils.log('sql folder', sqlfolder)
            self.folders['sql'] = sqlfolder
        self.utils.log(' sql folder', self.folders['sql'])
        self.utils.log(' run folder', self.folders['run'])

        self.apply_override(target_folder=sqlfolder, override_folder=override_folder)

        # clear pre-existing subfolders in "run" directory (file sets)
        self.utils.log('empty run folder entirely')
        self.utils.recursively_delete_subfolders(os.path.join(self.approot, self.folders['run']))

        # iterate all system level folders in "sql" folder...
        for sysfolder in os.listdir(os.path.join(self.approot, self.folders['sql'])):
            if os.path.isdir(os.path.join(self.approot, self.folders['sql'])):
                self.utils.log('\n' + '-' * self.utils.logspace)
                self.utils.log('SYSTEM FOLDER FOUND', sysfolder)

                if sysfolder not in self.systems or self.utils.dict_active(self.systems[sysfolder]) is False:  # must be ACTIVE (this test pre-dated systems.active change)
                    self.utils.log('folder not defined as an active system, skipping...')

                else:
                    # iterate all fileset subfolders in system folder...
                    for setfolder in os.listdir(os.path.join(self.approot, self.folders['sql'], sysfolder)):
                        if os.path.isdir(os.path.join(self.approot, self.folders['sql'], sysfolder, setfolder)):
                            self.utils.log('FILESET FOLDER FOUND', setfolder)

                            # what to do with non-fileset folders?  well, depends:
                            _continue = False
                            if setfolder not in self.filesets:
                                self.utils.log('  folder does NOT MATCH a defined fileset name', setfolder)
                                if self.settings['run_non_fileset_folders'].strip().lower() == 'true':
                                    self.utils.log(
                                        '  however setting: "run_non_fileset_folders" equals "true", continuing...')
                                    _continue = True

                                else:
                                    self.utils.log('  and setting: "run_non_fileset_folders" not equal "true", skipping...')
                                    _continue = False

                            else:  # setfolder in self.filesets
                                self.utils.log('  folder MATCHES a defined fileset name', setfolder)
                                if not self.utils.dict_active(self.systems[sysfolder]['filesets'][setfolder]):
                                    self.utils.log(
                                        "  however the system's fileset-override is marked as in-active, skipping...")
                                    _continue = False

                                elif not self.utils.dict_active(self.filesets[setfolder]):
                                    self.utils.log('  however fileset itself is marked as in-active, skipping...')
                                    _continue = False

                                else:
                                    self.utils.log('  and fileset record is active, continuing...')
                                    _continue = True

                            if _continue:

                                # define paths
                                sqlpath = os.path.join(self.approot, self.folders['sql'], sysfolder, setfolder)
                                runpath = os.path.join(self.approot, self.folders['run'], sysfolder)

                                # TODO: combine into single makedirs statement instead of 2 mkdir
                                if not os.path.isdir(runpath):
                                    self.utils.log('  creating system folder', runpath)
                                    os.mkdir(runpath)
                                runpath = os.path.join(self.approot, self.folders['run'], sysfolder, setfolder)
                                if not os.path.isdir(runpath):
                                    self.utils.log('  creating fileset folder', runpath)
                                    os.mkdir(runpath)

                                self.utils.recursive_copy(sqlpath, runpath, replace_existing=True)

                                # iterate all .coa.sql files in the fileset subfolder...
                                for runfile in os.listdir(runpath):
                                    runfilepath = os.path.join(runpath, runfile)
                                    if os.path.isfile(runfilepath) and runfile[-8:] == '.coa.sql':

                                        # if .coa.sql file, read into memory
                                        self.utils.log('\n  PROCESSING COA.SQL FILE', runfile)
                                        with open(runfilepath, 'r') as fh:
                                            runfiletext = fh.read()
                                            self.utils.log('  characters in file', str(len(runfiletext)))

                                        # SUBSTITUTE values for:  system-fileset override [source_systems.yaml --> filesets]
                                        if setfolder in self.systems[sysfolder]['filesets']:   # sysfolder is only ACTIVE systems per line 642(ish) above
                                            sub_dict = self.systems[sysfolder]['filesets'][setfolder]
                                            if self.utils.dict_active(sub_dict, 'system-fileset overrides'):
                                                runfiletext = self.utils.substitute(runfiletext, sub_dict,
                                                                              subname='system-fileset overrides (highest priority)')

                                        # SUBSTITUTE values for: system-defaults [source_systems.yaml]
                                        sub_dict = self.systems[sysfolder]   # sysfolder is only ACTIVE systems per line 642(ish) above
                                        if self.utils.dict_active(sub_dict, 'system defaults'):
                                            runfiletext = self.utils.substitute(runfiletext, sub_dict, skipkeys=['filesets'],
                                                                          subname='system defaults')

                                        # SUBSTITUTE values for: overall application defaults (never inactive) [config.yaml substitutions]
                                        self.utils.log('  always use dictionary')
                                        runfiletext = self.utils.substitute(runfiletext, self.substitutions,
                                                                      subname='overall app defaults (config.substitutions)')

                                        # SUBSTITUTE values for: TRANSCEND (mostly for db_coa and db_region)
                                        runfiletext = self.utils.substitute(runfiletext, self.transcend,
                                                                      subname='overall transcend database defaults (db_coa and db_region)',
                                                                      skipkeys=['host', 'username', 'password',
                                                                                'logmech'])

                                        # SUBSTITUTE values for: individual file subs [fileset.yaml --> files]
                                        if setfolder in self.filesets:
                                            sub_dict = {}
                                            for file_key, file_dict in self.filesets[setfolder]['files'].items():
                                                if ntpath.basename(file_dict['gitfile']) == runfile:
                                                    sub_dict = file_dict
                                                    break

                                            if sub_dict:
                                                runfiletext = self.utils.substitute(runfiletext, sub_dict,
                                                                                    skipkeys=['collection',
                                                                                              'dbsversion', 'gitfile'],
                                                                                    subname='file substitutions')

                                        # SUBSTITUTE values for: fileset defaults [fileset.yaml substitutions]
                                        if setfolder in self.filesets:
                                            sub_dict = self.filesets[setfolder]
                                            if self.utils.dict_active(sub_dict, 'fileset defaults'):
                                                runfiletext = self.utils.substitute(runfiletext, sub_dict, skipkeys=['files'],
                                                                              subname='fileset defaults (lowest priority)')

                                        # split sql file into many sql statements
                                        sqls_raw = runfiletext.split(';')
                                        self.utils.log('  sql statements in file', str(len(sqls_raw) - 1))
                                        sqls_done = []
                                        i = 0

                                        # loop thru individual sql statements within file
                                        for sql_raw in sqls_raw:

                                            # light formatting...
                                            sql = self.utils.format_sql(sql_raw)

                                            if sql != '':
                                                i += 1
                                                self.utils.log('  SQL %i' % i, '%s...' % sql[:50].replace('\n', ' '))

                                                # Get SPECIAL COMMANDS
                                                cmds = self.utils.get_special_commands(sql, '{{replaceMe:{cmdname}}}',
                                                                                 keys_to_skip=['save', 'load', 'call', 'vis', 'pptx'])
                                                sql = cmds['sql']  # sql stripped of commands (now in dict)
                                                del cmds['sql']

                                                self.utils.log('  processing special commands')
                                                for cmdname, cmdvalue in cmds.items():

                                                    # --> FILE <--: replace with local sql file
                                                    if str(cmdname[:4]).lower() == 'file':
                                                        self.utils.log('   replace variable with a local sql file')

                                                        if not os.path.isfile(os.path.join(runpath, cmdvalue)):
                                                            self.utils.log('custom file missing',
                                                                     os.path.join(runpath, cmdvalue), warning=True)
                                                            self.utils.log(
                                                                '   This may be by design, consult CSM for details.')
                                                            # raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), os.path.join(runpath, cmdvalue))
                                                        else:
                                                            self.utils.log('   specified file found', cmdvalue)
                                                            with open(os.path.join(runpath, cmdvalue), 'r') as fh:
                                                                tempsql = fh.read()
                                                            sqls_done.append('/* BEGIN file insert: %s */ \n%s' % (
                                                                cmdvalue, tempsql))
                                                            sql = sql.replace('{{replaceMe:%s}}' % cmdname,
                                                                              'END file insert: %s' % cmdvalue, 1)

                                                    # --> TEMP <--: load temp file from .csv
                                                    if str(cmdname[:4]).lower() == 'temp':
                                                        self.utils.log('   create temp (volatile) table from .csv')

                                                        if not os.path.isfile(os.path.join(runpath, cmdvalue)):
                                                            self.utils.log('csv file missing!!!',
                                                                     os.path.join(runpath, cmdvalue), error=True)
                                                            raise FileNotFoundError(errno.ENOENT,
                                                                                    os.strerror(errno.ENOENT),
                                                                                    os.path.join(runpath, cmdvalue))
                                                        else:
                                                            self.utils.log('   csv file found', cmdvalue)
                                                            tempsql = self.utils.sql_create_temp_from_csv(
                                                                os.path.join(runpath, cmdvalue))
                                                            sqls_done.append(tempsql)
                                                            sql = sql.replace('{{replaceMe:%s}}' % cmdname,
                                                                              'above volatile table create script for %s' % cmdvalue,
                                                                              1)

                                                    # --> LOOP <--: loop thru csv and generate one sql per csv row, with substitutions
                                                    if str(cmdname[:4]).lower() == 'loop':
                                                        self.utils.log('   loop sql once per row in .csv, with substitutions')

                                                        # can we find the file?
                                                        if not os.path.isfile(os.path.join(runpath, cmdvalue)):
                                                            self.utils.log('csv file missing!!!',
                                                                     os.path.join(runpath, cmdvalue), warning=True)
                                                        else:
                                                            self.utils.log('   file found!')
                                                            df = pd.read_csv(os.path.join(runpath, cmdvalue))
                                                            self.utils.log('   rows in file', str(len(df)))

                                                            # perform csv substitutions
                                                            self.utils.log(
                                                                '   perform csv file substitutions (find: {column_name}, replace: row value)')
                                                            for index, row in df.iterrows():
                                                                tempsql = sql
                                                                for col in df.columns:
                                                                    col = col.strip()
                                                                    tempsql = tempsql.replace(str('{%s}' % col),
                                                                                              str(row[col]).strip())
                                                                tempsql = tempsql.replace('{{replaceMe:%s}}' % cmdname,
                                                                                          ' csv row %i out of %i ' % (
                                                                                              index + 1, len(df)))
                                                                self.utils.log('   sql generated from row data',
                                                                         'character length = %i' % len(tempsql))
                                                                sqls_done.append(tempsql)
                                                            sql = ''  # don't append original sql again - it is only a template

                                                    # --> others, append special command back to the SQL for processing in the run phase
                                                    # if str(cmdname[:4]).lower() in ['save','load','call']:
                                                    #    sql = sql.replace('/* {{replaceMe:%s}} */' %cmdname,'/*{{%s:%s}}*/' %(cmdname, cmdvalue), 1)

                                            # after all special commands, append the original sql
                                            sqls_done.append(sql)

                                        # write out new finalized file content:
                                        self.utils.log('  writing out final sql')
                                        with open(runfilepath, 'w') as fh:
                                            fh.write('\n\n'.join(sqls_done))

        self.utils.log('done!')
        self.utils.log('time', str(dt.datetime.now()))

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
        self.utils.log('archive_prepared_sql started', header=True)
        self.utils.log('time', str(dt.datetime.now()))
        outputpath = self.make_output_folder(name)
        runpath = os.path.join(self.approot, self.folders['run'])
        self.utils.log('created output folder', outputpath)
        self.utils.log('moving all content from', runpath)
        self.utils.recursive_copy(runpath, outputpath)
        self.utils.logpath = os.path.join(outputpath, 'runlog.txt')
        self.utils.recursive_delete(os.path.join(self.approot, self.folders['run']))
        os.mkdir(os.path.join(self.approot, self.folders['run']))
        self.utils.log('done!')
        self.utils.log('time', str(dt.datetime.now()))

    def make_output_folder(self, foldername='', make_hidden_file=False, indent=0):
        # Build final folder name/path:
        name = str(dt.datetime.now())[:-7].replace(' ', '_').replace(':', '').strip()
        if foldername.strip() != '': name = '%s--%s' %(name, str(re.sub('[^0-9a-zA-Z]+', '_', foldername.strip())))
        outputpath = os.path.join(self.approot, self.folders['output'], name)
        self.utils.log('Defined output folder', outputpath)
        if not os.path.exists(outputpath):  os.makedirs(outputpath)

        if make_hidden_file:
            self.utils.log('save location of last-run output folder to hidden file', indent=indent)
            with open(os.path.join(self.approot, '.last_run_output_path.txt'), 'w') as lastoutput:
                # relative path in case of system change (i.e. switching laptops)
                lastoutput.write(outputpath[outputpath.find(self.folders['output']):])

        return outputpath

    def execute_run(self, name=''):
        self.utils.log('execute_run started', header=True)
        self.utils.log('time', str(dt.datetime.now()))

        # at this point, we make the assumption that everything in the "run" directory is valid

        # make output directory for execution output and other collateral
        runpath = os.path.join(self.approot, self.folders['run'])
        outputpath = self.make_output_folder(name)
        skip_dbs = self.utils.validate_boolean(self.settings['skip_dbs'],'bool')

        # create hidden file containing last run's output -- to remain in the root folder
        with open(os.path.join(self.approot, '.last_run_output_path.txt'), 'w') as lastoutput:

            # convert to relative path in case of system change (i.e. switching laptops)
            lastoutput.write(outputpath[outputpath.find(self.folders['output']):])

        self.utils.log('save location of last-run output folder to hidden file')
        self.utils.log('last-run output', outputpath)

        # loop through systems
        for sysname in os.listdir(runpath):
            sysfolder = os.path.join(runpath, sysname)
            if os.path.isdir(sysfolder):

                # iterate system folders  -- must exist in source_systems.yaml!
                if sysname not in self.systems or self.utils.dict_active(self.systems[sysname]) == False:  # ADDED to ensure ACTIVE systems only
                    self.utils.log('SYSTEM NOT FOUND IN SOURCE_SYSTEMS.YAML', sysname, warning=True)

                else:
                    # iterate file set folders -- ok to NOT exist, depending on setting
                    for setname in os.listdir(sysfolder):
                        setfolder = os.path.join(sysfolder, setname)
                        if os.path.isdir(setfolder):
                                                # ACTIVE ONLY, within loop a few lines above
                            if setname not in self.systems[sysname]['filesets'] and str(
                                    self.settings['run_non_fileset_folders']).strip().lower() != 'true':
                                self.utils.log('-' * self.utils.logspace)
                                self.utils.log('WARNING!!!\nfileset does not exist', setname)
                                self.utils.log(' AND setting "run_non_fileset_folders" is not "True"')
                                self.utils.log(' Skipping folder', setname)

                            else:
                                self.utils.log('SYSTEM:  %s   FILESET:  %s' % (sysname, setname), header=True)
                                workpath = setfolder
                                outputfo = os.path.join(outputpath, sysname, setname)

                                self.utils.log('work (sql) path', workpath)
                                self.utils.log('output path', outputfo)

                                # collect all prepared sql files, place in alpha order
                                coasqlfiles = []
                                for coafile in os.listdir(workpath):
                                    if coafile[:1] != '.' and coafile[-8:] == '.coa.sql':
                                        self.utils.log('found prepared sql file', coafile)
                                        coasqlfiles.append(coafile)

                                coasqlfiles.sort()

                                if len(coasqlfiles) == 0:
                                    self.utils.log('no .coa.sql files found in\n  %s' % workpath, warning=True)

                                else:
                                    self.utils.log('all sql files alpha-sorted for exeuction consistency')
                                    self.utils.log('sql files found', str(len(coasqlfiles)))

                                    # create output folder:
                                    self.utils.log('output folder', outputfo)
                                    if not os.path.exists(outputfo):
                                        os.makedirs(outputfo)
                                    self.outputpath = outputfo

                                    # create our upload-manifest, 1 manifest per fileset
                                    self.utils.log('creating upload manifest file')
                                    with open(os.path.join(outputfo, 'upload-manifest.json'), 'w') as manifest:
                                        manifest.write('{"entries":[ ')
                                    manifestdelim = '\n '

                                    # connect to customer system:   # ACTIVE ONLY within loop above
                                    conn = self.utils.open_connection(
                                        conntype=self.systems[sysname]['driver'],
                                        encryption=self.systems[sysname]['encryption'],
                                        system=self.systems[sysname],
                                        skip = skip_dbs)  # <------------------------------- Connect to the database

                                    # loop thru all sql files:
                                    for coasqlfile in sorted(coasqlfiles):
                                        self.utils.log('\nOPENING SQL FILE', coasqlfile)
                                        with open(os.path.join(workpath, coasqlfile), 'r') as coasqlfilehdlr:
                                            sqls = coasqlfilehdlr.read()  # all sql statements in a sql file

                                        sqlcnt = 0
                                        for sql in sqls.split(';'):  # loop thru the individual sql statements
                                            sqlcnt += 1

                                            if sql.strip() == '':
                                                self.utils.log('null statement, skipping')

                                            else:
                                                self.utils.log('\n---- SQL #%i' % sqlcnt)

                                                # pull out any embedded SQLcommands:
                                                sqlcmd = self.utils.get_special_commands(sql)
                                                sql = sqlcmd.pop('sql', '')


                                                df = self.utils.open_sql(conn, sql, skip = skip_dbs)  # <--------------------- Run SQL
                                                csvfile=''
                                                csvfile_exists=False

                                                if len(df) != 0:  # Save non-empty returns to .csv

                                                    if len(sqlcmd) == 0:
                                                        self.utils.log('no special commands found')

                                                    if 'save' not in sqlcmd:
                                                        sqlcmd['save'] = '%s.%s--%s' % (
                                                            sysname, setname, coasqlfile) + '%04d' % sqlcnt + '.csv'

                                                    # once built, append output folder, SiteID on the front, iterative counter if duplicates
                                                    # csvfile = os.path.join(outputfo, sqlcmd['save'])
                                                    csvfile = os.path.join(workpath, sqlcmd['save'])
                                                    i = 0
                                                    while os.path.isfile(csvfile):
                                                        i += 1
                                                        if i == 1:
                                                            csvfile = csvfile[:-4] + '.%03d' % i + csvfile[-4:]
                                                        else:
                                                            csvfile = csvfile[:-8] + '.%03d' % i + csvfile[-4:]
                                                    self.utils.log('CSV save location', csvfile)

                                                    self.utils.log('saving file...')
                                                    df.to_csv(csvfile, index=False)  # <---------------------- Save to .csv
                                                    self.utils.log('file saved!')
                                                    csvfile_exists = os.path.exists(csvfile)
                                                if 'vis' in sqlcmd:  # run visualization py file
                                                    if csvfile_exists == False:  # Avoid load error by skipping the manifest file entry if SQL returns zero records.
                                                        self.utils.log(
                                                            'The SQL returned Zero records and hence the file was not generated, So skipping the vis special command',
                                                            csvfile)
                                                    else:

                                                        self.utils.log('\nvis cmd', 'found')
                                                        vis_file = os.path.join(workpath, sqlcmd['vis'].replace('.csv', '.py'))
                                                        self.utils.log('vis py file', vis_file)
                                                        self.utils.log('running vis file..')
                                                        subprocess.run([sys.executable, vis_file])
                                                        self.utils.log('Vis file complete!')

                                                if 'pptx' in sqlcmd:  # insert to pptx file
                                                    from .pptx import replace_placeholders

                                                    self.utils.log('\npptx cmd', 'found')
                                                    pptx_file = Path(workpath) / sqlcmd['pptx']
                                                    self.utils.log('pptx file', str(pptx_file))
                                                    self.utils.log('inserting to pptx file..')
                                                    replace_placeholders(pptx_file, Path(workpath))
                                                    self.utils.log('pptx file complete!')

                                                if 'load' in sqlcmd:  # add to manifest

                                                    if csvfile_exists == False: #Avoid load error by skipping the manifest file entry if SQL returns zero records.
                                                        self.utils.log('The SQL returned Zero records and hence the file was not generated, So skipping the manifest entry',
                                                                       csvfile)
                                                    else:
                                                        self.utils.log(
                                                            'file marked for loading to Transcend, adding to upload-manifest.json')
                                                        if 'call' not in sqlcmd:
                                                            sqlcmd['call'] = ''

                                                        manifest_entry = '%s{"file": "%s",  "table": "%s",  "call": "%s"}' % (
                                                        manifestdelim, sqlcmd['save'], sqlcmd['load'],
                                                        sqlcmd['call'])
                                                        manifestdelim = '\n,'

                                                        with open(os.path.join(outputfo, 'upload-manifest.json'),
                                                              'a') as manifest:
                                                            manifest.write(manifest_entry)
                                                            self.utils.log('Manifest updated',
                                                                 str(manifest_entry).replace(',', ',\n'))

                                        # archive file we just processed (for re-run-ability)
                                        self.utils.log('Moving coa.sql file to Output folder', coasqlfile)
                                        src = os.path.join(workpath, coasqlfile)
                                        dst = os.path.join(outputfo, coasqlfile)
                                        shutil.move(src, dst)
                                        self.utils.log('')

                                # close JSON object
                                with open(os.path.join(outputfo, 'upload-manifest.json'), 'a') as manifest:
                                    manifest.write("\n  ]}")
                                    self.utils.log('closing out upload-manifest.json')

                                # Move all files from run folder to output, for posterity:
                                self.utils.log('moving all other run artifacts to output folder, for archiving')
                                self.utils.recursive_copy(workpath, outputfo, replace_existing=False)
                                self.utils.recursive_delete(workpath)

        # also COPY a few other operational files to output folder, for ease of use:
        self.utils.log('-' * self.utils.logspace)
        self.utils.log('post-processing')
        for srcpath in [os.path.join(self.approot, '.last_run_output_path.txt'),
                        self.configpath, self.filesetpath]:
            self.utils.log('copy to output folder root, for ease of use: \n  %s' % srcpath)
            dstpath = os.path.join(outputpath, os.path.basename(srcpath))
            shutil.copyfile(srcpath, dstpath)

        self.utils.log('\ndone!')
        self.utils.log('time', str(dt.datetime.now()))

        # after logging is done, move the log file too...
        runlogsrc = os.path.join(self.approot, self.folders['run'], 'runlog.txt')
        runlogdst = os.path.join(outputpath, 'runlog.txt')
        if os.path.isfile(runlogsrc): shutil.move(runlogsrc, runlogdst)

    def collect_data(self, name=''):
        self.utils.log('collect_data started', header=True)
        self.utils.log('time', str(dt.datetime.now()))

        # at this point, we make the assumption that everything in the "run" directory is valid

        # make output directory for execution output and other collateral
        runpath = os.path.join(self.approot, self.folders['run'])
        outputpath = self.make_output_folder(name)

        # create hidden file containing last run's output -- to remain in the root folder
        with open(os.path.join(self.approot, '.last_run_output_path.txt'), 'w') as lastoutput:

            # convert to relative path in case of system change (i.e. switching laptops)
            lastoutput.write(outputpath[outputpath.find(self.folders['output']):])

        self.utils.log('save location of last-run output folder to hidden file')
        self.utils.log('last-run output', outputpath)

        # loop through systems
        for sysname in os.listdir(runpath):
            sysfolder = os.path.join(runpath, sysname)
            if os.path.isdir(sysfolder):

                # iterate system folders  -- must exist in config.yaml!
                if sysname not in self.systems:
                    self.utils.log('SYSTEM NOT FOUND IN CONFIG.YAML', sysname, warning=True)

                else:
                    # iterate file set folders -- ok to NOT exist, depending on setting
                    for setname in os.listdir(sysfolder):
                        setfolder = os.path.join(sysfolder, setname)
                        if os.path.isdir(setfolder):

                            if setname not in self.systems[sysname]['filesets'] and str(
                                    self.settings['run_non_fileset_folders']).strip().lower() != 'true':
                                self.utils.log('-' * self.utils.logspace)
                                self.utils.log('WARNING!!!\nfileset does not exist', setname)
                                self.utils.log(' AND setting "run_non_fileset_folders" is not "True"')
                                self.utils.log(' Skipping folder', setname)

                            else:
                                self.utils.log('SYSTEM:  %s   FILESET:  %s' % (sysname, setname), header=True)
                                workpath = setfolder
                                outputfo = os.path.join(outputpath, sysname, setname)

                                self.utils.log('work (sql) path', workpath)
                                self.utils.log('output path', outputfo)

                                # collect all prepared sql files, place in alpha order
                                coasqlfiles = []
                                for coafile in os.listdir(workpath):
                                    if coafile[:1] != '.' and coafile[-8:] == '.coa.sql':
                                        self.utils.log('found prepared sql file', coafile)
                                        coasqlfiles.append(coafile)

                                coasqlfiles.sort()

                                if len(coasqlfiles) == 0:
                                    self.utils.log('no .coa.sql files found in\n  %s' % workpath, warning=True)

                                else:
                                    self.utils.log('all sql files alpha-sorted for exeuction consistency')
                                    self.utils.log('sql files found', str(len(coasqlfiles)))

                                    # create output folder:
                                    self.utils.log('output folder', outputfo)
                                    if not os.path.exists(outputfo):
                                        os.makedirs(outputfo)
                                    self.outputpath = outputfo

                                    # create our upload-manifest, 1 manifest per fileset
                                    self.utils.log('creating upload manifest file')
                                    with open(os.path.join(outputfo, 'upload-manifest.json'), 'w') as manifest:
                                        manifest.write('{"entries":[ ')
                                    manifestdelim = '\n '

                                    # connect to customer system:
                                    conn = self.utils.open_connection(
                                        conntype=self.systems[sysname]['driver'],
                                        encryption=self.systems[sysname]['encryption'],
                                        system=self.systems[sysname],
                                        skip = self.skip_dbs)  # <------------------------------- Connect to the database

                                    # loop thru all sql files:
                                    for coasqlfile in sorted(coasqlfiles):
                                        self.utils.log('\nOPENING SQL FILE', coasqlfile)
                                        with open(os.path.join(workpath, coasqlfile), 'r') as coasqlfilehdlr:
                                            sqls = coasqlfilehdlr.read()  # all sql statements in a sql file

                                        sqlcnt = 0
                                        for sql in sqls.split(';'):  # loop thru the individual sql statements
                                            sqlcnt += 1

                                            if sql.strip() == '':
                                                self.utils.log('null statement, skipping')

                                            else:
                                                self.utils.log('\n---- SQL #%i' % sqlcnt)

                                                # pull out any embedded SQLcommands:
                                                sqlcmd = self.utils.get_special_commands(sql)
                                                sql = sqlcmd.pop('sql', '')


                                                df = self.utils.open_sql(conn, sql, skip = self.skip_dbs)  # <--------------------- Run SQL
                                                csvfile=''
                                                csvfile_exists=False

                                                if len(df) != 0:  # Save non-empty returns to .csv

                                                    if len(sqlcmd) == 0:
                                                        self.utils.log('no special commands found')

                                                    if 'save' not in sqlcmd:
                                                        sqlcmd['save'] = '%s.%s--%s' % (
                                                            sysname, setname, coasqlfile) + '%04d' % sqlcnt + '.csv'

                                                    # once built, append output folder, SiteID on the front, iterative counter if duplicates
                                                    # csvfile = os.path.join(outputfo, sqlcmd['save'])
                                                    csvfile = os.path.join(workpath, sqlcmd['save'])
                                                    i = 0
                                                    while os.path.isfile(csvfile):
                                                        i += 1
                                                        if i == 1:
                                                            csvfile = csvfile[:-4] + '.%03d' % i + csvfile[-4:]
                                                        else:
                                                            csvfile = csvfile[:-8] + '.%03d' % i + csvfile[-4:]
                                                    self.utils.log('CSV save location', csvfile)

                                                    self.utils.log('saving file...')
                                                    df.to_csv(csvfile, index=False)  # <---------------------- Save to .csv
                                                    self.utils.log('file saved!')
                                                    csvfile_exists = os.path.exists(csvfile)


                                                if 'load' in sqlcmd:  # add to manifest

                                                    if csvfile_exists == False: #Avoid load error by skipping the manifest file entry if SQL returns zero records.
                                                        self.utils.log('The SQL returned Zero records and hence the file was not generated, So skipping the manifest entry',
                                                                       csvfile)
                                                    else:
                                                        self.utils.log(
                                                            'file marked for loading to Transcend, adding to upload-manifest.json')
                                                        if 'call' not in sqlcmd:
                                                            sqlcmd['call'] = ''

                                                        manifest_entry = '%s{"file": "%s",  "table": "%s",  "call": "%s"}' % (
                                                        manifestdelim, sqlcmd['save'], sqlcmd['load'],
                                                        sqlcmd['call'])
                                                        manifestdelim = '\n,'

                                                        with open(os.path.join(outputfo, 'upload-manifest.json'),
                                                              'a') as manifest:
                                                            manifest.write(manifest_entry)
                                                            self.utils.log('Manifest updated',
                                                                 str(manifest_entry).replace(',', ',\n'))

                                        # archive file we just processed (for re-run-ability)
                                        self.utils.log('Moving coa.sql file to Output folder', coasqlfile)
                                        src = os.path.join(workpath, coasqlfile)
                                        dst = os.path.join(outputfo, coasqlfile)
                                        shutil.move(src, dst)
                                        self.utils.log('')

                                # close JSON object
                                with open(os.path.join(outputfo, 'upload-manifest.json'), 'a') as manifest:
                                    manifest.write("\n  ]}")
                                    self.utils.log('closing out upload-manifest.json')

                                # Move all files from run folder to output, for posterity:
                                self.utils.log('moving all other run artifacts to output folder, for archiving')
                                self.utils.recursive_copy(workpath, outputfo, replace_existing=False)
                                self.utils.recursive_delete(workpath)

        # also COPY a few other operational files to output folder, for ease of use:
        self.utils.log('-' * self.utils.logspace)
        self.utils.log('post-processing')
        for srcpath in [os.path.join(self.approot, '.last_run_output_path.txt'),
                        self.configpath, self.filesetpath]:
            self.utils.log('copy to output folder root, for ease of use: \n  %s' % srcpath)
            dstpath = os.path.join(outputpath, os.path.basename(srcpath))
            shutil.copyfile(srcpath, dstpath)

        self.utils.log('\ndone!')
        self.utils.log('time', str(dt.datetime.now()))

        # after logging is done, move the log file too...
        runlogsrc = os.path.join(self.approot, self.folders['run'], 'runlog.txt')
        runlogdst = os.path.join(outputpath, 'runlog.txt')
        shutil.move(runlogsrc, runlogdst)

    def process_data(self, _outputpath=''):
        self.utils.log('process_data started', header=True)
        self.utils.log('time', str(dt.datetime.now()))
        #
        # Find the latest output path where the output of collect_data resides
        if _outputpath != '':  # use supplied path
            outputpath = _outputpath
            self.utils.log('output folder = manual param', outputpath)
        elif os.path.isfile(os.path.join(self.approot,
                                         '.last_run_output_path.txt')):  # get path from hidden .last_run_output_path.txt
            # .last_run_output_path.txt  in approot
            with open(os.path.join(self.approot, '.last_run_output_path.txt'), 'r') as fh:
                outputpath = fh.read().strip().split('\n')[0]
            outputpath = os.path.join(self.approot, outputpath)  # create absolute path from relative path in file
            self.utils.log('output folder', outputpath)
        elif self.outputpath != '':
            # local variable set
            outputpath = self.outputpath
            self.utils.log('output folder = class variable: coa.outputpath', outputpath)
        else:
            outputpath = ''
            self.utils.log('no output path defined')

        # now that outputfo is defined, let's make sure the dir actually exists:
        if not os.path.isdir(outputpath):
            self.utils.log('\nERROR = invalid path', outputpath)
            raise NotADirectoryError('Invalid Path: %s' % outputpath)

        else:
            self.outputpath = outputpath

        # update log file to correct location
        self.utils.log('updating runlog.txt location')
        self.utils.logpath = os.path.join(outputpath, 'runlog.txt')
        self.utils.bufferlogs = False
        self.utils.log('unbuffer logs')

        # at this point, we make the assumption that everything in the "run" directory is valid


        # create hidden file containing last run's output -- to remain in the root folder
        with open(os.path.join(self.approot, '.last_run_output_path.txt'), 'w') as lastoutput:

            # convert to relative path in case of system change (i.e. switching laptops)
            lastoutput.write(outputpath[outputpath.find(self.folders['output']):])

        self.utils.log('save location of last-run output folder to hidden file')
        self.utils.log('last-run output', outputpath)

        # loop through systems
        for sysname in os.listdir(outputpath):
            sysfolder = os.path.join(outputpath, sysname)
            if os.path.isdir(sysfolder):

                # iterate system folders  -- must exist in config.yaml!
                if sysname not in self.systems:
                    self.utils.log('SYSTEM NOT FOUND IN CONFIG.YAML', sysname, warning=True)

                else:
                    # iterate file set folders -- ok to NOT exist, depending on setting
                    for setname in os.listdir(sysfolder):
                        setfolder = os.path.join(sysfolder, setname)
                        if os.path.isdir(setfolder):

                            if setname not in self.systems[sysname]['filesets'] and str(
                                    self.settings['run_non_fileset_folders']).strip().lower() != 'true':
                                self.utils.log('-' * self.utils.logspace)
                                self.utils.log('WARNING!!!\nfileset does not exist', setname)
                                self.utils.log(' AND setting "run_non_fileset_folders" is not "True"')
                                self.utils.log(' Skipping folder', setname)

                            else:
                                self.utils.log('SYSTEM:  %s   FILESET:  %s' % (sysname, setname), header=True)
                                workpath = setfolder
                                outputfo = os.path.join(outputpath, sysname, setname)

                                self.utils.log('work (sql) path', workpath)
                                self.utils.log('output path', outputfo)

                                # collect all prepared sql files, place in alpha order
                                coasqlfiles = []
                                for coafile in os.listdir(workpath):
                                    if coafile[:1] != '.' and coafile[-8:] == '.coa.sql':
                                        self.utils.log('found prepared sql file', coafile)
                                        coasqlfiles.append(coafile)

                                coasqlfiles.sort()

                                if len(coasqlfiles) == 0:
                                    self.utils.log('no .coa.sql files found in\n  %s' % workpath, warning=True)

                                else:
                                    self.utils.log('all sql files alpha-sorted for exeuction consistency')
                                    self.utils.log('sql files found', str(len(coasqlfiles)))


                                    # loop thru all sql files:
                                    for coasqlfile in sorted(coasqlfiles):
                                        self.utils.log('\nOPENING SQL FILE', coasqlfile)
                                        with open(os.path.join(workpath, coasqlfile), 'r') as coasqlfilehdlr:
                                            sqls = coasqlfilehdlr.read()  # all sql statements in a sql file

                                        sqlcnt = 0
                                        for sql in sqls.split(';'):  # loop thru the individual sql statements
                                            sqlcnt += 1

                                            if sql.strip() == '':
                                                self.utils.log('null statement, skipping')

                                            else:
                                                self.utils.log('\n---- SQL #%i' % sqlcnt)

                                                # pull out any embedded SQLcommands:
                                                sqlcmd = self.utils.get_special_commands(sql)
                                                sql = sqlcmd.pop('sql', '')


                                                csvfile=''
                                                csvfile_exists=False

                                                if len(sqlcmd) == 0:
                                                    self.utils.log('no special commands found')

                                                if 'save' in sqlcmd:
                                                    csvfile = os.path.join(workpath, sqlcmd['save'])
                                                    csvfile_exists = os.path.exists(csvfile)



                                                if 'vis' in sqlcmd:  # run visualization py file
                                                    if csvfile_exists == False:  # Avoid load error by skipping the manifest file entry if SQL returns zero records.
                                                        self.utils.log(
                                                            'The SQL returned Zero records and hence the file was not generated, So skipping the vis special command',
                                                            csvfile)
                                                    else:

                                                        self.utils.log('\nvis cmd', 'found')
                                                        vis_file = os.path.join(workpath, sqlcmd['vis'].replace('.csv', '.py'))
                                                        self.utils.log('vis py file', vis_file)
                                                        self.utils.log('running vis file..')
                                                        os.system('python %s' % vis_file)
                                                        self.utils.log('Vis file complete!')

                                                if 'pptx' in sqlcmd:  # insert to pptx file
                                                    from .pptx import replace_placeholders

                                                    self.utils.log('\npptx cmd', 'found')
                                                    pptx_file = Path(workpath) / sqlcmd['pptx']
                                                    self.utils.log('pptx file', str(pptx_file))
                                                    self.utils.log('inserting to pptx file..')
                                                    replace_placeholders(pptx_file, Path(workpath))
                                                    self.utils.log('pptx file complete!')


        self.utils.log('\ndone!')
        self.utils.log('time', str(dt.datetime.now()))

    def process_manual_files(self,_outputpath=''):
        # This function assumes that the manual csv files are placed in the latest output folder
        self.utils.log('process_manual_files started', header=True)
        self.utils.log('time', str(dt.datetime.now()))
        self.utils.log('Running Prepare SQL step to get the required files for manual data processing')
        self.prepare_sql()
        runpath = os.path.join(self.approot, self.folders['run'])

        # Find the latest output path
        if _outputpath != '':  # use supplied path
            outputpath = _outputpath
            self.utils.log('output folder = manual param', outputpath)
        elif os.path.isfile(os.path.join(self.approot,
                                         '.last_run_output_path.txt')):  # get path from hidden .last_run_output_path.txt
            # .last_run_output_path.txt  in approot
            with open(os.path.join(self.approot, '.last_run_output_path.txt'), 'r') as fh:
                outputpath = fh.read().strip().split('\n')[0]
            outputpath = os.path.join(self.approot, outputpath)  # create absolute path from relative path in file
            self.utils.log('output folder', outputpath)
        elif self.outputpath != '':
            # local variable set
            outputpath = self.outputpath
            self.utils.log('output folder = class variable: coa.outputpath', outputpath)
        else:
            outputpath = ''
            self.utils.log('no output path defined')

        # now that outputfo is defined, let's make sure the dir actually exists:
        if not os.path.isdir(outputpath):
            self.utils.log('\nERROR = invalid path', outputpath)
            raise NotADirectoryError('Invalid Path: %s' % outputpath)

        else:
            self.outputpath = outputpath

        # update log file to correct location
        self.utils.log('updating runlog.txt location')
        self.utils.logpath = os.path.join(outputpath, 'runlog.txt')
        self.utils.bufferlogs = False
        self.utils.log('unbuffer logs')



        for dirs in os.listdir(runpath):
            if os.path.isdir(os.path.join(runpath,dirs)):
                shutil.move(os.path.join(runpath,dirs),outputpath)

        manual_files = []
        for files in os.listdir(outputpath):
            file_path=os.path.join(outputpath,files)
            files=os.path.basename(files)
            filename, extension = os.path.splitext(files)
            if str(extension).lower() == '.csv':
                manual_files.append(files)
                system_name = files.split('.')[0]
                fileset = files.split('.')[1]
                system_dir=os.path.join(outputpath,system_name)
                fileset_dir = os.path.join(system_dir, fileset)
                if os.path.isdir(fileset_dir):
                    shutil.copy(file_path, fileset_dir)
                    os.rename(os.path.join(fileset_dir, files),
                              os.path.join(fileset_dir, str(files.split('.')[2]) + '.' + str(files.split('.')[3])))
                    shutil.copy(os.path.join(outputpath,'upload-manifest.json'), os.path.join(fileset_dir, 'upload-manifest.json'))
                else:
                    os.mkdir(fileset_dir)
                    shutil.move(files, fileset_dir)
        # Move the runlog created by the prepare_sql function
        os.rename(os.path.join(runpath,'runlog.txt'),os.path.join(runpath,'prepare_sql_runlog.txt'))
        shutil.move(os.path.join(runpath,'prepare_sql_runlog.txt'), outputpath)

    def make_customer_files(self, name=''):
        self.utils.log('make_customer_files started', header=True)
        self.utils.log('time', str(dt.datetime.now()))

        self.bteq_prefix
        # at this point, we make the assumption that everything in the "run" directory is valid

        # make output directory for execution output and other collateral
        runpath = os.path.join(self.approot, self.folders['run'])
        outputpath = self.make_output_folder(name)

        # create hidden file containing last run's output -- to remain in the root folder
        with open(os.path.join(self.approot, '.last_run_output_path.txt'), 'w') as lastoutput:

            # convert to relative path in case of system change (i.e. switching laptops)
            lastoutput.write(outputpath[outputpath.find(self.folders['output']):])

        self.utils.log('save location of last-run output folder to hidden file')
        self.utils.log('last-run output', outputpath)

        self.utils.log('creating upload manifest file')
        with open(os.path.join(outputpath, 'upload-manifest.json'), 'a') as manifest:
            manifest.write('{"entries":[ ')

        # loop through systems
        for sysname in os.listdir(runpath):
            sysfolder = os.path.join(runpath, sysname)
            if os.path.isdir(sysfolder):

                # iterate system folders  -- must exist in config.yaml!
                if sysname not in self.systems or self.utils.dict_active(self.systems[sysname]) == False:  # ADDED to ensure ACTIVE systems only :
                    self.utils.log('SYSTEM NOT FOUND IN CONFIG.YAML', sysname, warning=True)

                else:

                    # iterate file set folders -- ok to NOT exist, depending on setting
                    for setname in os.listdir(sysfolder):
                        setfolder = os.path.join(sysfolder, setname)
                        if os.path.isdir(setfolder):
                                                # ACTIVE ONLY, within loop a few lines above
                            if setname not in self.systems[sysname]['filesets'] and str(
                                    self.settings['run_non_fileset_folders']).strip().lower() != 'true':
                                self.utils.log('-' * self.utils.logspace)
                                self.utils.log('WARNING!!!\nfileset does not exist', setname)
                                self.utils.log(' AND setting "run_non_fileset_folders" is not "True"')
                                self.utils.log(' Skipping folder', setname)

                            else:
                                self.utils.log('SYSTEM:  %s   FILESET:  %s' % (sysname, setname), header=True)
                                workpath = setfolder
                                #outputfo = os.path.join(outputpath, sysname, setname)

                                self.utils.log('work (sql) path', workpath)
                                #self.utils.log('output path', outputfo)

                                # collect all prepared sql files, place in alpha order
                                coasqlfiles = []
                                for coafile in os.listdir(workpath):
                                    if coafile[:1] != '.' and coafile[-8:] == '.coa.sql':
                                        self.utils.log('found prepared sql file', coafile)
                                        coasqlfiles.append(coafile)

                                coasqlfiles.sort()

                                if len(coasqlfiles) == 0:
                                    self.utils.log('no .coa.sql files found in\n  %s' % workpath, warning=True)

                                else:
                                    self.utils.log('all sql files alpha-sorted for execution consistency')
                                    self.utils.log('sql files found', str(len(coasqlfiles)))



                                    # create our upload-manifest, 1 manifest per fileset

                                    manifestdelim = '\n '


                                    # loop thru all sql files:
                                    for coasqlfile in sorted(coasqlfiles):
                                        self.utils.log('\nOPENING SQL FILE', coasqlfile)
                                        with open(os.path.join(workpath, coasqlfile), 'r') as coasqlfilehdlr:
                                            sqls = coasqlfilehdlr.read()  # all sql statements in a sql file

                                        sqlcnt = 0
                                        for sql in sqls.split(';'):  # loop thru the individual sql statements
                                            sqlcnt += 1

                                            if sql.strip() == '':
                                                self.utils.log('null statement, skipping')

                                            else:
                                                self.utils.log('\n---- SQL #%i' % sqlcnt)

                                                # pull out any embedded SQLcommands:
                                                sqlcmd = self.utils.get_special_commands(sql)


                                                if len(sqlcmd) == 0:
                                                    self.utils.log('no special commands found')


                                            file_ext = ".manual.coa.sql"
                                            manual_sqlfile_name = sysname + "." + setname + file_ext
                                            manual_sqlfile_path= os.path.join(outputpath,manual_sqlfile_name)
                                            manual_save_file_name = ""
                                            if 'save' in sqlcmd or 'load' in sqlcmd or 'call' in sqlcmd:
                                                manual_save_file_name=sysname + "." + setname + "." + sqlcmd['save']

                                                with open(manual_sqlfile_path, 'a') as manual_file:
                                                    manual_file.write(
                                                        "------------------------------------------------------------- \n")
                                                    manual_file.write(
                                                        "/* Save the result of the below sql as a csv file with name: " +
                                                        manual_save_file_name + "*/ \n")
                                                    manual_file.write(
                                                        "------------------------------------------------------------- \n")
                                                    manual_file.write(sql + ";")
                                                    manual_file.write("\n\n")
                                            else:
                                                with open(manual_sqlfile_path, 'a') as manual_file:
                                                    manual_file.write(sql + ";")
                                                    manual_file.write("\n\n")

                                            if 'load' in sqlcmd:  # add to manifest
                                                self.utils.log(
                                                            'file marked for loading to Transcend, adding to upload-manifest.json')
                                                if 'call' not in sqlcmd:
                                                    sqlcmd['call'] = ''
                                                manifestdelim = '\n,'
                                                manifest_entry = '{"file": "%s",  "table": "%s",  "call": "%s"}%s' % (
                                                     manual_save_file_name, sqlcmd['load'],
                                                    sqlcmd['call'],manifestdelim)


                                                manifest_filepath=os.path.join(outputpath, 'upload-manifest.json')
                                                manifest=open(manifest_filepath,'a')
                                                manifest.write(manifest_entry)
                                                self.utils.log('Manifest updated',str(manifest_entry).replace(',', ',\n'))
                                                manifest.close()

                                self.utils.recursive_delete(workpath)

        manifest = open(os.path.join(outputpath, 'upload-manifest.json'), 'a')
        manifest.write("\n  ]}")
        self.utils.log('closing out upload-manifest.json')
        manifest.close()
        # Below logic is to remove the comma (,) at the end of the last element in the manifest json file.
        with open(os.path.join(outputpath, 'upload-manifest.json'),'r') as manifest_file:
            manifest_content=manifest_file.read()
            manifest_content=manifest_content.replace(",\n  ]}","\n  ]}")
        with open(os.path.join(outputpath, 'upload-manifest.json'), 'w') as manifest_file:
            manifest_file.write(manifest_content)
        # also COPY a few other operational files to output folder, for ease of use:
        self.utils.log('-' * self.utils.logspace)
        self.utils.log('post-processing')
        for srcpath in [os.path.join(self.approot, '.last_run_output_path.txt'),
                        self.configpath, self.filesetpath]:
            self.utils.log('copy to output folder root, for ease of use: \n  %s' % srcpath)
            dstpath = os.path.join(outputpath, os.path.basename(srcpath))
            shutil.copyfile(srcpath, dstpath)

        self.utils.log('\ndone!')
        self.utils.log('time', str(dt.datetime.now()))

        # after logging is done, move the log file too...
        runlogsrc = os.path.join(self.approot, self.folders['run'], 'runlog.txt')
        runlogdst = os.path.join(outputpath, 'runlog.txt')
        shutil.move(runlogsrc, runlogdst)
        self.utils.log('make_customer_files Completed', header=True)

    def upload_to_transcend(self, _outputpath=''):
        self.utils.bufferlogs = True
        self.utils.log('upload_to_transcend started', header=True)
        self.utils.log('time', str(dt.datetime.now()))

        skip_dbs = self.utils.validate_boolean(self.settings['skip_dbs'],'bool')

        # process 3 ways to get output path
        if _outputpath != '':  # use supplied path
            outputpath = _outputpath
            self.utils.log('output folder = manual param', outputpath)
        elif os.path.isfile(os.path.join(self.approot, '.last_run_output_path.txt')):  # get path from hidden .last_run_output_path.txt
            # .last_run_output_path.txt  in approot
            with open(os.path.join(self.approot, '.last_run_output_path.txt'), 'r') as fh:
                outputpath = fh.read().strip().split('\n')[0]
            outputpath = os.path.join(self.approot, outputpath)  # create absolute path from relative path in file
            self.utils.log('output folder', outputpath)
        elif self.outputpath != '':
            # local variable set
            outputpath = self.outputpath
            self.utils.log('output folder = class variable: coa.outputpath', outputpath)
        else:
            outputpath = ''
            self.utils.log('no output path defined')

        # now that outputfo is defined, let's make sure the dir actually exists:
        if not os.path.isdir(outputpath):
            self.utils.log('\nERROR = invalid path', outputpath)
            raise NotADirectoryError('Invalid Path: %s' % outputpath)

        else:
            self.outputpath = outputpath

        # update log file to correct location
        self.utils.log('updating runlog.txt location')
        self.utils.logpath = os.path.join(outputpath, 'runlog.txt')
        self.utils.bufferlogs = False
        self.utils.log('unbuffer logs')

        # connect to Transcend
        self.utils.log('connecting to transcend')
        self.utils.log('    host', self.transcend['host'])
        self.utils.log('    logmech', self.transcend['logmech'])
        self.utils.log('    username', self.transcend['username'])
        self.utils.log('    password', self.transcend['password'])
        self.utils.log('    db_coa', self.transcend['db_coa'])
        self.utils.log("\nNOTE:  if you happen to see a scary WARNING below, DON'T PANIC!")
        self.utils.log("       it just means you already had an active connection that was replaced.\n")

        transcend = self.utils.open_connection(
            'teradataml',
            system=self.transcend,
            skip = skip_dbs)  # <--------------------------------- Connect

        # Walk the directory structure looking for upload_manifest.json files
        for workpath, subfo, files in os.walk(outputpath):
            self.utils.log('\nexamining folder', str(workpath).strip())
            workname = os.path.split(workpath)[1]
            if str(workname)[:1] != '.':  # no hidden folders
                if 'upload-manifest.json' in files:

                    self.utils.log('FOUND upload-manifest.json')
                    with open(os.path.join(workpath, 'upload-manifest.json'), 'r') as fh:
                        manifestjson = fh.read()
                    manifest = json.loads(manifestjson)
                    self.utils.log('upload count found', str(len(manifest['entries'])))
                    self.utils.log('manifest file', '\n%s' % str(manifest))

                    if len(manifest['entries']) == 0:
                        self.utils.log('nothing to upload, skipping', workpath)
                    else:

                        for entry in manifest['entries']:
                            successful_load = False  # flag to track if sp should be called after load attempt

                            # define database and table names
                            if '.' in entry['table']:
                                entry['schema'] = entry['table'].split('.')[0]
                                entry['table'] = entry['table'].split('.')[1]
                            else:
                                entry['schema'] = 'adlste_coa_stg'

                            self.utils.log('\nPROCESSING NEW ENTRY')
                            self.utils.log('  load file', entry['file'])
                            self.utils.log('  into table', entry['table'])
                            self.utils.log('  of schema', entry['schema'])
                            self.utils.log('  then call', entry['call'])
                            self.utils.log('-' * 10)

                            # open CSV and prepare for appending
                            csvfilepath = os.path.join(workpath, entry['file'])
                            self.utils.log('opening csv', csvfilepath)
                            dfcsv = pd.read_csv(csvfilepath)
                            dfcsv = dfcsv.where(pd.notnull(dfcsv), None)
                            self.utils.log('records found', str(len(dfcsv)))

                            # strip out any unnamed columns
                            for col in dfcsv.columns:
                                if col[:8] == 'Unnamed:':
                                    self.utils.log('unnamed column dropped', col)
                                    self.utils.log('  (usually the pandas index as a column, "Unnamed: 0")')
                                    dfcsv = dfcsv.drop(columns=[col])
                            self.utils.log('final column count', str(len(dfcsv.columns)))

                            # APPEND data to database
                            self.utils.log('\nuploading', str(dt.datetime.now()))
                            try:
                                # write_to_perm = True
                                # steps:
                                # 1. load to staging table (perm)
                                # 2. load staging data --> global temp table (GTT)
                                # 3. call sp on GTT to merge to final table
                                # 4. delete staging table (perm)
                                if self.settings['write_to_perm'].lower() == 'true':
                                    self.utils.log('write_to_perm', 'True')
                                    self.utils.log('perm table', entry['schema'] + '.' + entry['table'] + '_%s' % self.unique_id)

                                    # create staging table (perm) with unique id
                                    if not skip_dbs:
                                        df_to_sql(transcend['connection'], dfcsv, entry['table'], entry['schema'], copy_sfx=self.unique_id)
                                    self.utils.log('load to PERM and GTT complete', str(dt.datetime.now()))
                                    successful_load = True

                                # write_to_perm = False
                                # steps:
                                # 1. load into pre-created GTT table
                                #    1a. (will auto-create perm table if GTT doesnt exist)
                                # 2. call sp on GTT to merge to final table
                                else:
                                    self.utils.log('write_to_perm', 'False')
                                    if not skip_dbs:
                                        df_to_sql(transcend['connection'], dfcsv, entry['table'], entry['schema'])
                                    self.utils.log('load to GTT complete', str(dt.datetime.now()))
                                    successful_load = True

                            except Exception as err:
                                from textwrap import dedent

                                self.utils.log('\nERROR during UPLOAD', error=True)
                                self.utils.log(str(err))
                                self.utils.log('   (error repeated below)')
                                self.utils.log('\n    first 10 records of what was being uploaded (dataframe):')
                                self.utils.log(dfcsv[0:10])
                                self.utils.log('')
                                sql = dedent(f"""\
                                    Select ColumnName, ColumnType, ColumnFormat, ColumnLength, ColumnId
                                    from dbc.columns
                                    where databasename = '{entry['schema']}'
                                      and tablename = '{entry['table']}'
                                    order by ColumnId""")
                                self.utils.log(sql)
                                df = sql_to_df(transcend['connection'], sql)
                                self.utils.log('\n\n    structure of destination table:')
                                print(df)
                                self.utils.log('\n')
                                exit()  # todo possibly remove so that whole process doesnt stop on error?

                            # CALL any specified SPs only if data loaded successfully:
                            if str(entry['call']).strip() != "" and successful_load:
                                self.utils.log('\nStored Proc', str(entry['call']))
                                try:
                                    if not skip_dbs:
                                        with transcend['connection'].cursor() as csr:
                                            csr.execute('call %s ;' % str(entry['call']))
                                    self.utils.log('complete', str(dt.datetime.now()))

                                    # if write_to_perm == true, drop unique perm table after successful sp call
                                    if self.settings['write_to_perm'].lower() == 'true':
                                        self.utils.log('\ndrop unique perm table', entry['schema'] + '.' + entry['table'] + '_%s' % self.unique_id)

                                        if not skip_dbs:
                                            with transcend['connection'].cursor() as csr:
                                                csr.execute("""
                                                DROP TABLE {db}.{unique_table}
                                                """.format(db=entry['schema'],
                                                       unique_table=entry['table'] + '_%s' % self.unique_id))

                                        self.utils.log('complete', str(dt.datetime.now()))

                                except OperationalError as err:  # raise exception if database execution error (e.g. permissions issue)
                                    self.utils.log('\n\n')
                                    self.utils.log(str(err).partition('\n')[0], error=True)
                                    exit()

        self.utils.log('\ndone!')
        self.utils.log('time', str(dt.datetime.now()))

    def deactivate_all(self):
        """Sets all systems and system-filesets to False"""
        for sysname, sysobject  in  self.systems.items():
            sysobject['active'] = 'False'
            for setname, setobject in  sysobject['filesets'].items():
                setobject['active'] = 'False'

    def display_motd(self):
        webbrowser.open(self.motd_url)

    def yaml_config(self):
        tmp = []
        tmp.append('substitutions:')
        tmp.append('  account:            "Demo Customer"')
        tmp.append('  startdate:          "Current_Date - 7"')
        tmp.append('  enddate:            "Current_Date - 1"')
        tmp.append('')
        tmp.append('transcend:')
        tmp.append('  host:       "tdprdcop3.td.teradata.com"')
        tmp.append('  username:   "{td_quicklook}"')
        tmp.append('  password:   "{td_password}"')
        tmp.append('  logmech:    "LDAP"')
        tmp.append('  db_coa:     "adlste_coa"')
        tmp.append('  db_region:  "adlste_westcomm"')
        tmp.append('  db_stg:     "adlste_coa_stg"')
        tmp.append('')
        tmp.append('folders:')
        tmp.append('  override:  "0_override"')
        tmp.append('  download:  "1_download"')
        tmp.append('  sql:       "2_sql_store"')
        tmp.append('  run:       "3_ready_to_run"')
        tmp.append('  output:    "4_output"')
        tmp.append('')
        tmp.append('settings:')
        tmp.append('  githost:    "https://raw.githubusercontent.com/tdcoa/sql/master/"')
        tmp.append('  gitfileset: "filesets/filesets.yaml"')
        tmp.append('  gitmotd:    "motd.html"')
        tmp.append('  localfilesets:   "./{download}/filesets.yaml"')
        tmp.append('  secrets:    "secrets.yaml"')
        tmp.append('  systems:    "source_systems.yaml"')
        tmp.append('  text_format_extensions: [".sql", ".yaml", ".txt", ".csv", ".py"]')
        tmp.append('  gui_show_dev_filesets:   "False"')
        tmp.append('  run_non_fileset_folders: "True"')
        tmp.append('  skip_dbs:   "False"')
        tmp.append('  write_to_perm: "True"')
        return '\n'.join(tmp)

    def yaml_systems(self):
        tmp = []
        tmp.append('systems:')
        tmp.append('  Transcend:')
        tmp.append('    siteid:      "TRANSCEND02"  ')
        tmp.append('    active:      "True"')
        tmp.append('    host:        "tdprdcop3.td.teradata.com"')
        tmp.append('    username:    "{td_quicklook}"')
        tmp.append('    password:    "{td_password}"')
        tmp.append('    logmech:     "ldap"')
        tmp.append('    driver:      "sqlalchemy" ')
        tmp.append('    encryption:  "false"')
        tmp.append('    use:         "test"   ')
        tmp.append('    dbsversion:  "16.20"')
        tmp.append('    collection:  "dbc"')
        tmp.append('    filesets:')
        tmp.append('      demo:')
        tmp.append('        active:     "True"')
        return '\n'.join(tmp)

# ------------- everything below here is new /
# ------------- trying to reduce repetitive "file iteration" code
    def make_customer_files2(self, name=''):
        self.utils.log('generating manual customer files', header=True)
        info = os.path.join(self.approot, self.folders['run'])
        outfo = self.make_output_folder('assisted_run', make_hidden_file=True)

        self.iterate_coa('generate bteq file',       info, outfo, {'all_make_bteq': self.coasql_assist_bteq}, file_filter_regex="\.coa\.sql$", sqlfile=True)
        self.iterate_coa('move run files to output', info, outfo, {'move_files': self.coafile_move})  # default is all files, sqlfile=False
        self.iterate_coa('combine .coa files',      outfo, outfo, {'combine_files': self.coafile_combine},    file_filter_regex="\.coa\.(bteq|sql)$")

    def process_return_data2(self, folderpath):
        self.utils.log('processing completed run files: %s' %folderpath, header=True)
        info = outfo = folderpath

        funcs = {'convert_psv_to_csv': self.coafile_convert_psv2csv}
        self.iterate_coa('convert any psv to csv', info, outfo, funcs, file_filter_regex="\.(csv|psv)$")

        funcs = {'delete_json_manifests': self.coafile_delete}
        self.iterate_coa('remove any json manifests', info, outfo, funcs, file_filter_regex="manifest\.json$")

        funcs = {'vis':  self.coasql_visualize,
                 'pptx': self.coasql_pptx,
                 'load': self.coasql_make_uploadmanifest}
        self.iterate_coa('process special commands', info, outfo, funcs, file_filter_regex="\.coa\.sql$", sqlfile=True)

# ------------- iteration engine:
    def iterate_coa(self, proc_name='', folderpath_in='', folderpath_out='', functions={'do nothing': lambda *args: args[0]}, file_filter_regex='(.*?)', sqlfile=False):
        """iterate the folder_in path, recursively, pattern match files, and execute supplied function.
        """

        # define fully qualified in/out paths (well, down to approot level)
        self.utils.log('iteration started!', proc_name, header=True)
        fin  = folderpath_in  if folderpath_in  !='' else os.path.join(self.approot, self.folders['run'])
        fout = folderpath_out if folderpath_out !='' else fin
        self.utils.log('folder in:  %s' %fin, '\nfolder out: %s' %fout)
        self.utils.log('regex qualifier for files: %s' %file_filter_regex)
        self.utils.log('%i functions to be applied at %s level %s' %(len(functions), 'SQL' if sqlfile else 'FILE', tuple(functions.keys()) ))


        # iterate all subfolders and files (aka /fin/System/FileSet)
        self.utils.log('\nfiles:')

        for root, dirs, files in os.walk(fin):
            for filename in sorted(files):
                ind = 2
                srcpath = os.path.join(root, filename)
                dstpath = os.path.join(root.replace(fin,fout), filename)
                fileset = os.path.dirname(srcpath).split(os.sep)[-1]
                system  = os.path.dirname(srcpath).split(os.sep)[-2]
                if system not in self.systems: break  # must be in a real system
                self.utils.log('%s' %filename, indent=ind)
                ind += 2

                # only process if files match supplied regex pattern:
                if len(re.findall(file_filter_regex, srcpath)) == 0:   # NOT FOUND
                    self.utils.log('filter NOT satisfied', file_filter_regex, indent=ind)
                else:  # FOUND:
                    self.utils.log('filter IS satisfied', file_filter_regex, indent=ind)
                    self.utils.log('filepath in',  srcpath, indent=ind)
                    self.utils.log('filepath out', dstpath, indent=ind)

                    # create dest folder if missing:
                    if not os.path.exists(os.path.dirname(dstpath)):
                        self.utils.log('creating filepath out', os.path.dirname(dstpath), indent=ind)
                        os.makedirs(os.path.dirname(dstpath))

                    # throw a few assumptions in the trunk, and let's go!
                    trunk = {'filepath_in':srcpath, 'filepath_out':dstpath, 'filename':filename,
                             'folderpath_in':os.path.dirname(srcpath), 'folderpath_out':os.path.dirname(dstpath),
                             'fileset':fileset, 'system':system, 'index':0, 'phase':'execute', 'postwork':{}, 'sql':{}}

                    # EXECUTE functions against entire file:
                    if not sqlfile:
                        for nm, func in functions.items():
                            trunk['function_name'] = nm
                            trunk['log_indent'] = ind
                            trunk = func(trunk)
                            self.utils.log('done!', indent=ind)

                    # Split sqlfile on ';' and EXECUTE function against each sql statement
                    elif sqlfile:
                        with open(srcpath, 'r') as fh:
                            sqls_text = fh.read()
                        sqls = sqls_text.split(";")
                        self.utils.log('sql file contains %i statements' %len(sqls), indent=ind)
                        trunk['sql'] = {}
                        trunk['index'] = 0
                        trunk['phase'] = 'execute'

                        # iterate sqls
                        for sql in sqls:
                            trunk['index'] +=1
                            self.utils.log('processing sql #%i' %trunk['index'], indent=6)
                            ind=8
                            trunk['special_commands'] = self.utils.get_special_commands(sql, indent=ind)
                            trunk['sql']['original'] = sql
                            trunk['sql']['formatted'] = self.utils.format_sql(sql)
                            trunk['sql']['special_command_out'] = trunk['special_commands']['sql']
                            trunk['log_indent'] = ind+2

                            # for any "all" functions, or any special commands, execute:
                            for name, func in functions.items():
                                if name in trunk['special_commands'] or name[:3]=='all':
                                    trunk['function_name'] = name
                                    self.utils.log('qualifies for function: %s' %name, indent=ind)
                                    trunk = func(trunk)

                        # post work per file -- added to the trunk as needed by above func()
                        trunk['phase'] = 'postwork'
                        for postname, postfunc in trunk['postwork'].items():
                            trunk = postfunc(trunk)

# ------------- subprocesses designed to be plugged into the above engine
    def coasql_do_nothing(self, trunk):
        """literally, does nothing.  just a stub for testing/future work."""
        self.utils.log('subfunction called', trunk['function_name'], indent=trunk['log_indent'])
        return trunk

    def coafile_combine(self, trunk):
        """Combines all files supplied into one master file, by extension type.
        i.e., all .sql files will be one file, all .bteq files another.
        Combined master file will be named "!System_Fileset.ext"
        Note: special handling exists for bteq files, to ensure only one "bteq_prefix"
        section exists per master file, regardless of how many sub-bteq files there are"""
        self.utils.log('subfunction called', trunk['function_name'], indent=trunk['log_indent'])

        fileext = str(trunk['filepath_in'].split('.')[-1]).lower().strip()
        combinedfile = '!_%s_%s.combined.%s' %(trunk['system'], trunk['fileset'], fileext)
        combinedpath = os.path.join(trunk['folderpath_out'], combinedfile)
        combinedtext = ''

        self.utils.log('reading file content %s into file %s' %(trunk['filename'], combinedfile), indent=trunk['log_indent'])
        with open(trunk['filepath_in'], 'r') as fh:
            filetext =  fh.read()

        # determine if file is BTEQ, and if so, ensure only one logon credential on top:
        if self.bteq_prefix in filetext or fileext in ['btq','bteq']:
            filetext = filetext.replace(self.bteq_prefix, '')

            if not os.path.isfile(combinedpath):
                with open(combinedpath, 'w') as fh:
                    fh.write(self.bteq_prefix)

        # at this point, filetype doesn't matter... always append to master
        with open(combinedpath, 'a') as fh:
            fh.write(filetext)

        return trunk

    def coafile_move(self, trunk):
        self.utils.log('subfunction called', trunk['function_name'], indent=trunk['log_indent'])
        shutil.move(trunk['filepath_in'], trunk['filepath_out'])
        self.utils.log('file moved from', trunk['filepath_in'], indent=trunk['log_indent']+2)
        self.utils.log('file moved to',  trunk['filepath_out'], indent=trunk['log_indent']+2)
        trunk['filepath_in'] = trunk['filepath_out']
        return trunk

    def coafile_delete(self, trunk):
        self.utils.log('subfunction called', trunk['function_name'], indent=trunk['log_indent'])
        try:
            self.utils.log('deleting file', trunk['filepath_in'], indent=trunk['log_indent']+2)
            os.remove(trunk['filepath_in'])
            self.utils.log('file deleted ', trunk['filepath_in'], indent=trunk['log_indent']+2)
        except Exception as ex:
            self.utils.log(ex, error=True, indent=trunk['log_indent']+2)
        return trunk

    def coafile_convert_psv2csv(self, trunk):
        """Tests csv files for pipe-delimited or self.bteq_delim, and if true, convert to comma delimited"""
        self.utils.log('subfunction called', trunk['function_name'], indent=trunk['log_indent'])

        # open file and determine whether .csv or .psv:
        self.utils.log('opening file', trunk['filepath_in'], indent=trunk['log_indent']+2)
        filetext = open(trunk['filepath_in'],'r').read()

        # iterate to the best-fit delimiter amonst candidates, with bias towards earlier found
        self.utils.log('testing for best-fit delimiter candidate...')
        sep = self.bteq_delim
        for sepc in [',,', ',', '::', ':', ';;', ';', '||', '|']:
            if (filetext.count(sepc)*len(sepc)*1.1) > (filetext.count(sep)*len(sep)): sep = sepc
        self.utils.log('delimiter %s wins with %i instances found' %(str(sep), filetext.count(sep)))

        filetext = None # be kind
        self.utils.log('file delimiter determined as', sep, indent=trunk['log_indent']+4)

        # if sep is greater than 1 character, it's treated as regex... let's undo that
        if len(sep) > 1: sep = ''.join(['\\'+ c for c in sep])

        if sep != ',':
            df = pd.read_csv(trunk['filepath_in'], sep=sep)
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x) # trim whitespace from data
            df = df.rename(columns=lambda x: x.strip()) # trim whitespace from column headers
            df = df.where(pd.notnull(df), None) # handle nulls
            self.utils.log('records found', str(len(df)), indent=trunk['log_indent']+4)
            self.utils.log('columns', str(list(df.columns)), indent=trunk['log_indent']+4)
            if trunk['filepath_in'][-4:] == '.psv': trunk['filepath_in'] = trunk['filepath_in'][-4:] + '.csv'
            df.to_csv(trunk['filepath_in'], index=False, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            self.utils.log('file converted to .csv', indent=trunk['log_indent']+4)
        else:
            self.utils.log('file already .csv, no change', indent=trunk['log_indent']+4)

        return trunk

    def coasql_assist_bteq(self, trunk):
        """turns sql into bteq commands.  If trunk['phase']=='postwork' the processed
        will add prefix/suffix commands and save the .coa.bteq file."""
        self.utils.log('subfunction called', trunk['function_name'], indent=trunk['log_indent'])
        if 'bteq_sql' not in trunk: trunk['bteq_sql'] = []

        if trunk['phase'] == 'postwork':
            # wrap bteq with begin/end logic, and save file:
            trunk['bteq_sql'].insert(0, '-----> file: %s \n' %os.path.basename(trunk['filepath_in']) )
            trunk['bteq_sql'].insert(0, self.bteq_prefix)
            trunk['bteq_sql'].append('.export reset')
            trunk['bteq_filepath'] = trunk['filepath_out'].replace('.coa.sql','.coa.bteq')
            self.utils.log('file complete, saving...', trunk['bteq_filepath'], indent=trunk['log_indent'])
            with open(trunk['bteq_filepath'], 'w') as fh:
                fh.write('\n\n'.join(trunk['bteq_sql']))
            self.utils.log('complete!', indent=trunk['log_indent'])

        else: # still processing sql statements:
            self.utils.log('translating to bteq...', indent=trunk['log_indent'])
            if 'save' in trunk['special_commands']:
                trunk['bteq_sql'].append('.export reset')
                trunk['bteq_sql'].append('.export report file="%s" , close' %str(trunk['special_commands']['save']))
                self.utils.log('adding export commands', indent=trunk['log_indent'])

            trunk['bteq_sql'].append(trunk['sql']['formatted'])

        # register postwork function, so this is called after file is complete
        trunk['postwork']['assist_bteq'] = self.coasql_assist_bteq
        return trunk

    def coasql_visualize(self, trunk):
        """runs any external python script, specifically for visualizations"""
        self.utils.log('subfunction called', trunk['function_name'], indent=trunk['log_indent'])
        pypath = os.path.join(trunk['folderpath_in'], trunk['special_commands']['vis'].replace('.csv','.py'))
        self.utils.log('executing visualization', indent=trunk['log_indent']+2)
        self.utils.log('on file', trunk['special_commands']['vis'], indent=trunk['log_indent']+4)
        self.utils.log('using script', pypath, indent=trunk['log_indent']+4)
        subprocess.run([sys.executable, pypath])
        return trunk

    def coasql_pptx(self, trunk):
        """process any visualizations"""
        from .pptx import replace_placeholders
        self.utils.log('subfunction called', trunk['function_name'], indent=trunk['log_indent'])
        pptxpath = os.path.join(trunk['folderpath_in'], trunk['special_commands']['pptx'])
        self.utils.log('performing powerpoint substitutions on %s' %pptxpath, indent=trunk['log_indent']+2)
        pptx_file = Path(pptxpath)
        replace_placeholders(pptx_file, Path(trunk['folderpath_out']))
        self.utils.log('pptx file complete!', indent=trunk['log_indent']+2)
        return trunk

    def coasql_make_uploadmanifest(self, trunk):
        """creates upload_manifest.json (hopefully a legacy process soon)"""
        self.utils.log('subfunction called', 'make upload manifest', indent=trunk['log_indent'])
        s = l = c = ''
        umfile = 'upload-manifest.json'
        umpath = os.path.join(trunk['folderpath_out'], umfile)

        # POSTWORK: perform all substitutions (file level)
        if trunk['phase'] == 'postwork':
            subs = [self.transcend, self.substitutions, self.settings, self.systems[trunk['system']], self.filesets[trunk['fileset']], self.systems[trunk['system']]['filesets'][trunk['fileset']] ]

            self.utils.log('applying substitutions to upload manifest file', indent=trunk['log_indent']+4)
            with open(umpath, 'r+') as fh:
                umtext = fh.read()
                for sub in subs:
                    umtext = self.utils.substitute(umtext, sub)
                fh.seek(0)
                fh.write(umtext)
                fh.truncate()

        else: # not post-work, (per sql level):
            if 'save' in trunk['special_commands']: s = trunk['special_commands']['save']
            if 'load' in trunk['special_commands']: l = trunk['special_commands']['load']
            if 'call' in trunk['special_commands']: c = trunk['special_commands']['call']
            line = '\n{ "file": "%s",\n  "table": "%s",\n  "call": "%s"}' %(s,l,c)

            # no file = new run = lay in preable + line
            if not os.path.isfile(umpath):
                with open(umpath, 'w') as fh:
                    fh.write('{"entries":[\n %s \n]}' %line)
                self.utils.log('created new upload manifest file', indent=trunk['log_indent']+4)

            else: # file exists, just add comma + line (json close happens in postwork)
                with open(umpath, 'r') as fh:
                    lines = fh.readlines()
                if lines[-1].strip() == ']}': lines = lines[:-1]
                lines.append(',' + line)
                lines.append('\n]}')
                with open(umpath, 'w') as fh:
                    fh.write(''.join(lines))

            self.utils.log('added to upload manifest', line.replace('\n',''), indent=trunk['log_indent']+4)
            trunk['postwork']['upload_manifest'] = self.coasql_make_uploadmanifest # register for postwork
        return trunk





# ------------- subprocesses - stubs and partial work
    def coasql_upload(self, trunk):
        """process any visualizations"""
        self.utils.log('subfunction called', trunk['function_name'], indent=trunk['log_indent'])
        return trunk

    def coasql_execute_sql(self, trunk):
        self.utils.log('subfunction called', trunk['function_name'], indent=trunk['log_indent'])
        return trunk

    def coasql_save_csv(self, trunk):
        self.utils.log('subfunction called', trunk['function_name'], indent=trunk['log_indent'])
        return trunk

    def coasql_load(self, trunk):
        """when handed a sql statement that qualifes for loading, loads to Transcend."""
        self.utils.log('subfunction called', trunk['function_name'], indent=trunk['log_indent'])

        # find name of .csv to load:
        csvfilepath = os.path.join(trunk['folderpath_in'], trunk['special_commands']['save'])
        self.utils.log('csvfile',csvfilepath, indent=trunk['log_indent'])
        if not os.path.isfile(csvfile):
            self.utils.log('cannot find file', csvfilepath, warning=True)
            return trunk

        # build connection object
        if 'connection' not in trunk or trunk['connection'] is None:
            trunk['connection'] = self.utils.open_connection(
                conntype=self.systems[trunk['system']]['driver'],
                encryption=self.systems[trunk['system']]['encryption'],
                system=self.systems[trunk['system']],
                skip = skip_dbs)  # <------------------------------- Connect to the database

        self.utils.log('UPLOADING file', os.path.basename(csvfilepath), indent=trunk['log_indent']+2)
        self.utils.log('     TO system', trunk['system'], indent=trunk['log_indent']+2)

        # open csv as dataframe
        dfcsv = pd.read_csv(csvfilepath)
        dfcsv = dfcsv.where(pd.notnull(dfcsv), None)
        self.utils.log('records found', str(len(dfcsv)))

        # strip out any unnamed columns
        for col in dfcsv.columns:
            if col[:8] == 'Unnamed:':
                self.utils.log('unnamed column dropped', col)
                self.utils.log('  (usually the pandas index as a column, "Unnamed: 0")')
                dfcsv = dfcsv.drop(columns=[col])
        self.utils.log('final column count', str(len(dfcsv.columns)))




        return trunk

    def coasql_call(self, trunk):
        self.utils.log('subfunction called', trunk['function_name'], indent=trunk['log_indent'])
        return trunk
