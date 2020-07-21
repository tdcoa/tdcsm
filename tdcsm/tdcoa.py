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
from sqlalchemy.exc import OperationalError
from teradataml import DataFrame
from teradataml.dataframe.copy_to import copy_to_sql
import webbrowser


import tdcsm
from pathlib import Path
from tdcsm.utils import Utils  # includes Logger class


# todo create docstring for all methods


class tdcoa:
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
      >>> c.prepare_sql()
      >>> c.execute_run()          # target system VPN
      >>> c.upload_to_transcend()  # transcend VPN
    """

    # paths
    approot = '.'
    configpath = ''
    secretpath = ''
    systemspath = ''
    filesetpath = ''
    outputpath = ''
    version = "0.3.9.7.1"
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

    def __init__(self, approot='.', printlog=True, config='config.yaml', secrets='secrets.yaml', filesets='filesets.yaml', systems='source_systems.yaml', refresh_defaults=False):
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

        # filesets.yaml is validated at download time

        self.reload_config()
        if os.path.exists(self.filesetpath) and os.path.exists(self.systemspath):
            self.update_sourcesystem_yaml()

    # Function to add Source system yaml file with all the fileset entries in the filesets.yaml file and set active to false
    def update_sourcesystem_yaml(self):
        with open(self.systemspath, 'r') as fh:
            filesetstr = fh.read()
        sourcesysyaml = yaml.load(filesetstr, Loader=yaml.FullLoader)
        fh.close()
        with open(self.filesetpath, 'r') as fh:
            filesetstr = fh.read()
        filesetsyaml = yaml.load(filesetstr, Loader=yaml.FullLoader)
        fh.close()
        filesets = []
        for fileset, filesetobj in filesetsyaml.items():
            filesets.append(fileset)

        for sysname in sourcesysyaml['systems'].keys():
            sys_filesets = list(sourcesysyaml['systems'][sysname]['filesets'].keys())
            missing_filesets = list(set(filesets) - set(sys_filesets))
            for fileset in missing_filesets:
                sourcesysyaml['systems'][sysname]['filesets'][fileset] = {}
                sourcesysyaml['systems'][sysname]['filesets'][fileset]['active'] = 'False'

        with open(self.systemspath, 'w') as fh:
            fh.write(yaml.dump(sourcesysyaml))
        fh.close()
        self.utils.log('Updated the Sourcesystem yaml file with all the fileset entries in the filesets.yaml file')

    def reload_config(self, configpath='', secretpath='', systemspath='', refresh_defaults=False):
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

        configpath = self.configpath if configpath == '' else configpath
        secretpath = self.secretpath if secretpath == '' else secretpath
        systemspath = self.systemspath if systemspath == '' else systemspath
        self.refresh_defaults = refresh_defaults

        self.utils.bufferlogs = True
        self.utils.log('reload_config started', header=True)
        self.utils.log('time', str(dt.datetime.now()))
        self.utils.log('tdcoa version', self.version)


        # ensure all required configuration files are present:
        self.utils.log('checking core config files')
        startfiles = ['secrets.yaml','config.yaml','source_systems.yaml','run_gui.py','run_gui','run_cmdline.py','run_cmdline']
        startfilecontent = ''
        for startfile in startfiles:
            startfile_src = os.path.join(os.path.dirname(tdcsm.__file__), startfile)
            startfile_ovr = os.path.join(self.approot,'0_override', startfile)
            startfile_dst = os.path.join(self.approot, startfile)

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
        self.utils.check_setting(self.settings,
                           required_item_list=['githost', 'gitfileset', 'gitmotd', 'localfilesets',
                                               'run_non_fileset_folders', 'gui_show_dev_filesets'],
                           defaults=['https://raw.githubusercontent.com/tdcoa/sql/master/',
                                     'filesets.yaml',
                                     'motd.txt',
                                     '{download}/filesets.yaml',
                                     'True',
                                     'False'])

        # add skip_dbs back in as silent (unlisted) option
        self.skip_dbs = False
        if 'skip_dbs' in self.settings:
            if self.settings['skip_dbs'].strip().lower() == 'true':
                self.skip_dbs = True

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

        # download filesets.yaml every instantiation (now that download folder exists)
        if os.path.isfile(self.filesetpath):
            try:
                os.remove(self.filesetpath)
            except FileNotFoundError as e:
                pass

        githost = self.settings['githost']
        if githost[-1:] != '/':
            githost = githost + '/'
        self.utils.log('githost', githost)
        giturl = githost + self.settings['gitfileset']
        self.utils.log('downloading "filesets.yaml" from github')
        self.utils.log('  requesting url', giturl)
        filecontent = requests.get(giturl).content.decode('utf-8')
        savepath = os.path.join(self.approot, self.settings['localfilesets'])
        self.utils.log('saving filesets.yaml', savepath)
        with open(savepath, 'w') as fh:
            fh.write(filecontent)
        self.utils.log('filesets.yaml saved')

        # load filesets dictionary (active only)
        self.utils.log('loading dictionary', 'filesets (active only)')
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

        # load systems (active only)
        self.utils.log('loading system dictionaries (active only)')
        for sysname, sysobject in systemsyaml['systems'].items():
            if self.utils.dict_active(sysobject, sysname):
                self.systems.update({sysname: sysobject})

                # todo add default dbsversion and collection
                self.utils.check_setting(self.systems[sysname],
                                   required_item_list=['active', 'siteid', 'use', 'host', 'username', 'password',
                                                       'logmech', 'driver', 'encryption','manual_run'],
                                   defaults=['True', 'siteid123', 'unknown', 'customer.host.missing.com',
                                             'username_missing', 'password_missing', '', 'sqlalchemy', '', 'False'])

                if 'connectionstring' not in sysobject:
                    if sysobject['logmech'].strip() == '':
                        logmech = ''
                    else:
                        logmech = '/?logmech=%s' % sysobject['logmech']
                    sysobject['connectionstring'] = 'teradatasql://%s:%s@%s%s' % (sysobject['username'],
                                                                                  sysobject['password'],
                                                                                  sysobject['host'],
                                                                                  logmech)

        self.utils.log('done!')
        self.utils.log('time', str(dt.datetime.now()))

    def download_files(self):
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
        file_url = 'file://' + os.path.abspath(os.path.join(self.approot, 'motd.html'))
        webbrowser.open(file_url)

        # delete all pre-existing download folders
        # Commented the below code, in order to make the script download only if the files doesn't exist
        self.utils.recursively_delete_subfolders(os.path.join(self.approot, self.folders['download']))

        # set proper githost for filesets
        githost = githost + 'filesets/'

        # iterate all active systems.filesets:
        for sysname, sysobject in self.systems.items():
            if self.utils.dict_active(sysobject, sysname, also_contains_key='filesets'):
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

                                            # save logic for non binary write files
                                            if any(x in file_dict['gitfile'] for x in self.settings['text_format_extensions']):
                                                filecontent = response.text
                                                self.utils.log('    saving file to', savefile)
                                                with open(savefile, 'w') as fh:
                                                    fh.write(filecontent)

                                            # save logic for binary write files
                                            else:
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
        # Update the Sourcesystem yaml file with all the available filesets in the filesets.yaml
        if os.path.exists(self.filesetpath) and os.path.exists(self.systemspath):
            self.update_sourcesystem_yaml()
        self.utils.log('\ndone!')
        self.utils.log('time', str(dt.datetime.now()))

        self.copy_download_to_sql()

    def copy_download_to_sql(self, overwrite=False):
        self.utils.log('copy_download_to_sql started', header=True)
        self.utils.log('copy files from download folder (by fileset) to sql folder (by system)')
        self.utils.log('time', str(dt.datetime.now()))
        downloadpath = os.path.join(self.approot, self.folders['download'])
        sqlpath = os.path.join(self.approot, self.folders['sql'])

        self.utils.recursively_delete_subfolders(sqlpath)

        for sysname, sysobject in self.systems.items():
            self.utils.log('processing system', sysname)
            if self.utils.dict_active(sysobject, also_contains_key='filesets'):
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

                if sysfolder not in self.systems or self.utils.dict_active(self.systems[sysfolder]) is False:
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
                                        if setfolder in self.systems[sysfolder]['filesets']:
                                            sub_dict = self.systems[sysfolder]['filesets'][setfolder]
                                            if self.utils.dict_active(sub_dict, 'system-fileset overrides'):
                                                runfiletext = self.utils.substitute(runfiletext, sub_dict,
                                                                              subname='system-fileset overrides (highest priority)')

                                        # SUBSTITUTE values for: system-defaults [source_systems.yaml]
                                        sub_dict = self.systems[sysfolder]
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

    def make_output_folder(self, name=''):
        outputpath = os.path.join(self.approot, self.folders['output'],
                                  str(dt.datetime.now())[:-7].replace(' ', '_').replace(':', ''))
        if name.strip() != '':
            name = '-%s' % str(re.sub('[^0-9a-zA-Z]+', '_', name.strip()))
        outputpath = outputpath + name
        os.makedirs(outputpath)

        return outputpath

    def execute_run(self, name=''):
        self.utils.log('execute_run started', header=True)
        self.utils.log('time', str(dt.datetime.now()))

        # TODO: paramterize final database location (adlste_wetcomm should be {})
        #  when building out the upload_manifest.json, so EMEA and APAC can use

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

                                                    # ODBC CONNECTION ONLY:
                                                    # df.read_sql() does not work properly for odbc connections.
                                                    # Can directly execute using odbc connection but then there are no column names
                                                    # This code block retrieves the column names before saving the csv file
                                                    # Col names will be merged upon save
                                                    col_names = []
                                                    if conn['type'] not in ('sqlalchemy', 'teradataml'):
                                                        col_names = self.utils.open_sql(conn, sql, columns=True, skip = self.skip_dbs)

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
                                                    if conn['type'] in ('sqlalchemy', 'teradataml'):
                                                        df.to_csv(csvfile)  # <---------------------- Save to .csv
                                                    else:  # if odbc conn type, merge in column names
                                                        df.to_csv(csvfile, header=col_names)  # <---------------------- Save to .csv
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
                                                        os.system('python %s' % vis_file)
                                                        self.utils.log('Vis file complete!')

                                                if 'pptx' in sqlcmd:  # insert to pptx file
                                                    self.utils.log('\npptx cmd', 'found')
                                                    pptx_file = os.path.join(workpath, sqlcmd['pptx'])
                                                    self.utils.log('pptx file', pptx_file)
                                                    self.utils.log('inserting to pptx file..')
                                                    self.utils.insert_to_pptx(pptx_file, workpath)
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
        shutil.move(runlogsrc, runlogdst)


    def make_customer_files(self, name=''):
        self.utils.log('make_customer_files started', header=True)
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

        self.utils.log('creating upload manifest file')
        with open(os.path.join(outputpath, 'upload-manifest.json'), 'a') as manifest:
            manifest.write('{"entries":[ ')

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

        # connect to Transcend using TeradataML lib, for fast bulk uploads
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
            skip = self.skip_dbs)  # <--------------------------------- Connect

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

                            # APPEND data to database, via teradataml bulk upload:
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
                                    if not self.skip_dbs:
                                        copy_to_sql(dfcsv, entry['table'] + '_%s' % self.unique_id, entry['schema'], if_exists='replace')
                                    self.utils.log('complete', str(dt.datetime.now()))

                                    # load to GTT
                                    self.utils.log('\nload to GTT', entry['schema'] + '.' + entry['table'])
                                    if not self.skip_dbs:
                                        transcend['connection'].execute("""
                                        INSERT INTO {db}.{table} SELECT * FROM {db}.{unique_table}
                                        """.format(db=entry['schema'],
                                                   table=entry['table'],
                                                   unique_table=entry['table'] + '_%s' % self.unique_id))
                                    self.utils.log('complete', str(dt.datetime.now()))
                                    successful_load = True

                                # write_to_perm = False
                                # steps:
                                # 1. load into pre-created GTT table
                                #    1a. (will auto-create perm table if GTT doesnt exist)
                                # 2. call sp on GTT to merge to final table
                                else:
                                    self.utils.log('write_to_perm', 'False')
                                    if not self.skip_dbs:
                                        copy_to_sql(dfcsv, entry['table'], entry['schema'], if_exists='append')
                                    self.utils.log('complete', str(dt.datetime.now()))
                                    successful_load = True

                            except Exception as err:
                                self.utils.log('\nERROR during UPLOAD', error=True)
                                self.utils.log(str(err))
                                self.utils.log('   (error repeated below)')
                                self.utils.log('\n    first 10 records of what was being uploaded (dataframe):')
                                self.utils.log(dfcsv[0:10])
                                self.utils.log('')
                                sql = ["Select ColumnName, ColumnType, ColumnFormat, ColumnLength, ColumnId",
                                       "from dbc.columns ", "where databasename = '%s' " % entry['schema'],
                                       "  and tablename = '%s' " % entry['table']]
                                # sql.append("order by ColumnID;")
                                sql = '\n'.join(sql)
                                self.utils.log(sql)
                                df = DataFrame.from_query(sql)
                                df = df.sort(['ColumnId'], ascending=[True])
                                self.utils.log('\n\n    structure of destination table:')
                                print(df)
                                self.utils.log('\n')
                                exit()  # todo possibly remove so that whole process doesnt stop on error?

                            # CALL any specified SPs only if data loaded successfully:
                            if str(entry['call']).strip() != "" and successful_load:
                                self.utils.log('\nStored Proc', str(entry['call']))
                                try:
                                    if not self.skip_dbs:
                                        transcend['connection'].execute('call %s ;' % str(entry['call']))
                                    self.utils.log('complete', str(dt.datetime.now()))

                                    # if write_to_perm == true, drop unique perm table after successful sp call
                                    if self.settings['write_to_perm'].lower() == 'true':
                                        self.utils.log('\ndrop unique perm table', entry['schema'] + '.' + entry['table'] + '_%s' % self.unique_id)

                                        if not self.skip_dbs:
                                            transcend['connection'].execute("""
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
        tmp.append('  run_non_fileset_folders: "True"')
        tmp.append('  write_to_perm: "True"')
        return '\n'.join(tmp)

    def yaml_systems(self):
        tmp = []
        tmp.append('systems:')
        tmp.append('  Transcend_Source:')
        tmp.append('    siteid:      "TDCLOUD14TD03"  ')
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
        tmp.append('      level1_how_much:')
        tmp.append('        active: "False"')
        tmp.append('        startdate:  "Current_Date - 365"')
        tmp.append('        enddate:    "Current_Date - 1"')
        tmp.append('      dbql_core:')
        tmp.append('        active: "False"')
        tmp.append('        startdate:  "Current_Date - 45"')
        tmp.append('        enddate:    "Current_Date - 1"')
        return '\n'.join(tmp)
