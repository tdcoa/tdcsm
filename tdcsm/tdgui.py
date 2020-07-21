import subprocess, platform, os, copy, yaml
from datetime import datetime
from tkinter import *
from tkinter.ttk import *
from PIL import Image
from PIL import ImageTk
from tdcsm.tdcoa import tdcoa
import tdcsm

class coa():

    version = "0.2.0.3"
    debug = False

    entryvars = {}
    defaults = {}
    images = {'banner':{'file':'pic_TDCOA_Banner.gif', 'X':700, 'Y':27, 'scale':1, 'object':None, 'alttext':'Teradata CSM Automation'}
              ,'logo' :{'file':'pic_TDCOAdot.gif', 'X':330, 'Y':55, 'scale':0.5, 'object':None, 'alttext':'Teradata'}}
    sampleTx2 = {'systems_left':['one','two','three'],
                 'systems_right':['four','five','six'],
                 'filesets_left':['one','two','three'],
                 'filesets_right':['four','five','six']}
    approot = ''
    secrets = ''


    def __init__(self, approot='', secrets=''):
        print('GUI for TDCOA started')
        #self.version = str(datetime.now()).replace('-','').replace(':','').split('.')[0].replace(' ','.')
        if approot != '': self.defaults['approot'] = approot
        if secrets != '': self.defaults['secrets'] = secrets
        if platform.system()[:3]=='Win':
            self.localos='Win'
        else:
            self.localos='Mac'
        self.run()

    def set_defaults(self, **kwargs):
        ev = self.entryvars
        for name, default in kwargs.items():
            self.defaults[name] = default
        if 'approot'  not in self.defaults: self.defaults['approot']  = os.getcwd()
        if 'config'   not in self.defaults: self.defaults['config']   = 'config.yaml'
        if 'systems'  not in self.defaults: self.defaults['systems']  = 'source_systems.yaml'
        if 'filesets' not in self.defaults: self.defaults['filesets'] = '%s%sfilesets.yaml' %('1_download', os.sep)
        if 'secrets'  not in self.defaults: self.defaults['secrets']  = self.first_file_that_exists(
                                     os.path.join(self.defaults['approot'], '..','!secrets.yaml')
                                    ,os.path.join(self.defaults['approot'], '..', 'secrets.yaml')
                                    ,os.path.join(self.defaults['approot'], 'secrets.yaml'))
        self.defaults['secrets'] = self.defaults['secrets'].replace(self.defaults['approot']+os.sep,'')
        for name, default in self.defaults.items():
            if name not in self.entryvars: self.entryvars[name] = StringVar()
            self.entryvars[name].set(default)


# =================== BEGIN: MAKE NEW GUI OBJECT COLLECTIONS ==============================
    def define_styles(self, app=None):
        Style(app).theme_use('clam') #clam, alt, default, classic, aqua
        colors = {'quickrun':'#FBFE33', 'config':'#FFB2F7', 'download':'#7EFDFF', 'execute':'#FEBF25',  'upload':'#FE2525', 'help':'#D7DCBA'}
        font = 'Open Sans'
        Style(app).configure("TButton"    ,foreground="#ffffff", background="#646464", font=(font, '12')  )
        Style(app).configure("TFrame"     ,foreground="#ffffff", background="#000000", font=(font, '12')  )
        Style(app).configure("TNotebook"  ,foreground="#ffffff", background="#000000", font=(font, '12')  )
        Style(app).configure("TLabel"     ,foreground="#ffffff", background="#000000", font=(font, '12')  )
        Style(app).configure("title.TLabel",foreground="#ffffff", background="#000000", font=(font,'24', 'bold')  )

        Style(app).configure("quickrun-normal.TFrame"       ,foreground="#000000", background=colors['quickrun'], font=(font, '12')  )
        Style(app).configure("quickrun-normal.TButton"      ,foreground="#000000", background="#B1B804", font=(font, '12')  )
        Style(app).map("quickrun-normal.TButton", background=[("disabled","#B1B804")])
        Style(app).configure("quickrun-important.TButton"   ,foreground="#000000", background="#E7B525", font=(font, '12', 'bold')  )
        Style(app).configure("quickrun-normal.TCheckbutton" ,foreground="#000000", background=colors['quickrun'])
        Style(app).configure("quickrun-separator.TFrame"    ,foreground="#000000", background=colors['quickrun'], font=(font, '12')  )
        Style(app).configure("quickrun-normal.TLabel"       ,foreground="#000000", background=colors['quickrun'], font=(font, '12')  )
        Style(app).configure("quickrun-bold.TLabel"         ,foreground="#000000", background=colors['quickrun'], font=(font, '12', 'bold')  )
        Style(app).configure("quickrun-header.TLabel"       ,foreground="#000000", background=colors['quickrun'], font=(font, '24', 'bold')  )
        Style(app).configure("quickrun-normal.Treeview"     ,foreground="#000000", background="#FDFEDD", font=(font, '12')  )
        Style(app).configure("quickrun-normal.TEntry"       ,foreground="#000000", fieldbackground="#FDFEDD", font=(font, '12')  )

        Style(app).configure("config-normal.TFrame"         ,foreground="#000000", background=colors['config'], font=(font, '12')  )
        Style(app).configure("config-normal.TLabel"         ,foreground="#000000", background=colors['config'], font=(font, '12')  )
        Style(app).configure("config-bold.TLabel"           ,foreground="#000000", background=colors['config'], font=(font, '12', 'bold')  )

        Style(app).configure("download-normal.TFrame"       ,foreground="#000000", background=colors['download'], font=(font, '12')  )
        Style(app).configure("download-normal.TLabel"       ,foreground="#000000", background=colors['download'], font=(font, '12')  )
        Style(app).configure("download-bold.TLabel"         ,foreground="#000000", background=colors['download'], font=(font, '12', 'bold')  )

        Style(app).configure("execute-normal.TFrame"        ,foreground="#000000", background=colors['execute'], font=(font, '12')  )
        Style(app).configure("execute-normal.TLabel"        ,foreground="#000000", background=colors['execute'], font=(font, '12')  )
        Style(app).configure("execute-bold.TLabel"          ,foreground="#000000", background=colors['execute'], font=(font, '12', 'bold')  )

        Style(app).configure("upload-normal.TFrame"         ,foreground="#000000", background=colors['upload'], font=(font, '12')  )
        Style(app).configure("upload-normal.TLabel"         ,foreground="#000000", background=colors['upload'], font=(font, '12')  )
        Style(app).configure("upload-bold.TLabel"           ,foreground="#000000", background=colors['upload'], font=(font, '12', 'bold')  )

        Style(app).configure("help-normal.TFrame"           ,foreground="#000000", background=colors['help'], font=(font, '12')  )
        Style(app).configure("help-normal.TLabel"           ,foreground="#000000", background=colors['help'], font=(font, '12')  )
        Style(app).configure("help-bold.TLabel"             ,foreground="#000000", background=colors['help'], font=(font, '12', 'bold')  )

    def newframe_LEB(self, parent, labeltext='not set', btntext='not set', btncommand='test', style = 'default', lbl_width=12, btn_width=6):
        if btncommand not in self.entryvars: self.entryvars[btncommand] = StringVar()
        f = Frame(parent, padding=1, style=str('%s.TFrame' %style))
        l = Label(f, text=labeltext, width = lbl_width, anchor=E, style=str('%s.TLabel' %style))
        e = Entry(f, textvariable=self.entryvars[btncommand], style=str('%s.TEntry' %style))
        b = Button(f,text=btntext, command=lambda:self.button_click(btncommand, entrytext=self.entryvars[btncommand].get()), width=btn_width, style=str('%s.TButton' %style))
        l.pack(side=LEFT, fill=BOTH, expand=False, padx=0, pady=0)
        e.pack(side=LEFT, fill=BOTH, expand=True , padx=0, pady=0)
        b.pack(side=LEFT, fill=BOTH, expand=False, padx=0, pady=0)
        if btntext == '':
            b.state(["disabled"])
        if self.debug: print('created LEB: %s' %labeltext)
        return  f

    def newframe_CLB(self, parent, labeltext='', btntext = 'not set', btncommand='test', checkcommand=print('check command'), style = 'default'):
        f = Frame(parent, padding=1, style=str('%s.TFrame' %style))
        if btncommand not in self.entryvars: self.entryvars[btncommand] = IntVar(value=0)
        c = Checkbutton(f, variable=self.entryvars[btncommand], command=checkcommand, style=str('%s.TCheckbutton' %style))
        if labeltext != '': l = Label(f, text=labeltext, anchor=E, style=str('%s.TLabel' %style))
        b = Button(f,text=btntext, command=lambda:self.button_click(btncommand, state=self.entryvars[btncommand].get()), style=str('%s.TButton' %style))
        c.pack(side=LEFT, expand=False)
        if labeltext != '': l.pack(side=LEFT, expand=False)
        b.pack(side=RIGHT, fill=BOTH, expand=True)
        if self.debug: print('created CB: %s' %btntext)
        return f

    def newbutton(self, parent, btntext = 'not set', btncommand='test', command=print('check command'), btnwidth=15, style = 'default', side=RIGHT):
        if btncommand not in self.entryvars: self.entryvars[btncommand] = IntVar(value=0)
        b = Button(parent,text=btntext, command=lambda:self.button_click(btncommand), width=btnwidth, style=str('%s.TButton' %style))
        b.pack(side=side)
        return b

    def newframe_Tx2(self, parent, treetext='not set', treelabel_left='left tree', treelabel_right='right tree', width=10, treeheight=5, style = 'default'):
        f = Frame(parent,padding=3, style=str('%s.TFrame' %style))
        Label(parent, text=treetext, anchor=CENTER, style=str('%s.TLabel' %style)).pack(side=TOP, fill=BOTH, expand=False)
        leftname  = 'tv_%s_left'  %treetext.replace(' ','_').lower().strip()
        rightname = 'tv_%s_right' %treetext.replace(' ','_').lower().strip()
        tL = Treeview(f, height=treeheight, style=str('%s.Treeview' %style))
        tR = Treeview(f, height=treeheight, style=str('%s.Treeview' %style))
        tL.column("#0", width=100, minwidth=75)
        tR.column("#0", width=100, minwidth=75)
        tL.heading("#0",text=treelabel_left,  anchor=W)
        tR.heading("#0",text=treelabel_right, anchor=W)
        tL.bind("<<TreeviewSelect>>", lambda event: self.button_click(name=leftname , selected=tL.item(tL.focus())['text'] ))
        tR.bind("<<TreeviewSelect>>", lambda event: self.button_click(name=rightname, selected=tR.item(tR.focus())['text'] ))
        self.entryvars[leftname]  = tL
        self.entryvars[rightname] = tR
        tL.pack(side=LEFT,  fill=BOTH, expand=True)
        tR.pack(side=RIGHT, fill=BOTH, expand=True)
        if self.debug: print('created Tx2: %s' %treetext)
        return f

    def separator(self, parent, style='default', orient='h', width=3):
        o = HORIZONTAL
        if orient[:1].strip().lower() == 'v': o = VERTICAL
        #s = Separator(parent, orient=o, style='%s.TSeparator' %style)
        s = Frame(parent, borderwidth=width, style='%s-sep.TFrame' %style)
        s.pack(fill=X, expand=True)

    def newImage(self, parent, image_name='', img_width=100, img_height=100):
        i = self.images[image_name]
        x = int(i['X']*i['scale'])
        y = int(i['Y']*i['scale'])
        c = Canvas(parent, width=x+20, height=y+20, bg='black', bd=0, highlightthickness=0)
        pix = os.path.join(os.path.dirname(tdcsm.__file__), i['file'])
        try:
            img = Image.open(pix).resize((x,y), Image.ANTIALIAS)
            i['object'] = ImageTk.PhotoImage(img)
            c.create_image(10,10, anchor=NW, image=i['object'])
            print('created Image: %s' %image_name)
        except:
            print('Image Load Failed:', pix)
            Label(c, text=i['alttext'], anchor=CENTER).pack(side=TOP, fill=BOTH, expand=True)
        return c
# =================== END: MAKE NEW GUI OBJECT COLLECTIONS ==============================



# =================== START: HELPER FUNCTIONS =============================
#  these probably ought be added to util in the future

    def first_file_that_exists(self, *args):
        for file in args:
            if os.path.isfile(file): return file
        return 'NONE_FOUND'

    def print_dict(self, dict2print={}, lvl=0):
        """Simply prints dictionaries in a more nested/human readable form"""
        if lvl==0: print('-'*30)
        for n,v in dict2print.items():
            if type(v) == dict:
                print('%s%s:' %('  '*lvl, n))
                self.print_dict(v, lvl+1)
            else:
                print('%s%s:  %s'  %('  '*lvl, n, v))
        if lvl==0: print('-'*30)

    def split_dict(self, dict2split={}, delim_key='active', use_subdict='', default='<missing>', addifmissing=[]):
        """splits a supplied dictionary into multiple dictionaries based on some child value,
        then adds the split dictionaries under new parents for return, per delim_key found.
        For example:
        pets = {}
        pets['spot'] =  {'active':'True',  'type':'dog'}
        pets['jane'] =  {'active':'True',  'type':'cat'}
        pets['lucky'] = {'active':'False', 'type':'cat', 'legs':'3'
                        ,'fleas':{
                            'bobby': {'active':'False'},
                            'susie': {'active':'False'},
                            'bitey': {'active':'True'} }}

        print('By active key')
        print( split_dict(pets, delim_key='active'))
            {'True': {  'spot': {'active': 'True', 'type': 'dog'},
                        'jane': {'active': 'True', 'type': 'cat'}},
             'False': { 'lucky': {'active': 'False', 'type': 'cat', 'legs': '3'}}}

        print('By type key')
        print( split_dict(pets, delim_key='type'))
            {'dog': {   'spot': {'active': 'True', 'type': 'dog'}},
             'cat': {   'jane': {'active': 'True', 'type': 'cat'},
                        'lucky': {'active': 'False', 'type': 'cat', 'legs': '3'}}}

        # can also add default values, if delim_key is not found:
        print('By leg count, with default')
        print( split_dict(pets, delim_key='legs', default='4' ))
            {'1':   {   'spot': {'active': 'True', 'type': 'dog'}},
             '0.5': {   'jane': {'active': 'True', 'type': 'cat'},
                        'lucky': {'active': 'False', 'type': 'cat', 'legs': '3'}}}

        # can also use child dictionaries, instead of supplied dictionary:
        print('For fleas sub-dictionary, if found')
        print( split_dict(pets, delim_key='active', use_subdict='fleas' ))
            {'1':   {   'spot': {'active': 'True', 'type': 'dog'}},
             '0.5': {   'jane': {'active': 'True', 'type': 'cat'},
                        'lucky': {'active': 'False', 'type': 'cat', 'legs': '3'}}}
        """
        rtn = {}

        # build working dict
        workdict = {}
        if (use_subdict == ''):
            workdict = copy.deepcopy(dict2split)
        else:
            for pname, pdict in dict2split.items():
                if use_subdict in pdict:
                    for cname, cdict in pdict[use_subdict].items():
                        if delim_key in cdict:
                            workdict[cname] = cdict

        # build return dict
        for pname, childdict in workdict.items():
            if delim_key in childdict:
                if str(childdict[delim_key]) not in rtn: rtn[str(childdict[delim_key])]={}
                rtn[str(childdict[delim_key])][pname] = childdict
            else:
                if str(default) not in rtn: rtn[default]={}
                rtn[str(default)][pname] = childdict
        for aim in addifmissing:
            if aim not in rtn:  rtn[aim]={}
        return rtn

    def open_text_file(self, filepath, pathprefix=''):
        pth = os.path.join(pathprefix, filepath)
        print('Opening File in Default Editor:', pth)
        if self.localos=='Win':
            os.system("notepad.exe %s" %pth)
        else:
            subprocess.call(['open', pth])

    def open_folder_explorer(self, folderpath, pathprefix='', createifmissing=False):
        pth = os.path.join(pathprefix, folderpath)
        if not os.path.exists(pth): os.mkdir(pth)
        print('Opening Folder in File Explorer:', pth)
        if self.localos=='Win':
            os.system('%s' %pth)
        else:
            subprocess.call(['open', pth])

    def yaml_write(self, dict2write={}, filepath=''):
        with open(filepath, 'w') as fh:
            fh.write(yaml.dump(dict2write))

    def yaml_read(self, filepath=''):
        if os.path.isfile(filepath):
            with open(filepath, 'r') as fh:
                txt = fh.read()
        else:
            txt = 'no_such_file: "%s"' %filepath
        return yaml.load(txt)

# =================== END: HELPER FUNCTIONS ==============================



# =================== START: PROGRAM BEHAVIOR ==============================
    def close(*args):
        print("GUI Closed")
        exit()

    def entryvar(self, varname=''):
        if varname in self.entryvars:
            rtn = self.entryvars[varname].get()
        else:
            rtn = '<missing>'
            print('entryvar missing: %s' %varname)
        if self.debug: print(varname, rtn)
        return rtn

    def reload_Tx2(self, treetext='not set', leftlist=[], rightlist=[], exclude=[]):
        intrs = {str('tv_%s_left' %treetext):leftlist, str('tv_%s_right' %treetext):rightlist}
        for nm, lst in intrs.items():
            self.entryvars[nm].delete(*self.entryvars[nm].get_children())
            for itm in lst:
                if itm not in exclude:
                    self.entryvars[nm].insert('','end',text=str(itm))

    def upload_get_lastrun_folder (self, lastrunfile='.last_run_output_path.txt'):
        lastrunfile = os.path.join(self.entryvar('approot'), lastrunfile)
        if os.path.exists(lastrunfile):
            print('FOUND', lastrunfile)
            with open(lastrunfile,'r') as fh:
                outputfo = str(fh.read())
            print(outputfo)
            lastrunfolder = os.path.join(self.entryvar('approot'), outputfo)
            print(lastrunfolder)
            if os.path.exists(lastrunfolder):
                if  'last_output_folder' in self.entryvars:
                    self.entryvars['last_output_folder'].set(outputfo)
                return outputfo
        print('NOT FOUND:', lastrunfile)
        return '<missing>'

    def toggle_all_chk_quickrun(self):
        ev = self.entryvars
        if  ev['download_files'].get() == 1 \
        and ev['prepare_sql'].get() == 1 \
        and ev['execute_run'].get() == 1 \
        and ev['upload_to_transcend'].get() == 1 :
            # if all boxes checked, then uncheck:
            ev['download_files'].set(0)
            ev['prepare_sql'].set(0)
            ev['execute_run'].set(0)
            ev['make_customer_files'].set(0)
            ev['upload_to_transcend'].set(0)
            ev['run_all_checked'].set(0)
        else:   # otherwise, check all:
            ev['download_files'].set(1)
            ev['prepare_sql'].set(1)
            ev['execute_run'].set(1)
            ev['upload_to_transcend'].set(1)
            ev['run_all_checked'].set(1)

    def systems_save2disk(self):
        # --> stupid langGO error  >:^(
        # internal langGo processes throws wicked errors when called from inside TK
        # works fine in script, so this is a work-around until that is fixed  :^(
        # affects  coa.execute_run() and coa.upload_to_transcend
        # ALSO
        # we need to write source_systems.yaml to disk for the subprocess to pick up
        # but tdcoa does not keep inactive systems in-memory, which means we can't
        # just materialize coa.systems to disk directly, or we'll end up erasing
        # inactive systems from people's source_systems.yaml file.
        # Until tdcoa can be adjusted to keep inactive SYSTEMS in memory (coa.systems)
        # this process merges adjusted (active) and inactive dictionaries before
        # saving to disk.   Whew.

        # load from disk, and filter out only INACTIVE systems:
        syspath =os.path.join( self.entryvar('approot'), self.entryvar('systems'))
        sysdict = self.split_dict(self.yaml_read(syspath)['systems'],'active', default='True')
        if 'False' not in sysdict:
            sysInactive = {}
        else:
            sysInactive = sysdict['False']

        # merge new inactive systems with active systems, in-memory
        sysAll = {}
        sysAll['systems'] = {}
        for sysname, sysdict in self.coa.systems.items():
            sysAll['systems'][sysname] = sysdict
        for sysname, sysdict in sysInactive.items():
            if sysname not in sysAll['systems']:
                sysAll['systems'][sysname] = sysdict

        # write back to disk, so subprocess can pick up
        self.yaml_write(sysAll, syspath)

    def systems_readfromdisk(self):
        syspath =os.path.join( self.entryvar('approot'), self.entryvar('systems'))
        sysdict = self.yaml_read(syspath)['systems']
        self.coa.systems = sysdict

    def run_external(self, coa_function='execute_run()'):
        # --> stupid langGO error  >:^(
        # internal langGo processes throws wicked errors when called from inside TK
        # works fine in script, so this is a work-around until that is fixed  :^(
        # affects  coa.execute_run() and coa.upload_to_transcend

        self.coa.systems = self.split_dict(self.coa.systems, 'active', addifmissing=['True','False'])['True']
        if self.coa.systems != {}:
            # build and execute subprocess
            cmd = []
            cmd.append("from tdcsm.tdcoa import tdcoa;")
            cmd.append("c=tdcoa(approot='%s'," %self.entryvar('approot').replace('\\','\\\\'))
            cmd.append("config='%s',"   %self.entryvar('config'))
            cmd.append("systems='%s',"  %self.entryvar('systems'))
            cmd.append("secrets='%s');" %self.entryvar('secrets'))
            coacmd = coa_function.strip()
            if coacmd[:2] != 'c.': coacmd = 'c.%s' %coacmd
            if coacmd[-1:] != ')': coacmd = '%s()' %coacmd
            cmd.append(coacmd)
            cmd = ' '.join(cmd)
            print(cmd)
            os.system('python -c "%s"' %cmd)

    def button_click(self, name='', **kwargs):
        print('button clicked',  name)
        argstr = ''
        for n,v in kwargs.items():
            #argstr += ' - %s: %s\n' %(n,v)
            argstr += " - kwargs['%s'] = '%s'\n" %(n,v)
        print('%s' %argstr)

        if name == 'reload_config':
            self.systems_save2disk()
            self.coa.approot      = self.entryvar('approot')
            self.coa.configpath   = os.path.join(self.coa.approot, self.entryvar('config'))
            self.coa.secretpath   = os.path.join(self.coa.approot, self.entryvar('secrets'))
            self.coa.systemspath  = os.path.join(self.coa.approot, self.entryvar('systems'))
            self.coa.filesetpath  = os.path.join(self.coa.approot, self.entryvar('filesets'))
            self.coa.reload_config()
            print('approot: ', self.coa.approot)
            print('config: ', self.coa.configpath)
            print('secret: ', self.coa.secretpath)
            print('system: ', self.coa.systemspath)
            print('fileset: ', self.coa.filesetpath)
            self.systems_readfromdisk()
            self.button_click('tv_systems_left') # this 'click' will refresh both left and right treeviews
            self.button_click('tv_filesets_left')
        elif name == 'approot':
            self.open_folder_explorer(kwargs['entrytext'], createifmissing=True)
        elif name == 'download_files':
            self.coa.download_files()
        elif name == 'prepare_sql':
            self.coa.prepare_sql()
        elif name == 'execute_run':
            self.systems_save2disk()
            self.run_external('execute_run')
            self.upload_get_lastrun_folder()
            #self.coa.execute_run()
        elif name == 'make_customer_files':
            self.coa.make_customer_files()
        elif name == 'upload_to_transcend':
            self.systems_save2disk()
            self.run_external('upload_to_transcend')
            #self.coa.execute_run()
        elif name == 'last_output_folder':
            self.open_folder_explorer(os.path.join(self.entryvar('approot'), kwargs['entrytext']), createifmissing=True)
        elif name == 'run_all_checked':
            if self.entryvar('download_files')      ==1: self.button_click('download_files')
            if self.entryvar('prepare_sql')         ==1: self.button_click('prepare_sql')
            if self.entryvar('execute_run')         ==1: self.button_click('execute_run')
            if self.entryvar('make_customer_files') ==1: self.button_click('make_customer_files')
            if self.entryvar('upload_to_transcend') ==1: self.button_click('upload_to_transcend')

        elif name in ['config','systems','filesets']:
            self.open_text_file(kwargs['entrytext'], self.entryvar('approot'))

        elif name in ['tv_systems_left','tv_systems_right']:
            if 'selected' in kwargs.keys(): # if item was "selected" kwargs will return which item (else refresh without change)
                active = 'True'
                if name[-4:] == 'left':  active = 'False' # left is active, so click is moving to inactive
                self.coa.systems[kwargs['selected']]['active'] = active
            d = self.split_dict(self.coa.systems, 'active', default='True', addifmissing=['True','False'])
            self.reload_Tx2('systems', leftlist = d['True'].keys(), rightlist = d['False'].keys())

        elif name in ['tv_filesets_left','tv_filesets_right']:
            if 'selected' in kwargs.keys():  # if item was "selected" kwargs will return which item (else refresh without change)
                active = 'True'
                if name[-4:] == 'left':  active = 'False'
                for system in self.coa.systems.keys():  # iterate thru all systems, to update the right system.fileset object
                    self.coa.systems[system]['filesets'][kwargs['selected']]['active'] = active
            d = self.split_dict(self.coa.systems, 'active', 'filesets', default='True', addifmissing=['True','False'])
            if 'gui_show_dev_filesets' in self.coa.settings and self.coa.settings['gui_show_dev_filesets'] == 'True':
                exclude = []
            else:
                exclude = self.split_dict(self.coa.filesets, 'show_in_gui', default='True' )['False'].keys()
            self.reload_Tx2('filesets', leftlist = d['True'].keys(), rightlist = d['False'].keys(), exclude=exclude)



# =================== END: PROGRAM BEHAVIOR ==============================




# -------------------- ASSEMBLE GUI FROM ABOVE COMPONENTS ------------------------
    def run(self):
        print('GUI RUN: Setup')

        # SETUP APPLICATION, app, appframe, title
        app = Tk()
        self.app = app
        self.title = "TD Consumption Analytics (COA)"
        self.define_styles(app)
        self.set_defaults()
        app.wm_title(self.title)
        app.title(self.title)
        app.geometry('720x770')

        appframe = Frame(app, style="TFrame"); appframe.pack(fill=BOTH, expand=True)
        self.newImage(appframe, image_name='banner').pack(anchor=NW)
        Label(appframe, style="title.TLabel", text='TD Consumption Analytics (COA)').pack(anchor=NW)

        tabcontrol = Notebook(appframe, padding=5)
        tabcontrol.pack(fill=BOTH, expand=True, anchor=NW)

        Button(appframe, text="Close", command=lambda:self.close(), width=10).pack(side=RIGHT)
        #Label(appframe, style="TLabel", text='version "%s"' %self.version).pack(anchor='center')
        self.newImage(appframe, image_name='logo').pack(side=LEFT)

        tabQuickrun = Frame(tabcontrol, style="quickrun-normal.TFrame"); tabQuickrun.pack(fill=X, expand=True, anchor='n')
        tabConfig   = Frame(tabcontrol, style="default-normal.TFrame") ;   tabConfig.pack(fill=X, expand=True)
        tabDownload = Frame(tabcontrol, style="default-normal.TFrame") ; tabDownload.pack(fill=X, expand=True)
        tabExecute  = Frame(tabcontrol, style="default-normal.TFrame") ;  tabExecute.pack(fill=X, expand=True)
        tabUpload   = Frame(tabcontrol, style="default-normal.TFrame") ;   tabUpload.pack(fill=X, expand=True)
        tabHelp     = Frame(tabcontrol, style="default-normal.TFrame") ;     tabHelp.pack(fill=X, expand=True)
        tabcontrol.add(tabQuickrun, text='Quick Run')
        tabcontrol.add(tabConfig,   text='Config')
        tabcontrol.add(tabDownload, text='Download')
        tabcontrol.add(tabExecute,  text='Execute')
        tabcontrol.add(tabUpload,   text='Upload')
        tabcontrol.add(tabHelp,     text='Help')

        #-------------- TAB: QUICK RUN ------------------
        frmQuickrun_N  = Frame(tabQuickrun, padding=5, style="quickrun-normal.TFrame"); frmQuickrun_N.pack(fill=BOTH, expand=True, anchor=N)
        Label(frmQuickrun_N, text='   Step 1: Check your Config Files:', style='quickrun-bold.TLabel').pack(fill=X, anchor=N)
        self.newframe_LEB(frmQuickrun_N, labeltext=' AppRoot Path:', btntext='Open Folder', btn_width=10, btncommand='approot' , style='quickrun-normal').pack(fill=X,  expand=True)
        self.newframe_LEB(frmQuickrun_N, labeltext='  Config File:', btntext='Open File'  , btn_width=10, btncommand='config'  , style='quickrun-normal').pack(fill=X,  expand=True)
        self.newframe_LEB(frmQuickrun_N, labeltext=' Systems File:', btntext='Open File'  , btn_width=10, btncommand='systems' , style='quickrun-normal').pack(fill=X,  expand=True)
        self.newframe_LEB(frmQuickrun_N, labeltext=' Secrets File:', btntext=''           , btn_width=10, btncommand='secrets' , style='quickrun-normal').pack(fill=X,  expand=True)
        self.newframe_LEB(frmQuickrun_N, labeltext='FileSets File:', btntext='Open File'  , btn_width=10, btncommand='filesets', style='quickrun-normal').pack(fill=X, expand=True)
        self.newbutton(frmQuickrun_N, btntext='Reload Configs', btncommand='reload_config', command=lambda:self.button_click(btncommand), btnwidth=25, side=BOTTOM, style = 'quickrun-normal')

        frmQuickrun_S  = Frame(tabQuickrun, padding=5); frmQuickrun_S.pack(side=BOTTOM, fill=BOTH, expand=True, anchor=S)

        frmQuickrun_SW  = Frame(frmQuickrun_S, padding=5, style="quickrun-normal.TFrame"); frmQuickrun_SW.pack(side=LEFT, fill=BOTH, expand=True, anchor=N)
        Label(frmQuickrun_SW, text='   Step 2: Select Systems and FileSets:', style='quickrun-bold.TLabel').pack(fill=X, anchor=N)
        self.newframe_Tx2(frmQuickrun_SW, treetext='SYSTEMS',  treelabel_left='Active', treelabel_right='Inactive', width=25, treeheight=5, style = 'quickrun-normal').pack(fill=X, expand=True)
        self.newframe_Tx2(frmQuickrun_SW, treetext='FILESETS', treelabel_left='Active', treelabel_right='Inactive', width=25, treeheight=7, style = 'quickrun-normal').pack(fill=X, expand=True)

        frmQuickrun_SE  = Frame(frmQuickrun_S, padding=5, style="quickrun-normal.TFrame"); frmQuickrun_SE.pack(side=RIGHT, fill=BOTH, expand=True, anchor=N)
        Label(frmQuickrun_SE, text='   Step 3: Run the Process:', style='quickrun-bold.TLabel').pack(fill=X, anchor=N)
        #self.newframe_CLB(frmQuickrun_SE, btntext='Reload Configs',      btncommand='reload_configs',      style='quickrun-normal').pack(fill=X, expand=True)
        self.newframe_CLB(frmQuickrun_SE, btntext='Download Files',      btncommand='download_files',      style='quickrun-normal').pack(fill=X, expand=True)
        self.newframe_CLB(frmQuickrun_SE, btntext='Prepare SQL',         btncommand='prepare_sql',         style='quickrun-normal').pack(fill=X, expand=True)
        self.separator(frmQuickrun_SE, style='quickrun-normal', width=8)
        self.newframe_CLB(frmQuickrun_SE, btntext='Execute Run',         btncommand='execute_run',         style='quickrun-normal').pack(fill=X, expand=True)
        self.newframe_CLB(frmQuickrun_SE, btntext='Make Customer Files', btncommand='make_customer_files', style='quickrun-normal', labeltext='No Access?').pack(fill=X, expand=True)
        self.separator(frmQuickrun_SE, style='quickrun-normal', width=8)
        self.newframe_LEB(frmQuickrun_SE, labeltext='Output Folder:', btntext='Open', btncommand='last_output_folder', lbl_width=15, style='quickrun-normal').pack(fill=X, expand=True)
        self.newframe_CLB(frmQuickrun_SE, btntext='Upload to Transcend', btncommand='upload_to_transcend', style='quickrun-normal').pack(fill=X, expand=True)
        self.separator(frmQuickrun_SE, style='quickrun-normal', width=8)
        self.newframe_CLB(frmQuickrun_SE, btntext='Run All Checked', btncommand='run_all_checked', style='quickrun-normal', labeltext='<-- check all', checkcommand=self.toggle_all_chk_quickrun ).pack(fill=X, expand=True)


        #-------------- TAB: CONFIG ------------------
        frmConfig  = Frame(tabConfig, padding=5, style="config-normal.TFrame"); frmConfig.pack(fill=BOTH, expand=True, anchor=N)
        Label(frmConfig, text='COMING SOON!', style='config-bold.TLabel').pack(fill=X, anchor=N)

        #-------------- TAB: DOWNLOAD ------------------
        frmDownload  = Frame(tabDownload, padding=5, style="download-normal.TFrame"); frmDownload.pack(fill=BOTH, expand=True, anchor=N)
        Label(frmDownload, text='COMING SOON!', style='download-bold.TLabel').pack(fill=X, anchor=N)

        #-------------- TAB: EXECUTE ------------------
        frmExecute  = Frame(tabExecute, padding=5, style="execute-normal.TFrame"); frmExecute.pack(fill=BOTH, expand=True, anchor=N)
        Label(frmExecute, text='COMING SOON!', style='execute-bold.TLabel').pack(fill=X, anchor=N)

        #-------------- TAB: UPLOAD ------------------
        frmUpload  = Frame(tabUpload, padding=5, style="upload-normal.TFrame"); frmUpload.pack(fill=BOTH, expand=True, anchor=N)
        Label(frmUpload, text='COMING SOON!', style='upload-bold.TLabel').pack(fill=X, anchor=N)

        #-------------- TAB: HELP ------------------
        frmHelp  = Frame(tabHelp, padding=5, style="help-normal.TFrame"); frmHelp.pack(fill=BOTH, expand=True, anchor=N)
        Label(frmHelp, text='MORE COMING SOON!', style='help-bold.TLabel').pack(fill=X, anchor=N)
        Label(frmHelp, text='Version of tdgui = %s' %self.version, style='help-bold.TLabel').pack(fill=X, anchor=N)


        #-------------- RUN!!!
        self.coa = tdcoa(approot = self.entryvar('approot'), secrets = self.entryvar('secrets'))
        self.systems_readfromdisk()
        self.button_click('tv_systems_left') # this 'click' will refresh both left and right treeviews
        self.button_click('tv_filesets_left')
        self.upload_get_lastrun_folder()
        Label(frmHelp, text='Version of tdcsm = %s' %self.coa.version, style='help-bold.TLabel').pack(fill=X, anchor=N)

        app.bind('<Escape>', self.close)
        app.mainloop()
