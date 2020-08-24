import subprocess, platform, os, copy #, yaml
from datetime import datetime
from tkinter import *
from tkinter.ttk import *
from PIL import Image
from PIL import ImageTk
from tdcsm.tdcoa import tdcoa
import tdcsm

class coa():

    version = "0.4.0.0.5"
    debug = False

    entryvars = {}
    defaults = {}
    appsize = '600x660' # width x height
    images = {'banner':{'file':'pic_TDCOA_Banner.gif', 'X':700, 'Y':27, 'scale':0.8, 'object':None, 'alttext':'Teradata CSM Automation'}
              ,'logo' :{'file':'pic_TDCOAdot.gif', 'X':330, 'Y':55, 'scale':0.5, 'object':None, 'alttext':'Teradata'}}
    sampleTx2 = {'systems_left':['one','two','three'],
                 'systems_right':['four','five','six'],
                 'filesets_left':['one','two','three'],
                 'filesets_right':['four','five','six']}
    approot = ''
    secrets = ''
    motd = False
    skip_git = False
    show_hidden_filesets = False


    def __init__(self, approot='', secrets=''):
        print('GUI for TDCOA started')
        #self.version = str(datetime.now()).replace('-','').replace(':','').split('.')[0].replace(' ','.')
        if approot != '': self.defaults['approot'] = approot
        if secrets != '': self.defaults['secrets'] = secrets
        if platform.system()[:3]=='Win':
            self.localos='Win'
        else:
            self.localos='Mac'
        self.run_gui()

    def set_defaults(self, **kwargs):
        ev = self.entryvars
        for name, default in kwargs.items():
            self.defaults[name] = default
        if 'approot'  not in self.defaults: self.defaults['approot']  = os.getcwd()
        self.approot = self.defaults['approot']
        if 'config'   not in self.defaults: self.defaults['config']   = 'config.yaml'
        if 'systems'  not in self.defaults: self.defaults['systems']  = 'source_systems.yaml'
        if 'filesets' not in self.defaults: self.defaults['filesets'] = '%s%sfilesets.yaml' %('1_download', os.sep)
        if 'secrets'  not in self.defaults: self.defaults['secrets']  = self.first_file_that_exists(
                                                                         os.path.join('..','!secrets.yaml')
                                                                        ,os.path.join('..', 'secrets.yaml')
                                                                        ,os.path.join('secrets.yaml'))
        self.defaults['secrets'] = self.defaults['secrets'].replace(self.defaults['approot']+os.sep,'')
        for name, default in self.defaults.items():
            if name not in self.entryvars: self.entryvars[name] = StringVar()
            self.entryvars[name].set(default)


# =================== BEGIN: MAKE NEW GUI OBJECT COLLECTIONS ==============================
    def define_styles(self, app=None):
        Style(app).theme_use('clam') #clam, alt, default, classic, aqua
        colors = {  'config'   :'#FBFE33',
                    'normalrun':'#FFB2F7',
                    'assistedrun':'#7EFDFF',
                    'execute'  :'#FEBF25',
                    'upload'   :'#FE2525',
                    'help'     :'#D7DCBA'}
        font = 'Open Sans'
        Style(app).configure("TButton"    ,foreground="#ffffff", background="#646464", font=(font, '12')  )
        Style(app).configure("TFrame"     ,foreground="#ffffff", background="#000000", font=(font, '12')  )
        Style(app).configure("TNotebook"  ,foreground="#ffffff", background="#000000", font=(font, '12')  )
        Style(app).configure("TLabel"     ,foreground="#ffffff", background="#000000", font=(font, '12')  )
        Style(app).configure("title.TLabel",foreground="#ffffff", background="#000000", font=(font,'24', 'bold')  )

        for name, hex in colors.items():
            Style(app).configure("%s-normal.TFrame"       %name, foreground="#000000",   background=colors[name], font=(font, '12')  )
            Style(app).configure("%s-normal.TButton"      %name, foreground="#000000",   background=self.shade(colors[name]), font=(font, '12')  )
            Style(app).map(      "%s-normal.TButton"      %name, background=[("disabled",self.shade(colors[name],0.4))])
            Style(app).configure("%s-normal.TCheckbutton" %name, foreground="#000000",   background=colors[name])
            Style(app).configure("%s-separator.TFrame"    %name, foreground="#000000",   background=colors[name], font=(font, '12')  )
            Style(app).configure("%s-normal.TLabel"       %name, foreground="#000000",   background=colors[name], font=(font, '12')  )
            Style(app).configure("%s-bold.TLabel"         %name, foreground="#000000",   background=colors[name], font=(font, '12', 'bold')  )
            Style(app).configure("%s-header.TLabel"       %name, foreground="#000000",   background=colors[name], font=(font, '24', 'bold')  )
            Style(app).configure("%s-normal.Treeview"     %name, foreground="#000000",   background=self.tint(colors[name],0.8), font=(font, '12')  )
            Style(app).configure("%s-normal.TEntry"       %name, foreground="#000000",   fieldbackground=self.tint(colors[name],0.7), font=(font, '12')  )

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

    def newframe_CLB(self, parent, labeltext='', btntext = 'not set', btncommand='test', checkcommand=print('check command'), style = 'default', show_chkbox=True):
        f = Frame(parent, padding=1, style=str('%s.TFrame' %style))
        if btncommand not in self.entryvars: self.entryvars[btncommand] = IntVar(value=0)
        if show_chkbox:
            c = Checkbutton(f, variable=self.entryvars[btncommand], command=checkcommand, style=str('%s.TCheckbutton' %style))
        else:
            c = Label(f, text="     ", anchor=W, style=str('%s.TLabel' %style)) # widt of missing checkbox
        if labeltext != '': l = Label(f, text=labeltext, anchor=E, style=str('%s.TLabel' %style))
        b = Button(f,text=btntext, command=lambda:self.button_click(btncommand, state=self.entryvars[btncommand].get()), style=str('%s.TButton' %style))
        c.pack(side=LEFT, expand=False)
        if labeltext != '': l.pack(side=LEFT, expand=False)
        b.pack(side=RIGHT, fill=BOTH, expand=True)
        if self.debug: print('created CB: %s' %btntext)
        return f

    def newframe_LC(self, parent, labeltext='', checkcommand='test', style = 'default'):
        f = Frame(parent, padding=1, style=str('%s.TFrame' %style))
        if checkcommand not in self.entryvars: self.entryvars[checkcommand] = IntVar(value=0)
        c = Checkbutton(f, variable=self.entryvars[checkcommand], command=lambda:self.button_click(checkcommand, state=self.entryvars[checkcommand].get()), style=str('%s.TCheckbutton' %style))
        if labeltext != '': l = Label(f, text=labeltext, anchor=E, style=str('%s.TLabel' %style))
        c.pack(side=RIGHT, expand=False)
        if labeltext != '': l.pack(side=LEFT, expand=False)
        if self.debug: print('created CB: %s' %btntext)
        return f

    def newbutton(self, parent, btntext = 'not set', btncommand='test', btnwidth=15, style = 'default', side=RIGHT):
        if btncommand not in self.entryvars: self.entryvars[btncommand] = IntVar(value=0)
        b = Button(parent,text=btntext, command=lambda:self.button_click(btncommand), width=btnwidth, style=str('%s.TButton' %style))
        b.pack(side=side)
        return b

    def newframe_Tx2(self, parent, treetext='not set', treelabel_left='left tree', treelabel_right='right tree', width=10, treeheight=5, style = 'default'):
        f = Frame(parent, padding=6, style=str('%s.TFrame' %style))
        Label(f, padding=0, text=treetext, anchor=S, style=str('%s.TLabel' %style)).pack(side=TOP, expand=False)
        leftname  = 'tv_%s_left'  %treetext.replace(' ','_').replace('(','').replace(')','').lower().strip()
        rightname = 'tv_%s_right' %treetext.replace(' ','_').replace('(','').replace(')','').lower().strip()
        tL = Treeview(f, height=treeheight, style=str('%s.Treeview' %style))
        tR = Treeview(f, height=treeheight, style=str('%s.Treeview' %style))
        tL.column("#0", width=width) #, minwidth=int(width*0.9))
        tR.column("#0", width=width) #, minwidth=int(width*0.9))
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

    def newImage(self, parent, image_name=''):
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
#  TODO: Move all of these to the tdcsm.utils, and import back here.

    def first_file_that_exists(self, *args):
        for file in args:
            if os.path.isfile(os.path.join(self.approot, file)): return file
        return ''

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
             'False': { 'lucky': {'active': 'False', 'type': 'cat', 'legs': '3'
                                  ,'fleas':{'bobby': {'active':'False'},
                                            'susie': {'active':'False'},
                                            'bitey': {'active':'True'} }}}}

        print('By type key')
        print( split_dict(pets, delim_key='type'))
            {'dog': {  'spot': {'active': 'True', 'type': 'dog'}},
             'cat': {  'jane': {'active': 'True', 'type': 'cat'},
                       'lucky': {'active': 'False', 'type': 'cat', 'legs': '3'
                                  ,'fleas':{'bobby': {'active':'False'},
                                            'susie': {'active':'False'},
                                            'bitey': {'active':'True'} }}}}

        # can also add default values, if delim_key is not found:
        print('By leg count, with default')
        print( split_dict(pets, delim_key='legs', default='4' ))
            {'4': {  'spot': {'active': 'True', 'type': 'dog'}
                     'jane': {'active': 'True', 'type': 'cat'}},
             '3': {  'lucky': {'active': 'False', 'type': 'cat', 'legs': '3'
                               ,'fleas':{'bobby': {'active':'False'},
                                         'susie': {'active':'False'},
                                         'bitey': {'active':'True'} }}}}

        # can also use child dictionaries, instead of supplied dictionary:
        print('For fleas sub-dictionary, if found')
        print( split_dict(pets, delim_key='active', use_subdict='fleas', default='False'))
            {'False': { 'bobby': {'active': 'False'},
                        'susie': {'active': 'False'}},
             'True':  { 'bitey': {'active': 'True'}}}

        # can also gaurantee keys in the return set, even if there is no data:
        print('ensure you always have 4 pet types, even if empty')
        print( split_dict(pets, delim_key='type', addifmissing=['cat','dog','bird','gerbil']))
            {'dog': {  'spot': {'active': 'True', 'type': 'dog'}},
             'cat': {  'jane': {'active': 'True', 'type': 'cat'},
                       'lucky': {'active': 'False', 'type': 'cat', 'legs': '3'
                                  ,'fleas':{'bobby': {'active':'False'},
                                            'susie': {'active':'False'},
                                            'bitey': {'active':'True'} }}}
             'bird': {}
             'gerbil': {} }
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
            try:
                os.startfile(pth, "edit")
            except OSError:
                subprocess.run(["notepad", pth])
        else:
            subprocess.call(['open', pth])

    def open_folder_explorer(self, folderpath, pathprefix='', createifmissing=False):
        pth = os.path.join(pathprefix, folderpath)
        if not os.path.exists(pth): os.mkdir(pth)
        print('Opening Folder in File Explorer:', pth)
        if self.localos=='Win':
            os.startfile(pth, "explore")
        else:
            subprocess.call(['open', pth])

    @staticmethod
    def validate_boolean(sbool, returntype = 'string'):
        """accepts a string-as-boolean, and depending on returntype[:3], returns
          - 'str' == well-formed bool as string (default)
          - 'boo' == boolean value itself
          - 'int' == 0 or 1 per boolean standard
        Special note: this is the first step in transitioning YAML from string-as-boolean to pure boolean: isolate the change as-is.
        Once we've wrapped all instances here, we can change the default behavior once and test results."""
        if str(sbool).strip().lower() == 'true':
            if returntype[:3] == 'str': return 'True'
            if returntype[:3] == 'boo': return  True
            if returntype[:3] == 'int': return  1
        elif str(sbool).strip().lower() == 'false':
            if returntype[:3] == 'str': return 'False'
            if returntype[:3] == 'boo': return  False
            if returntype[:3] == 'int': return  0

    @staticmethod
    def safepath(strpath=''):
        """Converts a pre-existing string-path to windows-safe path.
        This is probably really easy, this is just a wrapper for said logic"""
        return str(strpath).replace('/', os.sep).replace(r':\U', r':\\U').strip()

    @staticmethod
    def shade(hexvalue="", pct=0.25):
        """takes one hex color value and returns a shaded/darkened hex color value."""
        h = hexvalue[1:] if hexvalue[:1]=="#" else hexvalue[:6]
        [r,g,b] = tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
        return '#{:02x}{:02x}{:02x}'.format( int(r*(1-pct)), int(g*(1-pct)), int(b*(1-pct)) )

    @staticmethod
    def tint(hexvalue="", pct=0.25):
        """takes one hex color value and returns a tinted/lightened hex color value."""
        h = hexvalue[1:] if hexvalue[:1]=="#" else hexvalue[:6]
        [r,g,b] = tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
        return '#{:02x}{:02x}{:02x}'.format( int(r+((255-r)*pct)), int(g+((255-g)*pct)), int(b+((255-b)*pct)) )

    def print_dict(self, dicttoprint={}, name='', lvl=0, secretdict={}):
        if lvl==0: print('\n%s\n%s\n%s' %('-'*30, str(name).upper(), '-'*30))
        for n,v in dicttoprint.items():
            if isinstance(v, dict):
                if lvl==0: print('')
                print(self.stripsecrets('%s%s:'  %(' '*lvl, n), secretdict))
                self.print_dict(v, lvl=lvl+2, secretdict=secretdict)
            else:
                print(self.stripsecrets('%s%s%s'  %(' '*lvl, str(n+':').ljust(20), str(v)), secretdict))

    def stripsecrets(self, msg='', secretdict={}):
        for nm, secret in secretdict.items():
            if secret in msg:
                msg = msg.replace(secret, '%s%s%s' % (secret[:1], '*' * (len(secret) - 2), secret[-1:]))
        return str(msg)

    # def yaml_write(self, dict2write={}, filepath=''):
    #     with open(filepath, 'w') as fh:
    #         fh.write(yaml.dump(dict2write))
    #
    # def yaml_read(self, filepath=''):
    #     if os.path.isfile(filepath):
    #         with open(filepath, 'r') as fh:
    #             txt = fh.read()
    #     else:
    #         txt = 'no_such_file: "%s"' %filepath
    #     return yaml.load(txt)

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

    def skip_dbs(self):
        return self.validate_boolean(self.coa.settings['skip_dbs'],'bool')

    def reload_Tx2(self, treetext='not set', leftlist=[], rightlist=[], exclude=[]):
        intrs = {str('tv_%s_left' %treetext):leftlist, str('tv_%s_right' %treetext):rightlist}
        for nm, lst in intrs.items():
            self.entryvars[nm].delete(*self.entryvars[nm].get_children())
            for itm in lst:
                if itm not in exclude:
                    self.entryvars[nm].insert('','end',text=str(itm))

    def upload_get_lastrun_folder (self, lastrunfile='.last_run_output_path.txt'):
        lastrunfilepath = os.path.join(self.entryvar('approot'), lastrunfile)
        print("updating 'Output Folder' textbox...")
        # open up breadcrumb file, extract folder location, and return if exists
        if os.path.exists(lastrunfilepath):
            print('breadcrumb found:', lastrunfilepath)
            with open(lastrunfilepath,'r') as fh:
                lastrunfolder = str(fh.read())
            lastrunpath = os.path.join(self.entryvar('approot'), lastrunfolder)

        else:  # if above breadcrumb method fails, default to the most recent output folder found
            outputdir = sorted(os.listdir(os.path.join(self.entryvar('approot'), self.coa.folders['output'])))
            lastrunfolder = 'empty_folder' if len(outputdir)==0 else outputdir[-1]
            print('breadcrumb not valid, retrieving most recent dated folder:', lastrunfolder)
            lastrunfolder = os.path.join(self.coa.folders['output'], lastrunfolder)
            lastrunpath = os.path.join(self.entryvar('approot'), lastrunfolder)
            if not os.path.exists(lastrunpath): os.makedirs(lastrunpath) # just in case the entire output folder is empty

        print('output folder assigned:', lastrunpath)
        self.entryvars['last_output_folder'].set(lastrunfolder)
        return lastrunpath

    def toggle_all_chk_normalrun(self):
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
            ev['make_customer_files'].set(0)
            ev['upload_to_transcend'].set(1)
            ev['run_all_checked'].set(1)
        print('--- check status: ')
        print('  download_files = ' + str(ev['download_files'].get()))
        print('  prepare_sql = ' + str(ev['prepare_sql'].get()))
        print('  execute_run = ' + str(ev['execute_run'].get()))
        print('  make_customer_files = ' + str(ev['make_customer_files'].get()))
        print('  upload_to_transcend = ' + str(ev['upload_to_transcend'].get()))
        print('  run_all_checked = ' + str(ev['run_all_checked'].get()))

    def toggle_all_chk_assistedrun(self):
        ev = self.entryvars
        if  ev['download_files'].get() == 1 \
        and ev['prepare_sql'].get() == 1 \
        and ev['make_customer_files'].get() == 1 :
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
            ev['make_customer_files'].set(1)
            ev['execute_run'].set(0)
            ev['upload_to_transcend'].set(0)
            ev['run_all_checked'].set(1)
        print('--- check status: ')
        print('  download_files = ' + str(ev['download_files'].get()))
        print('  prepare_sql = ' + str(ev['prepare_sql'].get()))
        print('  execute_run = ' + str(ev['execute_run'].get()))
        print('  make_customer_files = ' + str(ev['make_customer_files'].get()))
        print('  upload_to_transcend = ' + str(ev['upload_to_transcend'].get()))
        print('  run_all_checked = ' + str(ev['run_all_checked'].get()))

    def systems_save2disk(self):
        # --> stupid langGO error  >:^(
        # internal langGo processes throws wicked errors when called from inside TK
        # works fine in script, so this is a work-around until that is fixed  :^(
        # affects  coa.execute_run() and coa.upload_to_transcend

        # load from disk, and filter out only INACTIVE systems:
        syspath = self.safepath(os.path.join( self.entryvar('approot'), self.entryvar('systems')))
        sysAll = {}
        sysAll['systems'] = {}
        print('-'*40)
        for sysname, sysdict in self.coa.systems.items():
            print('SAVING:', sysname)
            sysdict['connectionstring'] = '' # removed as it contains password info, regenerated on reload_config()
            sysAll['systems'][sysname] = sysdict
        print('-'*40)

        self.yaml_write(sysAll, syspath)
        self.button_click('reload_config')
        return True

    def run_external(self, coa_function='execute_run()'):
        # --> stupid langGO error  >:^(
        # internal langGo processes throws wicked errors when called from inside TK
        # works fine in script, so this is a work-around until that is fixed  :^(
        # affects  coa.execute_run() and coa.upload_to_transcend
        if self.split_dict(self.coa.systems, 'active', addifmissing=['True','False'])['True'] != {}:
            # build and execute subprocess
            cmd = []
            cmd.append("from tdcsm.tdcoa import tdcoa;")
            cmd.append("c=tdcoa(approot='%s'," %self.safepath( self.entryvar('approot')))
            cmd.append("config='%s',"   %self.safepath(self.entryvar('config')))
            cmd.append("systems='%s',"  %self.safepath(self.entryvar('systems')))
            cmd.append("secrets='%s',"  %self.safepath(self.entryvar('secrets')))
            cmd.append("skip_dbs=%s);"  %str(self.skip_dbs()))

            coacmd = coa_function.strip()
            if coacmd[:2] != 'c.': coacmd = 'c.%s' %coacmd
            if coacmd[-1:] != ')': coacmd = '%s()' %coacmd
            cmd.append(coacmd)
            cmd = '%s -c "%s"' %(self.coa.settings['python_call'], ' '.join(cmd))
            print(cmd)
            os.system(cmd)

    def button_click(self, name='', **kwargs):
        print('button clicked',  name)
        argstr = ''
        for n,v in kwargs.items():
            #argstr += ' - %s: %s\n' %(n,v)
            argstr += " - kwargs['%s'] = '%s'\n" %(n,v)
        print('%s' %argstr)

        try:

            if name == 'reload_config':
                self.coa.approot      = self.entryvar('approot')
                self.coa.configpath   = os.path.join(self.coa.approot, self.entryvar('config'))
                self.coa.secretpath   = os.path.join(self.coa.approot, self.entryvar('secrets'))
                self.coa.systemspath  = os.path.join(self.coa.approot, self.entryvar('systems'))
                self.coa.filesetpath  = os.path.join(self.coa.approot, self.entryvar('filesets'))
                self.coa.reload_config(skip_dbs=self.skip_dbs(), skip_git=self.skip_git)
                print('approot: ', self.coa.approot)
                print('config: ', self.coa.configpath)
                print('secret: ', self.coa.secretpath)
                print('system: ', self.coa.systemspath)
                print('fileset: ', self.coa.filesetpath)
                self.coa.deactivate_all()
                self.entryvars['skip_dbs_toggle'].set(value=self.validate_boolean(self.coa.settings['skip_dbs'],'int'))
                self.upload_get_lastrun_folder()
                self.button_click('tv_systems_left') # this 'click' will refresh both left and right treeviews
                self.button_click('tv_filesets_left')
            elif name == 'approot':
                approot = kwargs['entrytext'].replace(r':\U',r':\\U')
                self.open_folder_explorer(approot, createifmissing=True)
            elif name == 'download_files':
                self.coa.download_files(self.motd)
                self.motd = False
            elif name == 'prepare_sql':
                self.coa.prepare_sql()
            elif name == 'execute_run':
                self.coa.execute_run()
                self.upload_get_lastrun_folder()
            elif name == 'make_customer_files':
                self.coa.make_customer_files2()
                self.upload_get_lastrun_folder()
            elif name == 'process_data':
                self.coa.process_return_data2(os.path.join(self.coa.approot, self.entryvar('last_output_folder')))
            elif name == 'upload_to_transcend':
                with open(os.path.join(self.entryvar('approot'), '.last_run_output_path.txt'), 'w') as fh:
                    fh.write(self.entryvar('last_output_folder'))
                self.coa.upload_to_transcend()
            elif name == 'motd':
                self.coa.display_motd()
            elif name == 'last_output_folder':
                self.open_folder_explorer(os.path.join(self.entryvar('approot'), kwargs['entrytext']), createifmissing=True)
            elif name == 'run_all_checked':
                if self.entryvar('download_files')      ==1: self.button_click('download_files')
                if self.entryvar('prepare_sql')         ==1: self.button_click('prepare_sql')
                if self.entryvar('execute_run')         ==1: self.button_click('execute_run')
                if self.entryvar('make_customer_files') ==1: self.button_click('make_customer_files')
                if self.entryvar('upload_to_transcend') ==1: self.button_click('upload_to_transcend')
            elif name == 'run_all_checked_assist':
                if self.entryvar('download_files')      ==1: self.button_click('download_files')
                if self.entryvar('prepare_sql')         ==1: self.button_click('prepare_sql')
                if self.entryvar('make_customer_files') ==1: self.button_click('make_customer_files')
            elif name in ['config','systems','filesets']:
                self.open_text_file(kwargs['entrytext'], self.entryvar('approot'))
            elif name in ['tv_systems_left','tv_systems_right','tv_systems_assisted_left','tv_systems_assisted_right']:
                if 'selected' in kwargs.keys(): # if item was "selected" kwargs will return which item (else refresh without change)
                    active = 'False' if name[-4:] == 'left' else 'True'
                    self.coa.systems[kwargs['selected']]['active'] = active
                d = self.split_dict(self.coa.systems, 'active', default='True', addifmissing=['True','False'])
                self.reload_Tx2('systems', leftlist = d['True'].keys(), rightlist = d['False'].keys())
                self.reload_Tx2('systems_assisted', leftlist = d['True'].keys(), rightlist = d['False'].keys())
            elif name in ['tv_filesets_left','tv_filesets_right','tv_filesets_assisted_left','tv_filesets_assisted_right']:
                if 'selected' in kwargs.keys():  # if item was "selected" kwargs will return which item (else refresh without change)
                    active = 'False' if name[-4:] == 'left' else 'True'
                    for system in self.coa.systems.keys():  # iterate thru all systems, to update the right system.fileset object
                        self.coa.systems[system]['filesets'][kwargs['selected']]['active'] = active
                d = self.split_dict(self.coa.systems, 'active', 'filesets', default='True', addifmissing=['True','False'])
                if 'gui_show_dev_filesets' in self.coa.settings and self.coa.settings['gui_show_dev_filesets'] == 'True':
                    exclude = []
                elif self.show_hidden_filesets:
                    exclude = []
                else:
                    exclude = self.split_dict(self.coa.filesets, 'show_in_gui', default='True' )['False'].keys()
                self.reload_Tx2('filesets', leftlist = d['True'].keys(), rightlist = d['False'].keys(), exclude=exclude)
                self.reload_Tx2('filesets_assisted', leftlist = d['True'].keys(), rightlist = d['False'].keys(), exclude=exclude)
            elif name == 'skip_dbs_toggle':
                self.coa.settings['skip_dbs'] = bool(kwargs['state'] == 1)
            elif name == 'skip_git_toggle':
                self.skip_git = bool(kwargs['state'] == 1)
            elif name == 'show_hiddenfilesets_toggle':
                self.show_hidden_filesets = bool(kwargs['state'] == 1)
            elif name == 'print_systems':
                self.print_dict(self.coa.systems, 'systems', 0, self.coa.secrets)
            elif name == 'print_filesets':
                self.print_dict(self.coa.filesets, 'filesets', 0, self.coa.secrets)
            elif name == 'print_config':
                self.print_dict(self.coa.substitutions, 'substitutions', 0, self.coa.secrets)
                self.print_dict(self.coa.transcend, 'transcend', 0, self.coa.secrets)
                self.print_dict(self.coa.settings, 'settings', 0, self.coa.secrets)
            elif name == 'print_entryvars':
                for nm, obj in sorted(self.entryvars.items()):
                    try:
                        print('%s%s%s'  %(' ', str(nm+':').ljust(30), str(obj.get()) ))
                    except Exception as err:
                        pass # just skip things that don't print neatly


        except Exception as err:   # TODO: I know, bad practice...
            print('\nERROR: \n%s\n' %str(err))
            #self.button_click('reload_config')

# =================== END: PROGRAM BEHAVIOR ==============================




# -------------------- ASSEMBLE GUI FROM ABOVE COMPONENTS ------------------------
    def run_gui(self):
        print('GUI RUN: Setup')

        # SETUP APPLICATION, app, appframe, title
        app = Tk()
        self.app = app
        self.title = "TD Consumption Analytics (COA)"
        self.define_styles(app)
        self.set_defaults()
        app.wm_title(self.title)
        app.title(self.title)
        app.geometry(self.appsize)

        #-------------- Page Setup ------------------
        appframe = Frame(app, style="TFrame"); appframe.pack(fill=BOTH, expand=True)
        self.newImage(appframe, image_name='banner').pack(anchor=NW)
        Label(appframe, style="title.TLabel", text='TD Consumption Analytics (COA)').pack(anchor=NW)

        tabcontrol = Notebook(appframe, padding=5)
        tabcontrol.pack(fill=BOTH, expand=True, anchor=NW)

        Button(appframe, text="Close",          width=10, command=lambda:self.close()).pack(padx=3, side=RIGHT)
        Button(appframe, text="MOTD",           width=7,  command=lambda:self.button_click("motd")).pack(padx=3, side=RIGHT)
        Button(appframe, text="Reload Configs", width=14, command=lambda:self.button_click("reload_config")).pack(padx=3, side=RIGHT)
        #Label(appframe, style="TLabel", text='version "%s"' %self.version).pack(anchor='center')
        self.newImage(appframe, image_name='logo').pack(side=LEFT)

        tabConfig   = Frame(tabcontrol, style="config-normal.TFrame"); tabConfig.pack(fill=X, expand=True, anchor='n')
        tabcontrol.add(tabConfig,  text='Config Files')
        tabNormalrun   = Frame(tabcontrol, style="normalrun-normal.TFrame"); tabNormalrun.pack(fill=X, expand=True, anchor='n')
        tabcontrol.add(tabNormalrun,  text='Normal Run')
        tabAssistedrun  = Frame(tabcontrol, style="assistedrun-normal.TFrame") ; tabAssistedrun.pack(fill=X, expand=True)
        tabcontrol.add(tabAssistedrun, text='Assisted Run')
        # tabDownload = Frame(tabcontrol, style="default-normal.TFrame") ; tabDownload.pack(fill=X, expand=True)
        # tabcontrol.add(tabDownload, text='Download')
        # tabExecute  = Frame(tabcontrol, style="default-normal.TFrame") ;  tabExecute.pack(fill=X, expand=True)
        # tabcontrol.add(tabExecute,  text='Execute')
        # tabUpload   = Frame(tabcontrol, style="default-normal.TFrame") ;   tabUpload.pack(fill=X, expand=True)
        # tabcontrol.add(tabUpload,   text='Upload')
        tabHelp     = Frame(tabcontrol, style="default-normal.TFrame") ;     tabHelp.pack(fill=X, expand=True)
        tabcontrol.add(tabHelp,     text='Help')


        #-------------- TAB: CONFIG ------------------
        frmConfig       = Frame(tabConfig, padding=5, style="config-normal.TFrame"); frmConfig.pack(fill=BOTH, expand=True, anchor=N)
        txt = []
        txt.append("Welcome to the COA thick-client.  Use this tool to collect data from customer systems, generate visualizations and presentations, and upload datasets to Transcend for further analysis.")
        txt.append("If you have DIRECT LOGIN ACCESS to your customer's system, use the NORMAL RUN tab.")
        txt.append("If you DO NOT have login credentials to your customer's system, use the ASSISTED RUN tab.")
        txt.append("All users should start by opening and editing the config files, found below.  For more help on file contents, please see our SharePoint User's Guide.")
        Label(frmConfig, text='\n\n'.join(txt), style='config-bold.TLabel', wraplength=450, justify="left").pack(fill=X, anchor=N, pady=30)
        Label(frmConfig, text='   Step 1: Check your Config Files:', style='config-bold.TLabel').pack(fill=X, anchor=N)

        frmConfigFiles  = Frame(frmConfig, padding=5, style="config-normal.TFrame"); frmConfigFiles.pack(fill=BOTH, expand=True, anchor=N)
        self.newframe_LEB(frmConfigFiles, labeltext=' AppRoot Path:', btntext='Open Folder', btn_width=10, btncommand='approot' , style='config-normal').pack(fill=X)
        self.newframe_LEB(frmConfigFiles, labeltext='  Config File:', btntext='Open File'  , btn_width=10, btncommand='config'  , style='config-normal').pack(fill=X)
        self.newframe_LEB(frmConfigFiles, labeltext=' Systems File:', btntext='Open File'  , btn_width=10, btncommand='systems' , style='config-normal').pack(fill=X)
        self.newframe_LEB(frmConfigFiles, labeltext=' Secrets File:', btntext=''           , btn_width=10, btncommand='secrets' , style='config-normal').pack(fill=X)
        self.newframe_LEB(frmConfigFiles, labeltext='FileSets File:', btntext='Open File'  , btn_width=10, btncommand='filesets', style='config-normal').pack(fill=X)


        #-------------- TAB: NORMAL RUN ------------------
        frmNormalrun_N  = Frame(tabNormalrun, style="normalrun-normal.TFrame"); frmNormalrun_N.pack(side=TOP,    padx=5,  fill=BOTH, expand=True, anchor=N)
        Label(frmNormalrun_N, text='   Step 2: Select Systems and FileSets:', style='normalrun-bold.TLabel').pack(fill=X, anchor=N)
        self.newframe_Tx2(frmNormalrun_N, treetext='SYSTEMS',  treelabel_left='Active', treelabel_right='Inactive', width=120, treeheight=5, style = 'normalrun-normal').pack(side=LEFT , fill=BOTH, expand=True)
        self.newframe_Tx2(frmNormalrun_N, treetext='FILESETS', treelabel_left='Active', treelabel_right='Inactive', width=150, treeheight=5, style = 'normalrun-normal').pack(side=RIGHT, fill=BOTH, expand=True)

        frmNormalrun_S  = Frame(tabNormalrun, style="normalrun-normal.TFrame"); frmNormalrun_S.pack(side=BOTTOM, padx=15, fill=BOTH, expand=True, anchor=S)
        Label(frmNormalrun_S, text='   Step 3: NORMAL RUN - YOU HAVE ACCESS TO THE CUSTOMER SYSTEM DIRECTLY:', style='normalrun-bold.TLabel').pack(fill=X, anchor=N)

        frmNormalrun_S1  = Frame(frmNormalrun_S, padding=5, style="normalrun-normal.TFrame"); frmNormalrun_S1.pack(side=TOP, fill=X, expand=True, anchor=N)
        self.separator(frmNormalrun_S, style='normalrun-normal', width=8)
        frmNormalrun_S2  = Frame(frmNormalrun_S, padding=5, style="normalrun-normal.TFrame"); frmNormalrun_S2.pack(side=TOP, fill=X, expand=True, anchor=N)
        self.separator(frmNormalrun_S, style='normalrun-normal', width=8)
        frmNormalrun_S3  = Frame(frmNormalrun_S, padding=5, style="normalrun-normal.TFrame"); frmNormalrun_S3.pack(side=TOP, fill=X, expand=True, anchor=N)
        self.separator(frmNormalrun_S, style='normalrun-normal', width=8)
        frmNormalrun_S4  = Frame(frmNormalrun_S, padding=5, style="normalrun-normal.TFrame"); frmNormalrun_S4.pack(side=TOP, fill=X, expand=True, anchor=N)

        Label(frmNormalrun_S1, text='Open\nInternet:     ', style='normalrun-bold.TLabel').pack(side=LEFT, anchor=W)
        self.newframe_CLB(frmNormalrun_S1, btntext='Download Files',      btncommand='download_files',      style='normalrun-normal').pack(fill=X, expand=True)
        self.newframe_CLB(frmNormalrun_S1, btntext='Prepare SQL',         btncommand='prepare_sql',         style='normalrun-normal').pack(fill=X, expand=True)
        Label(frmNormalrun_S2, text='Customer  \nVPN:', style='normalrun-bold.TLabel').pack(side=LEFT, anchor=W)
        self.newframe_CLB(frmNormalrun_S2, btntext='Execute Run',         btncommand='execute_run',         style='normalrun-normal').pack(fill=X, expand=True)
        Label(frmNormalrun_S3, text='Teradata  \nVPN:', style='normalrun-bold.TLabel').pack(side=LEFT, anchor=W)
        self.newframe_LEB(frmNormalrun_S3, labeltext='Output Folder:', btntext='Open', btncommand='last_output_folder', lbl_width=15, style='normalrun-normal').pack(fill=X, expand=True)
        self.newframe_CLB(frmNormalrun_S3, btntext='Upload to Transcend', btncommand='upload_to_transcend', style='normalrun-normal').pack(fill=X, expand=True)
        self.newframe_CLB(frmNormalrun_S4, btntext='Run All Checked', btncommand='run_all_checked', style='normalrun-normal', labeltext=' Check All\n<---------', checkcommand=self.toggle_all_chk_normalrun ).pack(fill=X)



        #-------------- TAB: ASSISTED RUN ------------------
        frmAssistedrun_N  = Frame(tabAssistedrun, style="assistedrun-normal.TFrame"); frmAssistedrun_N.pack(side=TOP,    padx=5,  fill=BOTH, expand=True, anchor=N)
        Label(frmAssistedrun_N, text='   Step 2: Select Systems and FileSets:', style='assistedrun-bold.TLabel').pack(fill=X, anchor=N)
        self.newframe_Tx2(frmAssistedrun_N, treetext='SYSTEMS (Assisted)',  treelabel_left='Active', treelabel_right='Inactive', width=120, treeheight=5, style = 'assistedrun-normal').pack(side=LEFT , fill=BOTH, expand=True)
        self.newframe_Tx2(frmAssistedrun_N, treetext='FILESETS (Assisted)', treelabel_left='Active', treelabel_right='Inactive', width=150, treeheight=5, style = 'assistedrun-normal').pack(side=RIGHT, fill=BOTH, expand=True)

        frmAssistedrun_S  = Frame(tabAssistedrun, style="assistedrun-normal.TFrame"); frmAssistedrun_S.pack(side=BOTTOM, padx=15, fill=BOTH, expand=True, anchor=S)
        Label(frmAssistedrun_S, text='   Step 3: ASSISTED RUN - YOU SEND FILES TO CUSTOMER CONTACT FOR EXECUTION:', style='assistedrun-bold.TLabel').pack(fill=X, anchor=N)

        frmAssistedrun_S1  = Frame(frmAssistedrun_S, padding=5, style="assistedrun-normal.TFrame"); frmAssistedrun_S1.pack(side=TOP, fill=X, expand=True, anchor=N)
        self.separator(frmAssistedrun_S, style='normalrun-normal', width=8)
        frmAssistedrun_S2  = Frame(frmAssistedrun_S, padding=5, style="assistedrun-normal.TFrame"); frmAssistedrun_S2.pack(side=TOP, fill=X, expand=True, anchor=N)
        self.separator(frmAssistedrun_S, style='normalrun-normal', width=8)
        frmAssistedrun_S3  = Frame(frmAssistedrun_S, padding=5, style="assistedrun-normal.TFrame"); frmAssistedrun_S3.pack(side=TOP, fill=X, expand=True, anchor=N)
        self.separator(frmAssistedrun_S, style='normalrun-normal', width=8)
        frmAssistedrun_S4  = Frame(frmAssistedrun_S, padding=5, style="assistedrun-normal.TFrame"); frmAssistedrun_S4.pack(side=TOP, fill=X, expand=True, anchor=N)

        Label(frmAssistedrun_S1, text='Open\nInternet:     ', style='assistedrun-bold.TLabel').pack(side=LEFT, anchor=W)
        self.newframe_CLB(frmAssistedrun_S1, btntext='Download Files',      btncommand='download_files',      style='assistedrun-normal').pack(fill=X, expand=True)
        Label(frmAssistedrun_S2, text='No\nInternet      \nNeeded:', style='assistedrun-bold.TLabel').pack(side=LEFT, anchor=W)
        self.newframe_CLB(frmAssistedrun_S2, btntext='Prepare SQL',         btncommand='prepare_sql',         style='assistedrun-normal').pack(fill=X, expand=True)
        self.newframe_CLB(frmAssistedrun_S2, btntext='Make Customer Files', btncommand='make_customer_files', style='assistedrun-normal').pack(fill=X, expand=True)
        self.newframe_LEB(frmAssistedrun_S2, labeltext='Output Folder:', btntext='Open', btncommand='last_output_folder', lbl_width=15, style='assistedrun-normal').pack(fill=X, expand=True)
        self.newframe_CLB(frmAssistedrun_S2, show_chkbox=False,  btntext='Process Return Data',  btncommand='process_data',        style='assistedrun-normal').pack(fill=X, expand=True)
        Label(frmAssistedrun_S3, text='Teradata   \nVPN:', style='assistedrun-bold.TLabel').pack(side=LEFT, anchor=W)
        self.newframe_CLB(frmAssistedrun_S3, show_chkbox=False,  btntext='Upload to Transcend', btncommand='upload_to_transcend', style='assistedrun-normal').pack(fill=X, expand=True)
        self.newframe_CLB(frmAssistedrun_S4, btntext='Run All Checked', btncommand='run_all_checked_assist', style='assistedrun-normal', labeltext=' Check All\n<---------', checkcommand=self.toggle_all_chk_assistedrun ).pack(fill=X)


        #-------------- TAB: HELP ------------------
        frmHelp  = Frame(tabHelp, padding=5, style="help-normal.TFrame"); frmHelp.pack(fill=BOTH, expand=True, anchor=N)
        frmHelp_N  = Frame(frmHelp, padding=5, style="help-normal.TFrame"); frmHelp_N.pack(side=TOP, fill=BOTH, expand=True, anchor=N)
        Label(frmHelp_N, text='This page contains mostly debugging information right now... MORE COMING SOON!', style='help-bold.TLabel').pack(fill=X, anchor=N)
        frmHelp_E  = Frame(frmHelp, padding=5, style="help-normal.TFrame"); frmHelp_E.pack(side=RIGHT, fill=X, expand=False, anchor=E)
        self.newframe_LC(frmHelp_E, labeltext='Skip_DBS Flag (debugging)', checkcommand='skip_dbs_toggle', style='help-normal').pack(anchor=S)
        self.newframe_LC(frmHelp_E, labeltext='Skip_Git Flag (debugging)', checkcommand='skip_git_toggle', style='help-normal').pack(anchor=S)
        self.newframe_LC(frmHelp_E, labeltext='Show Hidden Filesets (debugging)', checkcommand='show_hiddenfilesets_toggle', style='help-normal').pack(anchor=S)
        frmHelp_W  = Frame(frmHelp, padding=5, style="help-normal.TFrame"); frmHelp_W.pack(side=LEFT, expand=False, anchor=W)
        Label(frmHelp_W, text='Print Dictionary:', style='help-bold.TLabel').pack(fill=X, anchor=N)
        self.newbutton(frmHelp_W, btntext = 'Systems',  btncommand='print_systems',  btnwidth=15, style = 'help-normal', side=TOP)
        self.newbutton(frmHelp_W, btntext = 'Config',   btncommand='print_config',   btnwidth=15, style = 'help-normal', side=TOP)
        self.newbutton(frmHelp_W, btntext = 'FileSets', btncommand='print_filesets', btnwidth=15, style = 'help-normal', side=TOP)
        self.newbutton(frmHelp_W, btntext = 'GUI Elements', btncommand='print_entryvars', btnwidth=15, style = 'help-normal', side=TOP)



        #-------------- RUN!!!
        print('approot: ' + self.entryvar('approot') )
        print('config:  ' + self.entryvar('config') )
        print('systems: ' + self.entryvar('systems') )
        print('secrets: ' + self.entryvar('secrets') )

        self.coa = tdcoa(approot = self.entryvar('approot'), secrets = self.entryvar('secrets'))
        self.coa.deactivate_all()
        # these 'clicks' will refresh both left and right treeviews
        self.button_click('tv_systems_left')
        self.button_click('tv_filesets_left')
        self.button_click('tv_systems_assisted_left')
        self.button_click('tv_filesets_assisted_left')

        self.upload_get_lastrun_folder()

        # update our secrets file if default was different than coa.settings value (chicken/egg problem)
        secrets_from_settings = self.coa.settings['secrets']
        secrets_from_default =  self.entryvars['secrets'].get()
        if secrets_from_default != secrets_from_settings:
            self.entryvars['secrets'].set(self.first_file_that_exists(secrets_from_settings, secrets_from_default))
            self.coa.secrets = self.entryvars['secrets'].get()
            self.coa.reload_config(skip_git = True)
            self.coa.deactivate_all()
            self.button_click('tv_filesets_left')
            self.button_click('tv_filesets_assisted_left')

        # sync skip_dbs flag with coa.settings
        self.entryvars['skip_dbs_toggle'].set(value=self.validate_boolean(self.coa.settings['skip_dbs'],'int'))

        Label(frmHelp_N, text='Version of tdcsm = %s' %self.coa.version, style='help-bold.TLabel').pack(fill=X, anchor=N)
        Label(frmHelp_N, text='Version of tdgui = %s' %self.version, style='help-bold.TLabel').pack(fill=X, anchor=N)

        app.bind('<Escape>', self.close)
        app.mainloop()
