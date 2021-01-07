import subprocess, platform, os, copy #, yaml
import sys
from datetime import datetime
from tkinter import *
from tkinter.ttk import *
from PIL import Image
from PIL import ImageTk
from .tdcoa import tdcoa
import tdcsm

class coa():

    version = "0.4.1.3"
    debug = False

    entryvars = {}
    defaults = {}
    #appsize = '800x500' # width x height
    appwidth = 750
    appheight = 550
    sampleTx2 = {'systems_left':['one','two','three'],
                 'systems_right':['four','five','six'],
                 'filesets_left':['one','two','three'],
                 'filesets_right':['four','five','six']}
    approot = ''
    secrets = ''
    motd = False
    skip_git = False
    show_hidden_filesets = False
    fontsize = 10
    font = 'Open Sans'


    def __init__(self, approot='', secrets='', **kwargs):
        print('GUI for TDCOA started')
        #self.version = str(datetime.now()).replace('-','').replace(':','').split('.')[0].replace(' ','.')
        if approot != '': self.defaults['approot'] = approot
        if secrets != '': self.defaults['secrets'] = secrets
        if platform.system()[:3]=='Win':
            self.localos='Win'
            self.appwidth  = int(self.appwidth  * 1.2)
            self.appheight = int(self.appheight * 1.2)
        else:
            self.localos='Mac'
        self.appsize = str(int(self.appwidth)) + 'x' + str(int(self.appheight))
        self.images = {'banner':{'file':'pic_TDCOA_Banner.gif', 'X':700, 'Y':27, 'scale':(self.appwidth - 20) / 700, 'object':None, 'alttext':''}
                       ,'logo' :{'file':'pic_TDCOAdot.gif',     'X':330, 'Y':55, 'scale':0.5, 'object':None, 'alttext':'Teradata'}
                       ,'logo' :{'file':'pic_TDCOAdot2.gif',    'X':330, 'Y':55, 'scale':0.5, 'object':None, 'alttext':'Teradata'}}
        if 'versionprefix' in kwargs:
            self.versionprefix = kwargs['versionprefix']
            self.version = self.versionprefix + '.' + self.version
        else:
            self.versionprefix = ''

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
        colors = {  'config'     :'#FFE0A4',
                    'normalrun'  :'#C6E7E7',
                    'assistedrun':'#C6E7E7',
                    'execute'    :'#C6E7E7',
                    'upload'     :'#C6E7E7',
                    'help'       :'#BFBFBF'}
        font = self.font
        fontsize = self.fontsize
        Style(app).configure("TButton"    ,foreground="#ffffff", background="#404040", font=(font, str(fontsize))  )
        Style(app).configure("TFrame"     ,foreground="#ffffff", background="#394951", font=(font, str(fontsize))  )
        Style(app).configure("TNotebook"  ,foreground="#ffffff", background="#394951", font=(font, str(fontsize))  )
        Style(app).configure("TLabel"     ,foreground="#ffffff", background="#394951", font=(font, str(fontsize))  )
        Style(app).configure("title.TLabel",foreground="#ffffff", background="#394951", font=(font,str(fontsize*2), 'bold')  )

        for name, hex in colors.items():
            Style(app).configure("%s-normal.TFrame"       %name, foreground="#394951",   background=colors[name], font=(font, str(fontsize))  )
            Style(app).configure("%s-normal.TButton"      %name, foreground="#394951",   background=self.shade(colors[name]), font=(font, str(fontsize)), padding=(1,1,1,1)  )
            Style(app).map(      "%s-normal.TButton"      %name, background=[("disabled",self.shade(colors[name],0.4))])
            Style(app).configure("%s-normal.TCheckbutton" %name, foreground="#394951",   background=colors[name])
            Style(app).configure("%s-separator.TFrame"    %name, foreground="#394951",   background=colors[name], font=(font, str(fontsize))  )
            Style(app).configure("%s-normal.TLabel"       %name, foreground="#394951",   background=colors[name], font=(font, str(fontsize))  )
            Style(app).configure("%s-bold.TLabel"         %name, foreground="#394951",   background=colors[name], font=(font, str(fontsize), 'bold')  )
            Style(app).configure("%s-header.TLabel"       %name, foreground="#394951",   background=colors[name], font=(font, str(fontsize*2), 'bold')  )
            Style(app).configure("%s-normal.Treeview"     %name, foreground="#394951",   background=self.tint(colors[name],0.8), font=(font, str(fontsize))  )
            Style(app).configure("%s-normal.TEntry"       %name, foreground="#394951",   fieldbackground=self.tint(colors[name],0.7), font=(font, str(fontsize*10)), padding=(1,1,1,1)  )

    def newframe_LEB(self, parent, labeltext='not set', btntext='not set', btncommand='test', style = 'default', lbl_width=12, btn_width=6):
        if btncommand not in self.entryvars: self.entryvars[btncommand] = StringVar()
        f = Frame(parent, padding=1, style=str('%s.TFrame' %style))
        l = Label(f, text=labeltext, width = lbl_width, anchor=E, style=str('%s.TLabel' %style))
        e = Entry(f, textvariable=self.entryvars[btncommand], style=str('%s.TEntry' %style), font=(self.font, self.fontsize))
        b = Button(f,text=btntext, command=lambda:self.button_click(btncommand, entrytext=self.entryvars[btncommand].get()), width=btn_width, style=str('%s.TButton' %style))
        l.pack(side=LEFT, fill=BOTH, expand=False, padx=0, pady=0, ipady=1)
        e.pack(side=LEFT, fill=BOTH, expand=True , padx=0, pady=0, ipady=1)
        b.pack(side=LEFT, fill=BOTH, expand=False, padx=0, pady=0, ipady=1)
        if btntext == '': b.state(["disabled"])
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

    def newframe_CLBB(self, parent, labeltext='', btntext = 'not set', btncommand='test', btnwidth=20, btntext2='not set', btncommand2='test2', checkcommand=print('check command'), style = 'default', show_chkbox=True):
        f = Frame(parent, padding=1, style=str('%s.TFrame' %style))
        if btncommand not in self.entryvars: self.entryvars[btncommand] = IntVar(value=0)
        if btncommand2 not in self.entryvars: self.entryvars[btncommand2] = IntVar(value=0)
        if show_chkbox:
            c = Checkbutton(f, variable=self.entryvars[btncommand], command=checkcommand, style=str('%s.TCheckbutton' %style))
        else:
            c = Label(f, text="     ", anchor=W, style=str('%s.TLabel' %style)) # widt of missing checkbox
        if labeltext != '': l = Label(f, text=labeltext, anchor=E, style=str('%s.TLabel' %style))
        b  = Button(f,text=btntext,  command=lambda:self.button_click(btncommand,  state=self.entryvars[btncommand].get()),  width=btnwidth, style=str('%s.TButton' %style))
        b2 = Button(f,text=btntext2, command=lambda:self.button_click(btncommand2, state=self.entryvars[btncommand2].get()), style=str('%s.TButton' %style))
        c.pack(side=LEFT, expand=False)
        if labeltext != '': l.pack(side=LEFT, expand=False)
        b.pack(side=LEFT, fill=BOTH, expand=True)
        b2.pack(side=RIGHT, fill=BOTH, expand=False, ipadx=0, padx=0)
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

    def newImage(self, parent, image_name='', format=True):
        i = self.images[image_name]
        x = int(i['X']*i['scale'])
        y = int(i['Y']*i['scale'])
        c = Canvas(parent, width=x+20, height=y+20, bg='#394951', bd=0, highlightthickness=0)
        pix = os.path.join(os.path.dirname(tdcsm.__file__), i['file'])
        try:
            if format:
                img = Image.open(pix).resize((x,y), Image.ANTIALIAS)
                i['object'] = ImageTk.PhotoImage(img)
            else:
                img = PhotoImage(file=pix)
                i['object'] = img

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

    @staticmethod
    def print_complete(sectionname=''):
        msg = '\n' + str('#'*10) + " '" +  sectionname + "' complete! " + str('#'*10) + '\n'
        print(msg)

    def stripsecrets(self, msg='', secretdict={}):
        for nm, secret in secretdict.items():
            if secret in msg:
                msg = msg.replace(secret, '%s%s%s' % (secret[:1], '*' * (len(secret) - 2), secret[-1:]))
        return str(msg)

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
        print("updating 'Output Folder' textbox...")
        lastrunfilepath = os.path.join(self.entryvar('approot'), lastrunfile)

        # open up breadcrumb file, extract folder location, and return if exists
        if os.path.isfile(lastrunfilepath):
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
                # self.coa.update_sourcesystem_yaml()
                self.coa.reload_config(skip_dbs=self.skip_dbs(), skip_git=self.skip_git)
                print('approot: ', self.coa.approot)
                print('config: ', self.coa.configpath)
                print('secret: ', self.coa.secretpath)
                print('system: ', self.coa.systemspath)
                print('fileset: ', self.coa.filesetpath)
                self.coa.deactivate_all()
                self.entryvars['skip_dbs_toggle'].set(value=self.validate_boolean(self.coa.settings['skip_dbs'],'int'))
                self.entryvars['bteq_delim'].set(value=self.coa.bteq_delim)
                self.upload_get_lastrun_folder(lastrunfile='')
                self.button_click('tv_systems_left') # this 'click' will refresh both left and right treeviews
                self.button_click('tv_filesets_left')
                self.print_complete(name)
            elif name == 'approot':
                approot = kwargs['entrytext'].replace(r':\U',r':\\U')
                self.open_folder_explorer(approot, createifmissing=True)
            elif name == 'download_files':
                self.coa.download_files(self.motd)
                self.motd = False
                self.print_complete(name)
            elif name == 'prepare_sql':
                self.coa.prepare_sql()
                self.print_complete(name)
            elif name == 'execute_run':
                #self.button_click('opendir_run')
                self.coa.execute_run()
                self.upload_get_lastrun_folder()
                self.print_complete(name)
            elif name == 'make_customer_files':
                self.coa.make_customer_files2()
                self.upload_get_lastrun_folder()
                if 'entrytext' in kwargs:
                    self.open_folder_explorer(os.path.join(self.entryvar('approot'), kwargs['entrytext']), createifmissing=True)
                self.print_complete(name)
            elif name == 'process_data':
                self.button_click('opendir_run')
                self.coa.process_return_data2(os.path.join(self.coa.approot, self.entryvar('last_output_folder')))
                self.print_complete(name)
            elif name == 'upload_to_transcend':
                with open(os.path.join(self.entryvar('approot'), '.last_run_output_path.txt'), 'w') as fh:
                    fh.write(self.entryvar('last_output_folder'))
                self.coa.upload_to_transcend()
                self.print_complete(name)
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
                #self.reload_Tx2('systems_assisted', leftlist = d['True'].keys(), rightlist = d['False'].keys())
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
                #self.reload_Tx2('filesets_assisted', leftlist = d['True'].keys(), rightlist = d['False'].keys(), exclude=exclude)
            elif name == 'skip_dbs_toggle':
                self.coa.settings['skip_dbs'] = bool(kwargs['state'] == 1)
            elif name == 'skip_git_toggle':
                self.skip_git = bool(kwargs['state'] == 1)
            elif name == 'show_hiddenfilesets_toggle':
                self.show_hidden_filesets = bool(kwargs['state'] == 1)
            elif name[:8] == 'opendir_':
                dir = name[8:]
                print('opening %s' %dir)
                pth = os.path.join(self.entryvar('approot'), self.coa.folders[dir])
                self.open_folder_explorer(pth , createifmissing=False)
            elif name == 'bteq_delim':
                self.coa.bteq_delim = kwargs['entrytext']
                print('bteq delimiter updated')
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

# =================== END: PROGRAM BEHAVIOR ==============================




# -------------------- ASSEMBLE GUI FROM ABOVE COMPONENTS ------------------------
    def run_gui(self):
        print('GUI RUN: Setup')

        # SETUP APPLICATION, app, appframe, title
        app = Tk()
        self.app = app
        self.title = " Teradata Consumption Analytics and Collateral Automation Tool"
        self.define_styles(app)
        self.set_defaults()
        app.wm_title(self.title)
        app.title(self.title)
        app.geometry(self.appsize)

        #-------------- Page Setup ------------------
        appframe = Frame(app, style="TFrame"); appframe.pack(fill=BOTH, expand=True)
        # self.newImage(appframe, image_name='banner').pack(anchor=NW)
        Label(appframe, style="title.TLabel", text=self.title).pack(anchor=NW)


        self.tabcontrol = Notebook(appframe, padding=5)
        tabcontrol = self.tabcontrol
        tabcontrol.pack(fill=BOTH, expand=True, anchor=NW)

        bottomframe = Frame(app, style="TFrame", height=200); bottomframe.pack(fill=X, expand=False)
        Button(bottomframe, text="Close",          width=10, command=lambda:self.close()).pack(padx=3, side=RIGHT)
        Button(bottomframe, text="MOTD",           width=7,  command=lambda:self.button_click("motd")).pack(padx=3, side=RIGHT)
        Button(bottomframe, text="Reload Configs", width=14, command=lambda:self.button_click("reload_config")).pack(padx=3, side=RIGHT)
        #Label(appframe, style="TLabel", text='version "%s"' %self.version).pack(anchor='center')
        self.newImage(bottomframe, image_name='logo', format=False).pack(side=LEFT)

        tabConfig   = Frame(tabcontrol, style="config-normal.TFrame"); tabConfig.pack(fill=X, expand=True, anchor='n')
        tabobjConfig = tabcontrol.add(tabConfig,  text='Config Files')
        # tabNormalrun   = Frame(tabcontrol, style="normalrun-normal.TFrame"); tabNormalrun.pack(fill=X, expand=True, anchor='n')
        # tabcontrol.add(tabNormalrun,  text='Normal Run')
        # tabAssistedrun  = Frame(tabcontrol, style="assistedrun-normal.TFrame") ; tabAssistedrun.pack(fill=X, expand=True)
        # tabcontrol.add(tabAssistedrun, text='Assisted Run')
        tabQuickrun   = Frame(tabcontrol, style="normalrun-normal.TFrame"); tabQuickrun.pack(fill=X, expand=True, anchor='n')
        tabobjQuickrun = tabcontrol.add(tabQuickrun,  text='Quick Run')
        tabHelp     = Frame(tabcontrol, style="default-normal.TFrame") ;     tabHelp.pack(fill=X, expand=True)
        tablobjHelp = tabcontrol.add(tabHelp, text='Help')


        #-------------- TAB: CONFIG ------------------
        frmConfig = Frame(tabConfig, padding=5, style="config-normal.TFrame"); frmConfig.pack(fill=BOTH, expand=True, anchor=N)
        txt = []
        stylename = 'config'
        txt.append("Use this tool to collect data from customer systems, generate visualizations and presentations, and upload datasets to Transcend for further analysis.")
        txt.append("STEP 1 - Confirm that your configuration is up-to-date")
        txt.append("STEP 2 - Select the systems and filesets to execute")
        txt.append("STEP 3 - Execute the build steps, by either\n - executing directly, if you have access\n - create files to supply a contact to execute ")
        txt.append("If you want to reset everything, or any time you change a config file, click the 'Reload Configs' button below.")
        txt.append("Click on 'MOTD' below for list of links, including detailed instructions on SharePoint, Getting Started Guides, FileSet information, PowerBI, and more.")

        frmConfigw = Frame(frmConfig, padding=5, style="%s-normal.TFrame" %stylename); frmConfigw.pack(side=LEFT,  fill=BOTH, expand=True, anchor=W)
        frmConfige = Frame(frmConfig, padding=5, style="%s-normal.TFrame" %stylename); frmConfige.pack(side=RIGHT, fill=BOTH, expand=True, anchor=W)
        frmConfigsep = Frame(frmConfig, padding=1, style="default-normal.TFrame");   frmConfigsep.pack(side=LEFT, fill=Y, expand=True)


        Label(frmConfigw, text='Welcome to the COA Tool!', style='%s-bold.TLabel' %stylename, wraplength=300, justify="left").pack(fill=X, anchor=N, pady=8)
        Label(frmConfigw, text='\n\n'.join(txt), style='%s-normal.TLabel' %stylename, wraplength=300, justify="left").pack(fill=X, anchor=N, pady=8)
        self.separator(frmConfig, style='%s-normal' %stylename, width=8)
        Label(frmConfige, text='\n   Step 1: Check your Config Files:\n', style='%s-bold.TLabel' %stylename).pack(fill=X, anchor=N)

        frmConfigFiles  = Frame(frmConfige, padding=2, style="config-normal.TFrame"); frmConfigFiles.pack(fill=BOTH, expand=True, anchor=N)
        self.newframe_LEB(frmConfigFiles, labeltext=' AppRoot Path:', btntext='Open Folder', btn_width=10, btncommand='approot' , style='%s-normal' %stylename).pack(fill=X)
        self.newframe_LEB(frmConfigFiles, labeltext='  Config File:', btntext='Open File'  , btn_width=10, btncommand='config'  , style='%s-normal' %stylename).pack(fill=X)
        self.newframe_LEB(frmConfigFiles, labeltext=' Systems File:', btntext='Open File'  , btn_width=10, btncommand='systems' , style='%s-normal' %stylename).pack(fill=X)
        self.newframe_LEB(frmConfigFiles, labeltext=' Secrets File:', btntext=''           , btn_width=10, btncommand='secrets' , style='%s-normal' %stylename).pack(fill=X)
        self.newframe_LEB(frmConfigFiles, labeltext='FileSets File:', btntext='Open File'  , btn_width=10, btncommand='filesets', style='%s-normal' %stylename) # pack(fill=X)




        #-------------- TAB: Quick RUN ------------------
        stylename = 'normalrun'
        tabQuickrun_N  = Frame(tabQuickrun, style="%s-normal.TFrame" %stylename); tabQuickrun_N.pack(side=LEFT, padx=5,  fill=BOTH, expand=True, anchor=W)
        Label(tabQuickrun_N, text='\nStep 2: Select Systems and FileSets:', style='%s-bold.TLabel' %stylename).pack(fill=X, anchor=N)
        self.newframe_Tx2(tabQuickrun_N, treetext='SYSTEMS',  treelabel_left='Active', treelabel_right='Inactive', width=100, treeheight=1, style = '%s-normal' %stylename).pack(side=TOP , fill=BOTH, expand=True)
        self.newframe_Tx2(tabQuickrun_N, treetext='FILESETS', treelabel_left='Active', treelabel_right='Inactive', width=100, treeheight=5, style = '%s-normal' %stylename).pack(side=BOTTOM, fill=BOTH, expand=True)

        tabQuickrun_btns  = Frame(tabQuickrun, style="%s-normal.TFrame" %stylename); tabQuickrun_btns.pack(side=RIGHT, padx=15, fill=BOTH, expand=True, anchor=E)
        Label(tabQuickrun_btns, text='\nStep 3: Build files and execute (or have executed):', style='%s-bold.TLabel' %stylename).pack(fill=X, anchor=N)

        tabQuickrun_btns1  = Frame(tabQuickrun_btns, padding=5, style="%s-normal.TFrame" %stylename); tabQuickrun_btns1.pack(side=TOP, fill=BOTH, expand=True, anchor=N)
        self.separator(tabQuickrun_btns, style='%s-normal' %stylename, width=8)
        tabQuickrun_btns2  = Frame(tabQuickrun_btns, padding=5, style="%s-normal.TFrame" %stylename); tabQuickrun_btns2.pack(side=TOP, fill=BOTH, expand=True, anchor=N)
        self.separator(tabQuickrun_btns, style='%s-normal' %stylename, width=8)
        tabQuickrun_btns3  = Frame(tabQuickrun_btns, padding=5, style="%s-normal.TFrame" %stylename); tabQuickrun_btns3.pack(side=TOP, fill=BOTH, expand=True, anchor=N)
        self.separator(tabQuickrun_btns, style='%s-normal' %stylename, width=8)
        tabQuickrun_btns4  = Frame(tabQuickrun_btns, padding=5, style="%s-normal.TFrame" %stylename); tabQuickrun_btns4.pack(side=TOP, fill=BOTH, expand=True, anchor=N)

        Label(tabQuickrun_btns1, text='Open\nInternet:     ', style='%s-bold.TLabel' %stylename).pack(side=LEFT, anchor=W, expand=False)
        self.newframe_CLBB(tabQuickrun_btns1, btntext='Download Files',    btncommand='download_files', btnwidth=40, btntext2='Open Folder', btncommand2='opendir_download', style='%s-normal' %stylename).pack(fill=BOTH, expand=True)
        self.newframe_CLBB(tabQuickrun_btns1, btntext='Prepare SQL to Run',btncommand='prepare_sql',    btnwidth=40, btntext2='Open Folder', btncommand2='opendir_run',      style='%s-normal' %stylename).pack(fill=BOTH, expand=True)

        Label(tabQuickrun_btns2, text='Customer  \nVPN:', style='%s-bold.TLabel' %stylename).pack(side=LEFT, anchor=W, expand=False)
        tabQuickrun_btns2a = Frame(tabQuickrun_btns2, padding=0, style="%s-normal.TFrame" %stylename); tabQuickrun_btns2a.pack(side=TOP, fill=X, expand=False, anchor=N)
        Label(tabQuickrun_btns2a, text='%sExecute Directly:'       %str(' '*5), style='%s-normal.TLabel' %stylename).pack( side=LEFT ) #,  fill=X, expand=False, anchor=N)
        Label(tabQuickrun_btns2a, text='Hand Files to Customer:%s' %str(' '*20), style='%s-normal.TLabel' %stylename).pack(side=RIGHT) #, fill=X, expand=False, anchor=N)
        self.newframe_CLB(tabQuickrun_btns2, btntext='\nExecute Run\n',         btncommand='execute_run',         style='%s-normal' %stylename).pack(side=LEFT, fill=BOTH, expand=True)

        tabQuickrun_btns2sep = Frame(tabQuickrun_btns2, padding=10, style="default-normal.TFrame"); tabQuickrun_btns2sep.pack(side=LEFT, fill=Y, expand=False, padx=(20,0))

        tabQuickrun_btns2e = Frame(tabQuickrun_btns2, padding=1, style="%s-normal.TFrame" %stylename); tabQuickrun_btns2e.pack(side=RIGHT, fill=BOTH, expand=True, anchor=E)
        self.newframe_CLB(tabQuickrun_btns2e, show_chkbox=False, btntext='Make Customer Files', btncommand='make_customer_files', style='%s-normal' %stylename).pack(fill=BOTH, expand=True)
        self.newframe_CLB(tabQuickrun_btns2e, show_chkbox=False, btntext='Process Return Data', btncommand='process_data',        style='%s-normal' %stylename).pack(fill=BOTH, expand=True)


        Label(tabQuickrun_btns3, text='Teradata  \nVPN:', style='%s-bold.TLabel' %stylename).pack(side=LEFT, anchor=W, expand=False)
        self.newframe_LEB(tabQuickrun_btns3, labeltext='Output Folder:', btntext='Open Folder', btncommand='last_output_folder', lbl_width=15, btn_width=10, style='%s-normal' %stylename).pack(fill=BOTH, expand=True)
        self.newframe_CLB(tabQuickrun_btns3, btntext='Upload to Transcend', btncommand='upload_to_transcend', style='%s-normal' %stylename).pack(fill=BOTH, expand=True)
        self.newframe_CLB(tabQuickrun_btns4, btntext='Run All Checked', btncommand='run_all_checked', style='%s-normal' %stylename, labeltext=' Check All\n<---------', checkcommand=self.toggle_all_chk_normalrun ).pack(fill=BOTH, expand=True)




        #-------------- TAB: HELP ------------------
        frmHelp  = Frame(tabHelp, padding=5, style="help-normal.TFrame"); frmHelp.pack(fill=BOTH, expand=True, anchor=N)
        frmHelp_N  = Frame(frmHelp, padding=5, style="help-normal.TFrame"); frmHelp_N.pack(side=TOP, fill=BOTH, expand=True, anchor=N)

        helpmsg = """If you need help for any reason, there are a few options available:

        - COA SharePoint Site:
             https://teradata.sharepoint.com/teams/Sales/ConsumptionAnalytics/SitePages/COA.aspx
             This site has videos, user guides, install instructions, and more.

        - COA Weekly Office Hours:
             Please feel free to bring questions, issues, suggestions, and requests to our weekly office hours.
             Most SEs/CSMs have this already, but if you don't - reach out to Stephen.Hilton@Teradata.com and he will add you.

        - Reach out to peers on Channel STX_400 of MSTeams 'Sales Technology Exchange (STX)'

        - Email Stephen.Hilton@Teradata.com, preferably with the subject beginning with "COA:"

        You are currently running:
        """

        Label(frmHelp_N, text=helpmsg, style='help-bold.TLabel').pack(fill=X, anchor=N)
        frmHelp_E  = Frame(frmHelp, padding=5, style="help-normal.TFrame"); frmHelp_E.pack(side=RIGHT, fill=X, expand=False, anchor=E)
        Label(frmHelp_E, text='Other GUI Options (for debugging):', style='help-bold.TLabel').pack(fill=X, anchor=N)
        self.newframe_LC(frmHelp_E, labeltext='Skip_DBS Flag', checkcommand='skip_dbs_toggle', style='help-normal').pack(anchor=S)
        self.newframe_LC(frmHelp_E, labeltext='Skip_Git Flag', checkcommand='skip_git_toggle', style='help-normal').pack(anchor=S)
        self.newframe_LC(frmHelp_E, labeltext='Show Hidden Filesets', checkcommand='show_hiddenfilesets_toggle', style='help-normal').pack(anchor=S)
        self.newframe_LEB(frmHelp_E, labeltext='BTEQ Delimiter', btntext='Update', btncommand='bteq_delim', style = 'help-normal', lbl_width=20, btn_width=8).pack(anchor=S)
        frmHelp_W  = Frame(frmHelp, padding=5, style="help-normal.TFrame"); frmHelp_W.pack(side=LEFT, expand=False, anchor=W)
        Label(frmHelp_W, text='Print Dictionary (for debugging):', style='help-bold.TLabel').pack(fill=X, anchor=N)
        self.newbutton(frmHelp_W, btntext = 'Systems',  btncommand='print_systems',  btnwidth=15, style = 'help-normal', side=TOP)
        self.newbutton(frmHelp_W, btntext = 'Config',   btncommand='print_config',   btnwidth=15, style = 'help-normal', side=TOP)
        self.newbutton(frmHelp_W, btntext = 'FileSets', btncommand='print_filesets', btnwidth=15, style = 'help-normal', side=TOP)
        self.newbutton(frmHelp_W, btntext = 'GUI Elements', btncommand='print_entryvars', btnwidth=15, style = 'help-normal', side=TOP)



        #-------------- RUN!!!
        print('approot: ' + self.entryvar('approot') )
        print('config:  ' + self.entryvar('config') )
        print('systems: ' + self.entryvar('systems') )


        self.coa = tdcoa(approot = self.entryvar('approot'))
        self.version = self.coa.version
        self.entryvars['secrets'].set(self.first_file_that_exists(self.coa.settings['secrets'], os.path.join(self.entryvar('approot'),"secrets.yaml")))
        self.coa.reload_config(skip_git = True, secrets=self.entryvar('secrets'))
        self.entryvars['bteq_delim'].set(value=self.coa.bteq_delim)
        print('secrets: ' + self.entryvar('secrets') )

        self.coa.deactivate_all()
        self.upload_get_lastrun_folder()

        # these 'clicks' will refresh both left and right treeviews
        self.button_click('tv_systems_left')
        self.button_click('tv_filesets_left')


        # sync skip_dbs flag with coa.settings
        self.entryvars['skip_dbs_toggle'].set(value=self.validate_boolean(self.coa.settings['skip_dbs'],'int'))

        Label(frmHelp_N, text='Version of tdcsm = %s' %self.coa.version, style='help-bold.TLabel').pack(fill=X, anchor=N)
        Label(frmHelp_N, text='Version of tdgui = %s' %self.version, style='help-bold.TLabel').pack(fill=X, anchor=N)

        app.bind('<Escape>', self.close)
        #app.bind('<Left>',  self.select_tab(shift=-1))
        #app.bind('<Right>', self.select_tab(shift=1) )
        app.mainloop()
