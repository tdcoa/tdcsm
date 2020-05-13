# from tdcsm.tdcoa import tdcoa
import os
import shutil
from pathlib import Path
import pandas as pd
import yaml

from tdcsm.utils import Utils


def set_folder(path):
    # SECRETS - copy default if it does not exist in user provided path
    if not os.path.exists(os.path.join(path, 'secrets.yaml')):
        default_path = os.path.join(Path(__file__).parents[2], 'secrets.yaml')
        shutil.copyfile(default_path, os.path.join(path, 'secrets.yaml'))

    # CONFIG - copy default if it does not exist in user provided path
    if not os.path.exists(os.path.join(path, 'config.yaml')):
        default_path = os.path.join(Path(__file__).parents[2], 'config.yaml')
        shutil.copyfile(default_path, os.path.join(path, 'config.yaml'))


def test_connection(system_info):
    utils = Utils(version='0.0')

    connObject = utils.open_connection(
        conntype='sqlalchemy',  # todo allow for the other options (teradataml, sqlalchemy, odbc)
        system=system_info
    )

    # attempt sql call using connection object
    try:
        pd.read_sql('select * from dbc.tablesv sample 1', connObject['connection'])  # todo add better sql statement for test
        return True, ''
    except Exception as e:  # return first line of error
        return False, str(e).partition('\n')[0]


def save_source_system(source_systems, path):
    def get_secrets():
        x = {}
        for system_name, val_dict in source_systems.items():
            x['%s_username' % system_name.lower()] = val_dict['username']
            x['%s_password' % system_name.lower()] = val_dict['password']

            # replace with holders
            source_systems[system_name]['username'] = '{%s_username}' % system_name.lower()
            source_systems[system_name]['password'] = '{%s_password}' % system_name.lower()

        return x

    def save_config():
        # load config.yaml
        with open(os.path.join(path, 'config.yaml'), 'r') as f:
            full_config = f.readlines()

        # save instruction at top of config file for later
        instructions, config = [], []
        for i, line in enumerate(full_config):
            instructions.append(line) if line[0] == '#' else config.append(line)

        # load yaml into dict object
        config = yaml.load('\n'.join(config), Loader=yaml.FullLoader)

        # todo remove when this isnt in default config file
        try:
            del config['systems']['Altans_VDB']
        except Exception as e:
            pass

        config['systems'].update(source_systems)

        # dump yaml into stream and add appropriate styling
        stream = yaml.dump(config, default_flow_style=False, sort_keys=False)
        stream = stream.split('\n')
        system_section_flag = False
        for i, line in enumerate(stream):
            if line == 'systems:':
                system_section_flag = True
            elif line == 'transcend:':
                system_section_flag = False

            if (len(line) == len(line.lstrip())) and line != '\n':  # two empty lines between top level items
                stream[i] = '\n\n' + line

            elif (len(line) - len(line.lstrip()) == 2) and system_section_flag:  # add spacer line in systems sections between systems
                stream[i] = '\n' + line

            # for key in source_systems.keys():  # system name = key
            #     if key in line:
            #         stream[i] = '\n' + line

        stream = ''.join(instructions) + '\n'.join(stream)
        # print(stream)

        # save config.yaml
        with open(os.path.join(path, 'config.yaml'), 'w') as f:
            f.write(stream)

    def save_secrets(secrets):
        # load secrets.yaml
        with open(os.path.join(path, 'secrets.yaml'), 'r') as f:
            full_config = f.readlines()

        # save instruction at top of file for later
        instructions, s_config = [], []
        for i, line in enumerate(full_config):
            instructions.append(line) if line[0] == '#' else s_config.append(line)

        # load yaml into dict object
        s_config = yaml.load('\n'.join(s_config), Loader=yaml.FullLoader)

        # update yaml dict
        s_config['secrets'].update(secrets)

        # dump yaml into stream and add appropriate styling
        stream = yaml.dump(s_config, default_flow_style=False, sort_keys=False)
        stream = stream.split('\n')
        for i, line in enumerate(stream):
            if line == 'secrets:':
                stream[i] = '\n' + line

            elif 'password' in line:
                stream[i] = line + '\n'

        stream = ''.join(instructions) + '\n'.join(stream)
        # print(stream)

        # save secrets.yaml
        with open(os.path.join(path, 'secrets.yaml'), 'w') as f:
            f.write(stream)

    save_secrets(get_secrets())
    save_config()

