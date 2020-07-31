# tdcsm command line interface

In addition to the default *gui* interface, **tdcsm** supports a command line utility, also name `tdcsm`, which can be used to access some of the application functionality. It is important to note that the `tdcsm` cli utility will be available only when the python environment which has *tdcsm* application installed is active, that is, if *tdcsm* package was installed in a python virtual environment, it must be active.

## General Format

General format of `tdcsm` utility is:
```sh
tdcsm [<command> [<options>] [<sub-command> [<options>]]]
```

Examples:
```sh
tdcsm -h
tdcsm gui
tdcsm systems list -a
```

*Notes:*
1. *command*, and *sub-command* when applicable, offer manipulating a specific functionality as detailed later
1. Squre brackets indicate optional items
1. Angular brackets indicate an actual value must be used instead of the literally typing the text within
1. A brief help message can be shown using `-h` or `--help` option and is available for the main utility and for all of its  sub-commands.

## Global Options

Following two global options can be specified with any sub-commands.
- `--approot` overrides the default current directory as the application root folder
- `--secrets` specifies the location of `secrets.yaml` file **relative to** the application folder

## Sub Commands

### `gui`

This command starts a gui session. This is also the default sub-command. That is, typing `tdcsm` without any other parameters is equivalent to `tdcsm gui`

### `init`

Initializes a folder by downloading and creating default folders and files. This command is generally used only when starting the *tdcsm* application for the first time.

### `systems`

work with source systems defined in the `source_systems.yaml` file. It further offers following sub-commands
- **list**: list with all or named source systems. This is also the default sub-command. It supports two options:
- `-v`: lists additional information, such as *site-id* and active filesets for each source system
- `-a`: lists only the active entries
- **enable**: sets an inactive source-system to active status
- **disable**: sets an active source-system to inactive status
- **activate**: activates one or more *filesets* for the given system
- **deactivate**: deactivates one or more *filesets* for the given system

### `filesets`

list all or nameed filesets. This command offers following options:
- `-v`: lists additional information, shows each *gitfile* from the fileset
- `-a`: lists only the active entries
