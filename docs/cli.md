# tdcsm command line interface

In addition to the default *gui* interface, **tdcsm** supports a command line utility, also named `tdcsm`, which can be used to access some of the application functionality. It is important to note that the `tdcsm` cli utility will be available only when the python environment which has *tdcsm* application installed is active, that is, if *tdcsm* package was installed in a python virtual environment, it must be active.

## General Format

General format of `tdcsm` utility is:
```sh
tdcsm [<global-options>] [<command> [<options>] [<sub-command> [<options>]]]
```

Examples:
```sh
tdcsm -h
tdcsm gui
tdcsm systems list -a
```

*Notes:*
1. Square brackets indicate optional items
1. Angular brackets indicate an actual value must be provided instead of the literally typing the text within
1. A brief help message can be shown using `-h` or `--help` option and can be used with commands and sub-commands

## Global Options

Following global options can be specified and are applicable to all commands.
- `--approot` overrides the default current directory as the application root folder
- `--secrets` specifies the location of `secrets.yaml` file **relative to** the application folder

## Commands

### `gui`

This command starts a *gui* session. It is also the default command when no command is explicitly specified, that is, `tdcsm` without any other parameters is equivalent to `tdcsm gui`

### `init`

Initializes a folder by downloading and creating default folders and files. This command is generally only used once when running the *tdcsm* application for the first time

### `systems`

Allows listing and modifying some of the attributes of the source systems defined in the `source_systems.yaml` file. It further offers following sub-commands
1. **list**: list with all or named source systems. This is also the default sub-command. Supported options:
   - `-v`: lists additional information, such as *site-id* and active *filesets* for each source system
   - `-a`: lists only the active systems
1. **enable**: sets an inactive source-system to active status
1. **disable**: sets an active source-system to inactive status
1. **activate**: activates one or more *filesets* for the given source-system
1. **deactivate**: deactivates one or more *filesets* for the given source-system

### `filesets`

Allows listing all or named *filesets*. Supported options:
- `-v`: lists additional information, shows each *gitfile* that make up the *fileset*
- `-a`: lists only the active entries
