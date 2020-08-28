#! /usr/bin/env python
"tdcsm command-line interface"

import argparse
from pathlib import Path
from typing import Any, Sequence, Callable, List, Optional
from logging import getLogger

from .tdgui import coa as tdgui
from .tdcoa import tdcoa
from .model import load_filesets, load_srcsys, dump_srcsys, SrcSys, FileSet, SQLFile

logger = getLogger(__name__)
apppath = Path.cwd()
secrets = 'secrets.yaml'


def start_gui() -> None:
	"invoe tdcsm GUI"
	tdgui(str(apppath), secrets)


def tabulate(rows: Sequence[Sequence[str]], headers: Sequence[str]) -> None:
	"format and print tablulated data"
	widths = [len(h) for h in headers]
	if rows:
		widths = [max(w) for w in zip((max(map(len, col)) for col in zip(*rows)), widths)]

	fmt = '  '.join("{{:{}}}".format(w) for w in widths).format

	print(fmt(*headers))
	print(fmt(*('-' * w for w in widths)))
	for row in rows:
		print(fmt(*row))


def show_systems(name: Optional[List[str]] = None, verbose: bool = False, active: bool = False) -> None:
	"show source system information"
	def make_row(k: str, p: SrcSys) -> List[str]:
		return [k, 'Yes' if p.active else 'No']

	def make_details(k: str, p: SrcSys) -> List[str]:
		return make_row(k, p) + [p.siteid, ','.join(kf for kf, pf in p.filesets.items() if pf.active)]

	systems = load_srcsys(approot=apppath)

	if name:
		systems = {k: v for k, v in systems.items() if k in name}
	if active:
		systems = {k: v for k, v in systems.items() if v.active}

	if verbose:
		tabulate([make_details(k, p) for k, p in systems.items()], ["System", "Enabled", "Site ID", "Filesets"])
	else:
		tabulate([make_row(k, p) for k, p in systems.items()], ["System", "Enabled"])


def enable_systems(systems: Sequence[str], enable: bool = True) -> None:
	"show source system information"
	srcsys = load_srcsys(approot=apppath)

	changed = 0
	for n in systems:
		try:
			if srcsys[n].active != enable:
				srcsys[n].active = enable
				changed += 1
		except KeyError:
			logger.error("'%s' is not a valid source system name", n)

	if changed > 0:
		dump_srcsys(srcsys, approot=apppath)
	else:
		logger.warning('No source systems were changed')


def activate_filesets(system: str, filesets: Sequence[str], activate: bool = True) -> None:
	"show source system information"
	srcsys = load_srcsys(approot=apppath)

	if system not in srcsys:
		raise SystemExit(f"'{system}' is not a valid source system name")

	changed = 0
	for k in filesets:
		try:
			if srcsys[system].filesets[k].active != activate:
				srcsys[system].filesets[k].active = activate
				changed += 1
		except KeyError:
			logger.error("'%s' is not a valid fileset name", k)

	if changed > 0:
		dump_srcsys(srcsys, approot=apppath)
	else:
		logger.warning('No filesets were changed')


def show_filesets(name: List[str], verbose: bool, active: bool) -> None:
	"show filesets information"
	filesets = load_filesets()

	def make_row(k: str, p: FileSet) -> List[str]:
		return [k, 'Yes' if p.active else 'No', '' if p.fileset_version is None else p.fileset_version]

	def make_details(k: str, p: FileSet, f: SQLFile) -> List[str]:
		return make_row(k, p) + [f.gitfile]

	if active:
		filesets = {k: v for k, v in filesets.items() if v.active}
	if name:
		filesets = {k: v for k, v in filesets.items() if k in name}

	if verbose:
		tabulate([make_details(k, p, f) for k, p in filesets.items() for f in p.files.values()], ["System", "Active", "Version", "GIT File"])
	else:
		tabulate([make_row(k, p) for k, p in filesets.items()], ["System", "Active", "Version"])


def run_sets(action: Sequence[str]) -> None:
	"run an action, can be all which runs all actions"
	app = tdcoa(str(apppath), secrets)

	for a, fn in [('download', app.download_files), ('prepare', app.prepare_sql), ('execute', app.execute_run), ('upload', app.upload_to_transcend)]:
		if a in action:
			fn()


def first_time() -> None:
	"Initialize a folder for the first time"
	_ = tdcoa(str(apppath))


def run(approot: Path, secfile: str, cmd: Callable, **kwargs: Any) -> None:
	"run script with given validated parameters"
	global apppath, secrets

	apppath = approot
	if not (apppath / 'source_systems.yaml').exists() and cmd not in [first_time, start_gui]:
		raise SystemExit("Missing source_systems.yaml file, please use init or gui")

	secrets = secfile

	cmd(**kwargs)


def main(argv: Sequence[str] = None) -> None:
	"script entry-point"
	def add_subp_systems(parser: argparse.ArgumentParser) -> None:
		subp = parser.add_subparsers(help='Choose one sub-command', dest='cmd')
		parser.set_defaults(cmd=show_systems)

		p = subp.add_parser('list', help='list source systems')
		p.set_defaults(cmd=show_systems)
		p.add_argument('name', nargs='*', help='show only named systems')
		p.add_argument('-a', '--active', action='store_true', help='show only active (enabled) entries')
		p.add_argument('-v', '--verbose', action='store_true', help='also include active filesets')

		p = subp.add_parser('enable', help='enable source system')
		p.set_defaults(cmd=enable_systems)
		p.add_argument('systems', nargs='+', help='source system names')

		p = subp.add_parser('disable', help='disable source system')
		p.set_defaults(cmd=lambda systems: enable_systems(systems, enable=False))
		p.add_argument('systems', nargs='+', help='source system names')

		p = subp.add_parser('activate', help='activate filesets for a source system')
		p.set_defaults(cmd=activate_filesets)
		p.add_argument('system', help='source system name')
		p.add_argument('filesets', nargs='+', help='source system name')

		p = subp.add_parser('deactivate', help='deactivate filesets for a source system')
		p.set_defaults(cmd=lambda system, filesets: activate_filesets(system, filesets, activate=False))
		p.add_argument('system', help='source system name')
		p.add_argument('filesets', nargs='+', help='source system name')

	def folder(v: str) -> Path:
		"returns Path from string if it exists and a folder"
		if Path(v).is_dir():
			return Path(v)
		raise argparse.ArgumentTypeError("'%s' does not exist or is not a directory" % v)

	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument('--approot', type=folder, metavar='DIR', default=Path.cwd(),
		help='directory of the approot (working directory) in which to house all COA collateral')
	parser.add_argument('--secrets', dest='secfile', metavar='FILE', default=secrets,
		help='name of a YAML file, relative to APPROOT, containing secrets. default: secrents.yaml')

	parser.set_defaults(cmd=start_gui)

	subp = parser.add_subparsers(help='Choose one sub-command')

	p = subp.add_parser('gui', help='Start a GUI session')
	p.set_defaults(cmd=start_gui)

	p = subp.add_parser('init', help='Initialize COA folder for the first-time')
	p.set_defaults(cmd=first_time)

	p = subp.add_parser('systems', help='Source Systems')
	add_subp_systems(p)

	p = subp.add_parser('filesets', help='Filesets information')
	p.set_defaults(cmd=show_filesets)
	p.add_argument('name', nargs='*', help='show only named filesets')
	p.add_argument('-a', '--active', action='store_true', help='show only active entries')
	p.add_argument('-v', '--verbose', action='store_true', help='also include gitfile names')

	p = subp.add_parser('run', help='Run actions against filesets')
	p.set_defaults(cmd=run_sets)
	p.add_argument('action', nargs='+', choices=['download', 'prepare', 'execute', 'upload'], help='actions to run')

	run(**vars(parser.parse_args(argv)))


if __name__ == '__main__':
	main()
