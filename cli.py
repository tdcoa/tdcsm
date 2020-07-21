#! /usr/bin/env python
"tdcsm command-line interface"

import argparse
from pathlib import Path
from typing import Any, Sequence, Callable, List, Dict
from yaml import safe_load
from tdcsm.tdgui import coa


root = Path.cwd()


def gui() -> None:
	"invoe tdcsm GUI"
	coa()


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


def show_systems(verbose: bool, active: bool) -> None:
	"show source system information"
	def make_row(k: str, p: Dict[str, Any]) -> List[str]:
		return [k, 'Yes' if p['active'].lower() == 'true' else 'No']

	def make_details(k: str, p: Dict[str, Any]) -> List[str]:
		return make_row(k, p) + [p["siteid"], ','.join(f for f, a in p['filesets'].items() if a['active'] == 'True')]

	with open(root / 'source_systems.yaml') as f:
		systems = safe_load(f)['systems'].items()

	if active:
		systems = filter(lambda kp: kp[1]['active'].lower() == 'true', systems)

	if verbose:
		tabulate([make_details(k, p) for k, p in systems], ["System", "Active", "Site ID", "Filesets"])
	else:
		tabulate([make_row(k, p) for k, p in systems], ["System", "Active"])


def show_filesets(verbose: bool, active: bool) -> None:
	"show filesets information"
	with open(root / '1_download' / 'filesets.yaml') as f:
		sets = safe_load(f).items()

	def make_row(k: str, p: Dict[str, Any]) -> List[str]:
		return [k, 'Yes' if p['active'].lower() == 'true' else 'No', p['fileset_version']]

	def make_details(k: str, p: Dict[str, Any], f: Dict[str, Any]) -> List[str]:
		return make_row(k, p) + [f['gitfile']]

	if active:
		sets = filter(lambda kp: kp[1]['active'].lower() == 'true', sets)

	if verbose:
		tabulate([make_details(k, p, f) for k, p in sets for f in p['files'].values()], ["System", "Active", "Version", "GIT File"])
	else:
		tabulate([make_row(k, p) for k, p in sets], ["System", "Active", "Version"])


def run_sets(action: str) -> None:
	"run an action, can be all which runs all actions"
	from tdcsm.tdcoa import tdcoa

	app = tdcoa()

	if action in ['download', 'all']:
		app.download_files()
	if action in ['prepare', 'all']:
		app.prepare_sql()
	if action in ['execute', 'all']:
		app.execute_run()
	if action in ['upload', 'all']:
		app.upload_to_transcend()


def run(approot: Path, secrets: Path, cmd: Callable, **kwargs: Any) -> None:
	"run script with given validated parameters"
	global root

	root = approot
	secrets_path = approot / secrets
	if not secrets_path.is_file():
		raise SystemExit("Secrets file '{}' not found in '{}'".format(secrets_path, approot))

	cmd(**kwargs)


def main(argv: Sequence[str] = None) -> None:
	"script entry-point"
	def folder(v: str) -> Path:
		"returns Path from string if it exists and a folder"
		if Path(v).is_dir():
			return Path(v)
		raise argparse.ArgumentTypeError("'%s' is not a valid file name" % v)

	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument('--approot', type=folder, default=Path.cwd(),
		help='directory of the approot (working directory) in which to house all COA collateral')
	parser.add_argument('--secrets', type=Path, default='secrets.yaml', help='location to your secrets.yaml file RELATIVE TO APPROOT directory above')

	parser.set_defaults(cmd=gui)

	subp = parser.add_subparsers(help='Choose one sub-command')

	p = subp.add_parser('gui', help='Start a GUI session')
	p.set_defaults(cmd=gui)

	p = subp.add_parser('systems', help='Source System information')
	p.set_defaults(cmd=show_systems)
	p.add_argument('-a', '--active', action='store_true', help='show only active entries')
	p.add_argument('-v', '--verbose', action='store_true', help='also include active filesets')

	p = subp.add_parser('filesets', help='Filesets information')
	p.set_defaults(cmd=show_filesets)
	p.add_argument('-a', '--active', action='store_true', help='show only active entries')
	p.add_argument('-v', '--verbose', action='store_true', help='also include gitfile names')

	p = subp.add_parser('run', help='Run actions against filesets')
	p.set_defaults(cmd=run_sets)
	p.add_argument('action', choices=['download', 'prepare', 'execute', 'upload', 'all'], help='actions to run')

	run(**vars(parser.parse_args(argv)))


if __name__ == '__main__':
	main()
