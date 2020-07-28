"Models"

from typing import Optional, Dict, Any
from pathlib import Path
from pydantic import BaseModel
from yaml import safe_load, dump


class SQLFile(BaseModel):
	"A single SQL file that is part of FileSet"
	class Config:
		"allow unknown attributes"
		extra = "allow"

	gitfile: str


class FileSet(BaseModel):
	"A set of SQL Files"
	class Config:
		"allow unknown attributes"
		extra = "allow"

	active: bool
	fileset_version: Optional[str]
	files: Dict[str, SQLFile]


class FilesetRef(BaseModel):
	"Fileset reference"
	class Config:
		"allow unknown attributes"
		extra = "allow"

	active: bool


class Transcend(BaseModel):
	"Teradata Transcend System"
	username: str
	password: str
	logmech: Optional[str] = "LDAP"
	host: str = "tdprd.td.teradata.com"
	db_coa: str = "adlste_coa"
	db_region: str = "adlste_westcomm"
	db_stg: str = "adlste_coa_stg"


class SrcSys(BaseModel):
	"Teradata Source System"
	class Config:
		"allow unknown attributes"
		extra = "allow"

	host: str
	username: str
	password: str
	siteid: str
	filesets: Dict[str, FilesetRef]
	logmech: Optional[str]
	encryption: bool = False
	active: bool = True
	driver: str = 'sqlalchemy'
	collection: str = 'pdcr'
	dbsversion: Optional[str] = '16.20'


def load_filesets(fname: str = 'filesets.yaml', download_dir: Path = Path.cwd() / '1_download') -> Dict[str, FileSet]:
	"load source systems"
	with open(download_dir / fname) as f:
		yaml = safe_load(f)

	return {n: FileSet(**v) for n, v in yaml.items()}


def load_srcsys(fname: str = 'source_systems.yaml', approot: Path = Path.cwd()) -> Dict[str, SrcSys]:
	"load source systems"
	with open(approot / fname) as f:
		yaml = safe_load(f)

	return {n: SrcSys(**v) for n, v in yaml['systems'].items()}


def dump_srcsys(sys: Dict[str, SrcSys], fname: str = 'source_systems.yaml', approot: Path = Path.cwd()) -> None:
	"load source systems"
	# TODO: remove legacy code that stores boolean as text in yaml
	def nobool(value: Any) -> Any:
		"return value, including nested, converted to string if it is bool or as is"
		if isinstance(value, dict):
			return {k: nobool(v) for k, v in value.items()}
		if isinstance(value, list):
			return [nobool(v) for v in value]
		if isinstance(value, bool):
			return str(value)
		return value

	with open(approot / fname, 'w') as f:
		dump({'systems': {k: nobool(v.dict()) for k, v in sys.items()}}, f)
