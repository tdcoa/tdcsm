"Teradata datbase utility module"
from typing import Optional
import logging
from json import dumps
from decimal import Decimal
import pandas as pd
import teradatasql as db

logging.basicConfig(format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def connect(host: str, username: str, password: str, encryption: str = 'false', logmech: Optional[str] = None) -> db.TeradataConnection:
	"return database connection"
	conn_opts = dict(host=host, user=username, password=password)
	if encryption:
		conn_opts['encryptdata'] = encryption
	if logmech:
		conn_opts['logmech'] = logmech

	conn_str = dumps(conn_opts)
	logger.debug("connect string: %s", conn_str.replace(password, "*****"))

	return db.connect(conn_str)


def sql_to_df(conn: db.TeradataConnection, sql: str) -> pd.DataFrame:
	"run sql using database connection and return result as a pandas dataframe"
	logger.debug('preparing to execute: "%s"', sql)

	with conn.cursor() as csr:
		csr.execute(sql)

		columns = [d[0] for d in csr.description]
		data = csr.fetchall()

		logger.debug("rows: %d, columns: %s", csr.rowcount, columns)

	# emulate pandas.read_sql_query(coerce_float = True)
	return pd.DataFrame(data=([float(c) if isinstance(c, Decimal) else c for c in row] for row in data), columns=columns)


def df_to_sql(
	conn: db.TeradataConnection,
	df: pd.DataFrame,
	table: str,
	schema: str,
	copy_sfx: Optional[str] = None
) -> None:
	"""
	Save pandas dataframe to a database table.
	- dataframe's column names are used as table column names.
	- table must exist and must have all non-defaulted columns in dataframe
	- if a unique_sfx is not None, a new table is created by appending _suffix
	- otherwise data from dataframe is inserted into target dataframe
	- if append is not set, data is deleted first from the table
	"""

	collist = ','.join(f'"{c}"' for c in df.columns.values.tolist())
	parms = ','.join(['?'] * len(df.columns))

	with conn.cursor() as csr:
		csr.execute(f'INSERT INTO "{schema}"."{table}"({collist}) VALUES({parms})', df.values.tolist())
		if copy_sfx is not None:
			copy = f"{table}_{copy_sfx}"
			try:
				csr.execute(f'CREATE MULTISET TABLE "{schema}"."{copy}" AS "{schema}"."{table}" WITH DATA')
			except db.OperationalError:
				csr.execute(f'DELETE FROM "{schema}"."{copy}"')
				csr.execute(f'INSERT INTO "{schema}"."{copy}" SELECT * FROM "{schema}"."{table}"')


def run(sql: str, sysname: str, debug: bool = False) -> None:
	"script entry-point"
	from .tdcoa import tdcoa

	if debug:
		logger.setLevel(logging.DEBUG)

	srcsys = tdcoa().systems[sysname]
	conn = connect(host=srcsys["host"], username=srcsys["username"], password=srcsys["password"], logmech=srcsys["logmech"])
	df = sql_to_df(conn, sql)

	print(df)

	logger.debug("closing connection")
	conn.close()


if __name__ == '__main__':
	from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

	p = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)

	p.add_argument("sql", nargs='?', default='select * from dbc.dbcinfov', help="sql query to test")
	p.add_argument("--sysname", default='Transcend_Source', help="Source system to connect to")
	p.add_argument("--debug", action='store_true', help="show debug messages")

	run(**vars(p.parse_args()))
