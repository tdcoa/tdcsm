"Teradata datbase utility module"
from typing import Optional
from json import dumps
from decimal import Decimal
import pandas as pd
import teradatasql as db


def connect(host: str, username: str, password: str, encryption: str = 'false', logmech: Optional[str] = None) -> db.TeradataConnection:
	"return database connection"
	conn_opts = dict(host=host, user=username, password=password)
	if encryption:
		conn_opts['encryptdata'] = encryption
	if logmech:
		conn_opts['logmech'] = logmech

	return db.connect(dumps(conn_opts))


def sql_to_df(conn: db.TeradataConnection, sql: str) -> pd.DataFrame:
	"run sql using database connection and return result as a pandas dataframe"
	with conn.cursor() as csr:
		csr.execute(sql)
		columns = [d[0] for d in csr.description]
		data = csr.fetchall()

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
