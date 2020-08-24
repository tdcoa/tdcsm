"Powerpoint instantiation from a template"
from __future__ import annotations

import abc
import csv
import logging
import re
from argparse import ArgumentParser
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Tuple, Iterable, List, Any, ClassVar, Optional, Union, Dict, Type, cast

import pptx
from pptx.presentation import Presentation
from pptx.shapes.base import BaseShape
from pptx.shapes.shapetree import SlideShapes, GroupShapes
from pptx.enum.shapes import MSO_SHAPE_TYPE

ShapeContainer = Union[SlideShapes, GroupShapes]

Loc = Union[None, int, str]
logging.basicConfig(format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
tags: Dict[str, Type[Placeholder]] = {}


@dataclass
class Placeholder(abc.ABC):
	"Base, Abastract class for place-holders"
	tag: ClassVar[str] = '?'

	slnum: int
	container: ShapeContainer
	shape: BaseShape
	datafile: str
	loc: Loc  # location of the pattern, can be a column-num for tables or the pattern string

	def __str__(self) -> str:
		loc = f", loc={self.loc}" if self.loc is not None else ''
		return f"type={self.tag}, data={self.datafile}, slide#={self.slnum}, shape={self.shape.name}{loc}"

	@abc.abstractmethod
	def replace(self, datapath: Path) -> None:
		"replace placeholders with values loaded from the datapath"

	@classmethod
	def parse(cls, slnum: int, container: ShapeContainer, shape: BaseShape, data: str, loc: Loc) -> Placeholder:
		"parse values to instantiate an object"
		return cls(slnum, container, shape, data, loc)

	@classmethod
	def __init_subclass__(cls, **_: Any) -> None:
		tags[cls.tag] = cls


@dataclass
class PicPlaceHolder(Placeholder):
	"Placeholder for a Picture"
	tag: ClassVar[str] = 'pic'

	def replace(self, datapath: Path) -> None:
		logger.debug("Replacing: %s", self.datafile)
		self.container.add_picture(
			str(datapath / self.datafile),
			left=self.shape.left,
			top=self.shape.top,
			height=self.shape.height,
			width=self.shape.width
		)
		self.shape.text = f'**{datapath / self.datafile}**'

	@classmethod
	def parse(cls, slnum: int, container: ShapeContainer, shape: BaseShape, data: str, _: Loc) -> Placeholder:
		return cls(slnum, container, shape, data, None)


@dataclass
class ColPlaceHolder(Placeholder):
	"Placeholder for a column of values from a csv file"
	tag: ClassVar[str] = 'col'

	colnum: int

	def replace(self, datapath: Path) -> None:
		logger.debug("Replacing: %s[%d]", self.datafile, self.colnum)
		data = (r[cast(int, self.loc)] for r in load_csv(datapath / self.datafile))
		cells = (r.cells[self.loc] for r in self.shape.table.rows)

		for cell, datum in zip(cells, data):
			cell.text = str(datum)

	@classmethod
	def parse(cls, slnum: int, container: ShapeContainer, shape: BaseShape, data: str, loc: Loc) -> Placeholder:
		m = re.fullmatch(r"([^\[\]]+)\[(\d+)\]", data)
		if m is None:
			raise ValueError("valid 'col' specification must '<data>[<colnum>]'")
		return cls(slnum, container, shape, m.group(1), loc, int(m.group(2)))


@dataclass
class ValPlaceHolder(Placeholder):
	"Placeholder for a single value from a csv file"
	tag: ClassVar[str] = 'val'

	rownum: int
	colnum: int

	def replace(self, datapath: Path) -> None:
		try:
			data = load_csv(datapath / self.datafile)[self.rownum][self.colnum - 1]
		except IndexError:
			data = None

		if data is None:
			raise ValueError(f'Invalid cell#[{self.rownum}:{self.colnum}] in {datapath / self.datafile}')

		pat = cast(str, self.loc)
		logger.debug("Replacing: %s", pat)
		for e, para in enumerate(self.shape.text_frame.paragraphs):
			fnm, fsz, fbd, fil = para.font.name, para.font.size, para.font.bold, para.font.italic
			for r in para.runs:
				if r.font.name is not None:
					fnm, fsz, fbd, fil = r.font.name, r.font.size, r.font.bold, r.font.italic
				break
			if pat in para.text:
				logger.debug("Paragraph# %d, Font Name: %s, Size: %s, Bold: %s, Ital: %s", e, fnm, str(fsz), fbd, fil)
				para.text = para.text.replace(pat, data)
				para.font.name, para.font.size, para.font.bold, para.font.italic = fnm, fsz, fbd, fil
				return

	@classmethod
	def parse(cls, slnum: int, container: ShapeContainer, shape: BaseShape, data: str, loc: Loc) -> Placeholder:
		m = re.fullmatch(r"([^\[\]]+)\[(\d+):(\d+)\]", data)
		if m is None:
			raise ValueError(f"{data} is invalid 'val' specification; must '<data>[<rownum>:<colnum>]'")
		return cls(slnum, container, shape, m.group(1), loc, int(m.group(2)), int(m.group(3)))


@lru_cache
def load_csv(csvpath: Path) -> List[List[Any]]:
	"load CSV file and return a list of all rows"
	with csvpath.open() as f:
		return list(csv.reader(f))


def find_placeholders(ppt: Presentation) -> Iterable[Placeholder]:
	"iterate (shape, palceholder) pair over all presentation placeholders"

	def iter_shapes(container: ShapeContainer) -> Iterable[Tuple[ShapeContainer, BaseShape]]:
		"iterate over member shapes of the container"
		for sh in container:
			if sh.shape_type == MSO_SHAPE_TYPE.GROUP:
				yield from iter_shapes(sh.shapes)
			else:
				yield (container, sh)

	def iter_tags(slnum: int, parent: ShapeContainer, shape: BaseShape) -> Iterable[Placeholder]:
		"extract tags from the shape"
		if shape.has_table:
			for e, c in enumerate(shape.table.rows[0].cells):
				m2 = re.fullmatch("{{([^:]+):(.+?)}}", c.text.rstrip())
				if m2:
					tag, datafile = m2.group(1), m2.group(2)
					yield tags[tag].parse(slnum, parent, shape, datafile, e)

		elif shape.has_text_frame:
			for m in re.finditer("{{([^:]+):(.+?)}}", shape.text):
				tag, datafile, pat = m.group(1), m.group(2), m.group(0)
				yield tags[tag].parse(slnum, parent, shape, datafile, pat)

	for slnum, slide in enumerate(ppt.slides, start=1):
		for container, shape in iter_shapes(slide.shapes):
			logger.debug(f"slide#: {slnum}, type: {shape.shape_type}, text?: {shape.has_text_frame}, table?: {shape.has_table}")
			try:
				yield from iter_tags(slnum, container, shape)
			except KeyError as ex:
				logger.error("Invalid replacement tag '%s' on slide# %d, shape name: %s", str(ex), slnum, shape.name)
			except ValueError as ex:
				logger.error("Invalid Tag[slide#=%d, shape name=%s]: %s", slnum, shape.name, str(ex))


def replace_placeholders(pptpath: Path, datapath: Path, output: Optional[Path] = None) -> None:
	"replace all place-holders with the contents from the file"
	load_csv.cache_clear()  # cache only for this call
	logger.debug('starting - load_csv cache_info: %s', str(load_csv.cache_info()))

	ppt = pptx.Presentation(pptpath)
	for ph in find_placeholders(ppt):
		ph.replace(datapath)
	ppt.save(output if output is not None else pptpath)

	logger.debug('finished - load_csv cache_info: %s', str(load_csv.cache_info()))


def run(ppttmpl: Path, pptout: Optional[Path] = None, data: Optional[Path] = None, debug: bool = False) -> None:
	"script entry-point"
	if debug:
		logger.setLevel(logging.DEBUG)

	if pptout is None:
		ppt = pptx.Presentation(ppttmpl)
		for ph in find_placeholders(ppt):
			print(str(ph))
	else:
		replace_placeholders(ppttmpl, (data if data is not None else ppttmpl.parent), output=pptout)


if __name__ == '__main__':
	p = ArgumentParser(description=__doc__)

	p.add_argument("ppttmpl", type=Path, help="Presentation template that contains place-holder markers")
	p.add_argument("pptout", type=Path, nargs='?', help="Perform replacements and write output")
	p.add_argument("--data", type=Path, help="folder containing substitution data, defaults to same path as template")
	p.add_argument("--debug", action='store_true', help="show debug messages")

	run(**vars(p.parse_args()))
