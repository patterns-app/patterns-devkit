"""
Adapted from docstring-parser, Copyright (c) 2018 Marcin Kurczewski, License MIT
"""

import enum
import inspect
import re
import typing as T
from collections import OrderedDict, namedtuple
from enum import IntEnum

PARAM_KEYWORDS = {
    "param",
    "parameter",
}
INPUT_KEYWORDS = {
    "input",
    "inputs",
}
OUTPUT_KEYWORDS = {"output", "outputs"}
RAISES_KEYWORDS = {"raises", "raise", "except", "exception"}
RETURNS_KEYWORDS = {"return", "returns"}
YIELDS_KEYWORDS = {"yield", "yields"}


class ParseError(RuntimeError):
    """Base class for all parsing related errors."""


class DocstringStyle(enum.Enum):
    """Docstring style."""

    rest = 1
    google = 2
    numpydoc = 3
    auto = 255


class DocstringMeta:
    """Docstring meta information.

    Symbolizes lines in form of

        :param arg: description
        :raises ValueError: if something happens
    """

    def __init__(self, args: T.List[str], description: str) -> None:
        """Initialize self.

        :param args: list of arguments. The exact content of this variable is
                     dependent on the kind of docstring; it's used to distinguish between
                     custom docstring meta information items.
        :param description: associated docstring description.
        """
        self.args = args
        self.description = description


class DocstringParam(DocstringMeta):
    """DocstringMeta symbolizing :param metadata."""

    def __init__(
        self,
        args: T.List[str],
        description: T.Optional[str],
        arg_name: str,
        type_name: T.Optional[str],
        is_optional: T.Optional[bool],
        default: T.Optional[str],
    ) -> None:
        """Initialize self."""
        super().__init__(args, description)
        self.arg_name = arg_name
        self.type_name = type_name
        self.is_optional = is_optional
        self.default = default


class DocstringInput(DocstringMeta):
    """"""

    def __init__(
        self,
        args: T.List[str],
        description: T.Optional[str],
        input_name: str,
        type_name: T.Optional[str],
        is_optional: T.Optional[bool],
        default: T.Optional[str],
    ) -> None:
        """Initialize self."""
        super().__init__(args, description)
        self.input_name = input_name
        self.type_name = type_name
        self.is_optional = is_optional
        self.default = default


class DocstringReturns(DocstringMeta):
    """DocstringMeta symbolizing :returns or :yields metadata."""

    def __init__(
        self,
        args: T.List[str],
        description: T.Optional[str],
        type_name: T.Optional[str],
        is_generator: bool,
        return_name: T.Optional[str] = None,
    ) -> None:
        """Initialize self."""
        super().__init__(args, description)
        self.type_name = type_name
        self.is_generator = is_generator
        self.return_name = return_name


class DocstringRaises(DocstringMeta):
    """DocstringMeta symbolizing :raises metadata."""

    def __init__(
        self,
        args: T.List[str],
        description: T.Optional[str],
        type_name: T.Optional[str],
    ) -> None:
        """Initialize self."""
        super().__init__(args, description)
        self.type_name = type_name
        self.description = description


class DocstringDeprecated(DocstringMeta):
    """DocstringMeta symbolizing deprecation metadata."""

    def __init__(
        self,
        args: T.List[str],
        description: T.Optional[str],
        version: T.Optional[str],
    ) -> None:
        """Initialize self."""
        super().__init__(args, description)
        self.version = version
        self.description = description


class Docstring:
    """Docstring object representation."""

    def __init__(
        self,
        style=None,  # type: T.Optional[DocstringStyle]
    ) -> None:
        """Initialize self."""
        self.short_description = None  # type: T.Optional[str]
        self.long_description = None  # type: T.Optional[str]
        self.blank_after_short_description = False
        self.blank_after_long_description = False
        self.meta = []  # type: T.List[DocstringMeta]
        self.style = style  # type: T.Optional[DocstringStyle]

    @property
    def params(self) -> T.List[DocstringParam]:
        return [item for item in self.meta if isinstance(item, DocstringParam)]

    @property
    def inputs(self) -> T.List[DocstringInput]:
        return [item for item in self.meta if isinstance(item, DocstringInput)]

    @property
    def raises(self) -> T.List[DocstringRaises]:
        return [item for item in self.meta if isinstance(item, DocstringRaises)]

    @property
    def returns(self) -> T.Optional[DocstringReturns]:
        for item in self.meta:
            if isinstance(item, DocstringReturns):
                return item
        return None

    @property
    def deprecation(self) -> T.Optional[DocstringDeprecated]:
        for item in self.meta:
            if isinstance(item, DocstringDeprecated):
                return item
        return None


class SectionType(IntEnum):
    """Types of sections."""

    SINGULAR = 0
    """For sections like examples."""

    MULTIPLE = 1
    """For sections like params."""

    SINGULAR_OR_MULTIPLE = 2
    """For sections like returns or yields."""


class Section(namedtuple("SectionBase", "title key type")):
    """A docstring section."""


GOOGLE_TYPED_ARG_REGEX = re.compile(r"\s*(.+?)\s*\(\s*(.*[^\s]+)\s*\)")
GOOGLE_ARG_DESC_REGEX = re.compile(r".*\. Defaults to (.+)\.")
MULTIPLE_PATTERN = re.compile(r"(\s*[^:\s]+:)|([^:]*\]:.*)")

DEFAULT_SECTIONS = [
    Section("Inputs", "input", SectionType.MULTIPLE),
    # Section("Input", "input", SectionType.MULTIPLE),
    Section("Parameters", "param", SectionType.MULTIPLE),
    Section("Params", "param", SectionType.MULTIPLE),
    # Section("Raises", "raises", SectionType.MULTIPLE),
    # Section("Exceptions", "raises", SectionType.MULTIPLE),
    # Section("Except", "raises", SectionType.MULTIPLE),
    # Section("Attributes", "attribute", SectionType.MULTIPLE),
    Section("Example", "examples", SectionType.SINGULAR),
    Section("Examples", "examples", SectionType.SINGULAR),
    Section("Returns", "returns", SectionType.SINGULAR_OR_MULTIPLE),
    Section("Yields", "yields", SectionType.SINGULAR_OR_MULTIPLE),
    Section("Output", "output", SectionType.SINGULAR_OR_MULTIPLE),
    Section("Outputs", "output", SectionType.SINGULAR_OR_MULTIPLE),
]


class BasisParser:
    def __init__(self, sections: T.Optional[T.List[Section]] = None, title_colon=True):
        """Setup sections.

        :param sections: Recognized sections or None to defaults.
        :param title_colon: require colon after section title.
        """
        if not sections:
            sections = DEFAULT_SECTIONS
        self.sections = {s.title: s for s in sections}
        self.title_colon = title_colon
        self._setup()

    def _setup(self):
        if self.title_colon:
            colon = ":"
        else:
            colon = ""
        self.titles_re = re.compile(
            "^("
            + "|".join("(%s)" % t for t in self.sections)
            + ")"
            + colon
            + "[ \t\r\f\v]*$",
            flags=re.M,
        )

    def _build_meta(self, text: str, title: str) -> DocstringMeta:
        """Build docstring element.

        :param text: docstring element text
        :param title: title of section containing element
        :return:
        """

        section = self.sections[title]

        if (
            section.type == SectionType.SINGULAR_OR_MULTIPLE
            and not MULTIPLE_PATTERN.match(text)
        ) or section.type == SectionType.SINGULAR:
            return self._build_single_meta(section, text)

        if ":" not in text:
            raise ParseError("Expected a colon in {}.".format(repr(text)))

        # Split spec and description
        before, desc = text.split(":", 1)
        if desc:
            desc = desc[1:] if desc[0] == " " else desc
            if "\n" in desc:
                first_line, rest = desc.split("\n", 1)
                desc = first_line + "\n" + inspect.cleandoc(rest)
            desc = desc.strip("\n")

        return self._build_multi_meta(section, before, desc)

    def _build_single_meta(self, section: Section, desc: str) -> DocstringMeta:
        if section.key in RETURNS_KEYWORDS | YIELDS_KEYWORDS | OUTPUT_KEYWORDS:
            return DocstringReturns(
                args=[section.key],
                description=desc,
                type_name=None,
                is_generator=section.key in YIELDS_KEYWORDS,
            )
        if section.key in RAISES_KEYWORDS:
            return DocstringRaises(args=[section.key], description=desc, type_name=None)
        if section.key in PARAM_KEYWORDS | INPUT_KEYWORDS:
            raise ParseError("Expected parameter or input name.")
        return DocstringMeta(args=[section.key], description=desc)

    def _build_multi_meta(
        self, section: Section, before: str, desc: str
    ) -> DocstringMeta:
        if section.key in PARAM_KEYWORDS | INPUT_KEYWORDS:
            m = GOOGLE_TYPED_ARG_REGEX.match(before)
            if m:
                arg_name, type_name = m.group(1, 2)
                if type_name.endswith(", optional"):
                    is_optional = True
                    type_name = type_name[:-10]
                elif type_name.endswith("?"):
                    is_optional = True
                    type_name = type_name[:-1]
                else:
                    is_optional = False
            else:
                arg_name, type_name = before, None
                is_optional = None

            m = GOOGLE_ARG_DESC_REGEX.match(desc)
            default = m.group(1) if m else None

            if section.key in PARAM_KEYWORDS:
                return DocstringParam(
                    args=[section.key, before],
                    description=desc,
                    arg_name=arg_name,
                    type_name=type_name,
                    is_optional=is_optional,
                    default=default,
                )
            if section.key in INPUT_KEYWORDS:
                return DocstringInput(
                    args=[section.key, before],
                    description=desc,
                    input_name=arg_name,
                    type_name=type_name,
                    is_optional=is_optional,
                    default=default,
                )
        if section.key in RETURNS_KEYWORDS | YIELDS_KEYWORDS | OUTPUT_KEYWORDS:
            return DocstringReturns(
                args=[section.key, before],
                description=desc,
                type_name=before,
                is_generator=section.key in YIELDS_KEYWORDS,
            )
        if section.key in RAISES_KEYWORDS:
            return DocstringRaises(
                args=[section.key, before], description=desc, type_name=before
            )
        return DocstringMeta(args=[section.key, before], description=desc)

    def add_section(self, section: Section):
        """Add or replace a section.

        :param section: The new section.
        """

        self.sections[section.title] = section
        self._setup()

    def parse(self, text: str) -> Docstring:
        """Parse the Google-style docstring into its components.

        :returns: parsed docstring
        """
        ret = Docstring(style=DocstringStyle.google)
        if not text:
            return ret

        # Clean according to PEP-0257
        text = inspect.cleandoc(text)

        # Find first title and split on its position
        match = self.titles_re.search(text)
        if match:
            desc_chunk = text[: match.start()]
            meta_chunk = text[match.start() :]
        else:
            desc_chunk = text
            meta_chunk = ""

        # Break description into short and long parts
        parts = desc_chunk.split("\n", 1)
        ret.short_description = parts[0] or None
        if len(parts) > 1:
            long_desc_chunk = parts[1] or ""
            ret.blank_after_short_description = long_desc_chunk.startswith("\n")
            ret.blank_after_long_description = long_desc_chunk.endswith("\n\n")
            ret.long_description = long_desc_chunk.strip() or None

        # Split by sections determined by titles
        matches = list(self.titles_re.finditer(meta_chunk))
        if not matches:
            return ret
        splits = []
        for j in range(len(matches) - 1):
            splits.append((matches[j].end(), matches[j + 1].start()))
        splits.append((matches[-1].end(), len(meta_chunk)))

        chunks = OrderedDict()  # type: T.Mapping[str,str]
        for j, (start, end) in enumerate(splits):
            title = matches[j].group(1)
            if title not in self.sections:
                continue

            # Clear Any Unknown Meta
            # Ref: https://github.com/rr-/docstring_parser/issues/29
            meta_details = meta_chunk[start:end]
            unknown_meta = re.search(r"\n\S", meta_details)
            if unknown_meta is not None:
                meta_details = meta_details[: unknown_meta.start()]

            chunks[title] = meta_details.strip("\n")
        if not chunks:
            return ret

        # Add elements from each chunk
        for title, chunk in chunks.items():
            # Determine indent
            indent_match = re.search(r"^\s+", chunk)
            if not indent_match:
                raise ParseError('Can\'t infer indent from "{}"'.format(chunk))
            indent = indent_match.group()

            # Check for singular elements
            if self.sections[title].type in [
                SectionType.SINGULAR,
                SectionType.SINGULAR_OR_MULTIPLE,
            ]:
                part = inspect.cleandoc(chunk)
                ret.meta.append(self._build_meta(part, title))
                continue

            # Split based on lines which have exactly that indent
            _re = "^" + indent + r"(?=\S)"
            c_matches = list(re.finditer(_re, chunk, flags=re.M))
            if not c_matches:
                raise ParseError('No specification for "{}": "{}"'.format(title, chunk))
            c_splits = []
            for j in range(len(c_matches) - 1):
                c_splits.append((c_matches[j].end(), c_matches[j + 1].start()))
            c_splits.append((c_matches[-1].end(), len(chunk)))
            for j, (start, end) in enumerate(c_splits):
                part = chunk[start:end].strip("\n")
                ret.meta.append(self._build_meta(part, title))

        return ret
