from __future__ import print_function, division, absolute_import

import importlib
import logging
import os
import os.path

from docutils.core import publish_string
from docutils.writers import Writer

_logger = logging.getLogger(__name__)


def main():
    self_path = os.path.dirname(__file__)
    docs_dir = os.path.abspath(os.path.join(self_path, '..'))
    src_dir = os.path.abspath(os.path.join(self_path, '..', 'docs', 'src'))

    for fname in relwalk(src_dir):
        if not fname.endswith('.md'):
            continue

        source = os.path.join(src_dir, fname)
        target = os.path.join(docs_dir, fname)

        _logger.info('transform %s -> %s', source, target)

        with open(source, 'rt') as fobj:
            content = fobj.read()

        content = transform(content)

        with open(target, 'wt') as fobj:
            fobj.write(content)


def relwalk(absroot, relroot='.'):
    for fname in os.listdir(absroot):
        relpath = os.path.join(relroot, fname)
        abspath = os.path.join(absroot, fname)

        if fname in {'.', '..'}:
            continue

        if os.path.isfile(abspath):
            yield relpath

        elif os.path.isdir(abspath):
            yield from relwalk(abspath, relpath)


def transform(content):
    lines = []
    for line in content.splitlines():
        if line.startswith('.. autofunction::'):
            lines += autofunction(line)

        else:
            lines.append(line)

    return '\n'.join(lines)


def autofunction(line):
    _, what = line.split('::')
    obj = import_object(what)

    yield '## {}'.format(what)
    yield ''
    yield render_docstring(obj)


def render_docstring(obj):
    doc = obj.__doc__ or '<undocumented>'
    doc = unindent(doc)

    return publish_string(
        doc,
        writer=MarkdownWriter(),
        settings_overrides={'output_encoding': 'unicode'}
    )


class MarkdownWriter(Writer):
    def translate(self):
        self.output = ''.join(self._translate(self.document))

    def _translate(self, node):
        func = '_translate_{}'.format(type(node).__name__)
        func = getattr(self, func)
        return func(node)

    def _translate_children(self, node):
        for c in node.children:
            yield from self._translate(c)

    _translate_document = _translate_children

    def _translate_paragraph(self, node):
        yield from self._translate_children(node)
        yield '\n\n'

    def _translate_literal_block(self, node):
        yield '```\n'
        yield node.astext()
        yield '\n'
        yield '```\n'
        yield '\n'

    def _translate_Text(self, node):
        yield node.astext()

    def _translate_literal(self, node):
        yield '`{}`'.format(node.astext())

    def _translate_field_name(self, node):
        yield '**{}** '.format(node.astext())

    _translate_field_list = _translate_field = _translate_field_body = _translate_children


def unindent(doc):
    def impl():
        lines = doc.splitlines()
        indent = find_indet(lines)

        if lines:
            yield lines[0]

        for line in lines[1:]:
            yield line[indent:]

    return '\n'.join(impl())


def find_indet(lines):
    for line in lines[1:]:
        if not line.strip():
            continue

        return len(line) - len(line.lstrip())

    return 0


def import_object(what):
    mod, _, what = what.rpartition('.')
    mod = mod.strip()
    what = what.strip()

    mod = importlib.import_module(mod)
    return getattr(mod, what)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
