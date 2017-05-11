from __future__ import print_function, division, absolute_import

import os
import os.path


def main():
    docs_dir = os.path.dirname(__file__)
    src_dir = os.path.join(docs_dir, 'src')

    for fname in os.listdir(src_dir):
        if not fname.endswith('.md'):
            continue

        source = os.path.join(src_dir, fname)
        target = os.path.join(docs_dir, fname)

        with open(source, 'r') as fobj:
            content = fobj.read()

        with open(target, 'w') as fobj:
            fobj.write(content)


if __name__ == "__main__":
    main()
