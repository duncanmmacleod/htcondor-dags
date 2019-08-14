#!/usr/bin/env python

import os
import re
import json
import argparse


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("src")
    parser.add_argument(
        "--test-mode",
        help="Testing mode (only transfers small files)",
        default=False,
        action="store_true",
        dest="test_mode",
    )

    args = parser.parse_args()

    return args


_SIMPLE_FNAME_RE = re.compile("^[0-9A-Za-z_./:\-]+$")


def simple_fname(fname):
    return _SIMPLE_FNAME_RE.match(fname)


def generate_file_listing(src, manifest, test_mode=False):
    with open(manifest, "w") as fp:
        for root, dirs, fnames in os.walk(src):
            for fname in fnames:
                full_fname = os.path.normpath(os.path.join(root, fname))
                size = os.stat(full_fname).st_size
                if test_mode and size > 50 * 1024 * 1024:
                    continue
                if simple_fname(full_fname):
                    fp.write("{} {}\n".format(full_fname, size))
                else:
                    info = {"name": full_fname, "size": size}
                    fp.write("{}\n".format(json.dumps(info)))


if __name__ == "__main__":
    args = parse_args()

    generate_file_listing(args.src, "source_manifest.txt")
