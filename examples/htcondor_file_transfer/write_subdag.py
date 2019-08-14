#!/usr/bin/env python

import os
import sys
import json
import argparse
import logging
import errno

from generate_file_listing import generate_file_listing


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("source_prefix")
    parser.add_argument("source_manifest")
    parser.add_argument("destination_prefix")
    parser.add_argument("destination_manifest")
    parser.add_argument("transfer_manifest")
    parser.add_argument(
        "--test-mode",
        help="Testing mode (only transfers small files)",
        default=False,
        action="store_true",
        dest="test_mode",
    )

    args = parser.parse_args()

    return args


def parse_manifest(prefix, manifest, log_name):
    prefix = os.path.normpath(prefix)
    files = {}
    with open(manifest, "r") as fd:
        for line in fd.readlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("{"):
                info = json.loads(line)
                if "name" not in info:
                    raise Exception(
                        "Manifest line missing 'name' key.  Current line: %s" % line
                    )
                fname = info["name"]
                if "size" not in info:
                    raise Exception(
                        "Manifest line missing 'size' key.  Currenty line: %s" % line
                    )
                size = int(info["size"])
            else:
                info = line.strip().split()
                if len(info) != 2:
                    raise Exception(
                        "Manifest lines must have two columns.  Current line: %s" % line
                    )
                fname = info[0].decode("utf-8")
                size = int(info[1])

            if not fname.startswith(prefix):
                logging.error(
                    "%s file (%s) does not start with specified prefix", log_name, fname
                )
            fname = fname[len(prefix) + 1 :]
            if not fname:
                logging.warning(
                    "%s file, stripped of prefix (%s), is empty", log_name, prefix
                )
                continue
            files[fname] = size
    return files


def write_subdag(
    source_prefix,
    source_manifest,
    destination_prefix,
    destination_manifest,
    transfer_manifest,
    test_mode=False,
):
    src_files = parse_manifest(source_prefix, source_manifest, "Source")

    generate_file_listing(destination_prefix, "destination_manifest.txt")
    dest_files = parse_manifest(
        destination_prefix, "destination_manifest.txt", "Destination"
    )

    files_to_xfer = set()
    for fname in src_files:
        if src_files[fname] != dest_files.get(fname, -1):
            files_to_xfer.add(fname)

    transfer_manifest = os.path.join(destination_prefix, "transfer_manifest.txt")
    if not os.path.exists(transfer_manifest):
        try:
            os.makedirs(destination_prefix)
        except OSError as oe:
            if oe.errno != errno.EEXIST:
                raise
        fd = os.open(transfer_manifest, os.O_CREAT | os.O_RDONLY)
        os.close(fd)

    files_verified = set()
    with open(transfer_manifest, "r") as fp:
        for line in fp.readlines():
            line = line.strip()
            if not line or line[0] == "#":
                continue
            info = line.strip().split()
            if info[0] != "TRANSFER_VERIFIED":
                continue
            if info[1] == "{":
                info = json.loads(" ".join(info[1:]))
                if "name" not in info or "digest" not in info or "size" not in info:
                    continue
            elif len(info) != 5:
                continue
            else:
                fname, hexdigest, size = info[1:-1]
            relative_fname = fname
            files_verified.add(relative_fname)

    files_to_verify = set()
    for fname in src_files:
        if fname in files_to_xfer:
            continue
        if fname not in files_verified:
            files_to_verify.add(fname)

    info = os.path.split(sys.argv[0])
    full_exec_path = os.path.join(os.path.abspath(info[0]), info[1])

    with open("xfer_file.sub", "w") as fp:
        fp.write(XFER_FILE_JOB.format(full_exec_path))
    with open("verify_file.sub", "w") as fp:
        fp.write(VERIFY_FILE_JOB.format(full_exec_path))

    idx = 0
    dest_dirs = set()
    jsonidx = 0
    cur_jsonidx = 0
    cmd_info = {}
    with open("do_work.dag", "w") as fp:
        fp.write(
            DO_WORK_DAG_HEADER.format("MAXJOBS TRANSFER_JOBS 1" if test_mode else "")
        )
        files_to_xfer = list(files_to_xfer)
        files_to_xfer.sort()
        for fname in files_to_xfer:
            idx += 1
            src_file = os.path.join(source_prefix, fname)
            src_file_noslash = fname.replace("/", "_")
            dest = os.path.join(destination_prefix, fname)
            dest_dirs.add(os.path.split(dest)[0])

            logging.info("File transfer to perform: %s->%s", src_file, dest)
            cur_jsonidx = idx / 1000
            if jsonidx != cur_jsonidx:
                # xfer_commands_{fileidx}.json
                with open("xfer_commands_{}.json".format(jsonidx), "w") as cmd_fp:
                    json.dump(cmd_info, cmd_fp)
                jsonidx = cur_jsonidx
                cmd_info = {}
            cmd_info[str(idx)] = {
                "src_file": src_file,
                "src_file_noslash": src_file_noslash,
                "dest": dest,
                "transfer_manifest": transfer_manifest,
                "destination_prefix": destination_prefix,
            }
            fp.write(
                DO_WORK_DAG_XFER_SNIPPET.format(
                    name=idx,
                    fileidx=cur_jsonidx,
                    xfer_py=full_exec_path,
                    src_file=src_file,
                    src_file_noslash=src_file_noslash,
                    dest=dest,
                )
            )

        with open("xfer_commands_{}.json".format(cur_jsonidx), "w") as cmd_fp:
            json.dump(cmd_info, cmd_fp)

        idx = 0
        jsonidx = 0
        cur_jsonidx = 0
        for fname in files_to_verify:
            idx += 1
            src_file = os.path.join(source_prefix, fname)
            src_file_noslash = fname.replace("/", "_")
            dest = os.path.join(destination_prefix, fname)
            logging.info("File to verify: %s", src_file)
            cur_jsonidx = idx / 1000
            if jsonidx != cur_jsonidx:
                with open("verify_commands_{}.json".format(jsonidx), "w") as cmd_fp:
                    json.dump(cmd_info, cmd_fp)
                jsonidx = cur_jsonidx
                cmd_info = {}
            cmd_info[str(idx)] = {
                "src_file": src_file,
                "src_file_noslash": src_file_noslash,
                "dest": dest,
                "transfer_manifest": transfer_manifest,
                "destination_prefix": destination_prefix,
            }
            fp.write(
                DO_WORK_DAG_VERIFY_SNIPPET.format(
                    name=idx,
                    fileidx=cur_jsonidx,
                    xfer_py=full_exec_path,
                    src_file=src_file,
                    src_file_noslash=src_file_noslash,
                    dest=dest,
                )
            )

        with open("verify_commands_{}.json".format(cur_jsonidx), "w") as cmd_fp:
            json.dump(cmd_info, cmd_fp)

    for dest_dir in dest_dirs:
        try:
            os.makedirs(dest_dir)
        except OSError as oe:
            if oe.errno != errno.EEXIST:
                raise

    bytes_to_transfer = sum(src_files[fname] for fname in files_to_xfer)
    bytes_to_verify = sum(src_files[fname] for fname in files_to_verify)
    with open(transfer_manifest, "a") as fp:
        fp.write(
            "SYNC_REQUEST {} files_at_source={} files_to_transfer={} bytes_to_transfer={} files_to_verify={} bytes_to_verify={} timestamp={}\n".format(
                source_prefix,
                len(src_files),
                len(files_to_xfer),
                bytes_to_transfer,
                len(files_to_verify),
                bytes_to_verify,
                time.time(),
            )
        )
        for fname in files_to_xfer:
            if simple_fname(fname):
                fp.write("TRANSFER_REQUEST {} {}\n".format(fname, src_files[fname]))
            else:
                info = {"name": fname, "size": src_files[fname]}
                fp.write("TRANSFER_REQUEST {}\n".format(json.dumps(info)))
        for fname in files_to_verify:
            if simple_fname(fname):
                fp.write("VERIFY_REQUEST {} {}\n".format(fname, src_files[fname]))
            else:
                info = {"name": fname, "size": src_files[fname]}
                fp.write("VERIFY_REQUEST {}\n".format(json.dumps(info)))


if __name__ == "__main__":
    args = parse_args()

    write_subdag(args.src, "source_manifest.txt")
