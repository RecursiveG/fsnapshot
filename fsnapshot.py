#!/usr/bin/env python3

# SPDX-License-Identifier: MIT
# Copyright 2021-2022, Recursive G
# This project is opensourced under the MIT license.
#
'''Folder snapshot tool.

Take snapshot of a folder:
If a snapshot file is given as an input, it will be used as the base and identical files will be skipped.
Files will be skipped if the name and size are the same. Files that are different only in content WILL cause problem.
A progress bar will be displayed on stdout by default, and can be disabled by --noprogress_bar

  fsnapshot.py --take_snapshot=<folder> --snapshot_out=<output_json_file>
               [--snapshot_in=<base_snapshot>] [--noprogress_bar]

Diff snapshots:
Take two snapshot files and compute their diff. The result JSON is printed to stdout.

  fsnapshot.py --diff_snapshot=<first_snapshot> --snapshot_in=<second_snapshot>

Quick compare folder and snapshot:
Compare a folder and a snapshot, then report the different to stdout in human readable format.
Only check file name and size, no content nor time compare.

  fsnapshot.py --quick_compare=<folder> --snapshot_in=<snapshot_json> [--noprogress_bar]
'''

# initial hash: indir -> outfile
# update snapshot: indir, old_snapshot -> new_snapshot, changelist
# copy: src, dst, changelist ->
#   delete if same hash: delete; else skip;
#   add    if not exists: add; elif same hash: skip; else rename then add;
#   overwrite if same hash: overwrite; else rename then add;
# quick_compare: only compare file name and file size.

from absl import app
from absl import flags
from dataclasses import dataclass
from pathlib import Path
from tqdm import tqdm
from typing import Dict

import os
import json
import sys
import xxhash

FLAGS = flags.FLAGS
flags.DEFINE_string("take_snapshot", None, "")
flags.DEFINE_string("diff_snapshot", None, "")
flags.DEFINE_string("quick_compare", None, "")

flags.DEFINE_boolean("progress_bar", True, "Display a progress bar when possible")
flags.DEFINE_boolean("escape_unicode_in_json", False, "Escape unicode chars in JSON output as \\uXXXX")
flags.DEFINE_string("testonly_json_time_override", None, "Deterministic timestamp value")
flags.DEFINE_string("snapshot_in", None, "")
flags.DEFINE_string("snapshot_out", None, "")

flags.mark_flags_as_mutual_exclusive(["take_snapshot", "diff_snapshot", "quick_compare"], required=True)


@dataclass
class FileSnapshot:
    is_dir: bool
    path: str
    size: int
    xxh3: str


@dataclass
class FileChange:
    path: str
    old_type: str    # absent, file, dir
    old_size: int
    old_xxh3: str

    new_type: str
    new_size: int
    new_xxh3: str    # add/overwrite, nullable


def snapshot_to_obj(snap: Dict[str, FileSnapshot]) -> Dict:
    import datetime
    time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    if FLAGS.testonly_json_time_override is not None:
        time = FLAGS.testonly_json_time_override
    inner_d = dict()
    for p, fs in snap.items():
        assert p == fs.path, "bug"
        if fs.is_dir:
            inner_d[p] = dict(is_dir=True)
        else:
            inner_d[p] = dict(is_dir=False, size=fs.size, xxh3=fs.xxh3)
    return dict(time=time, files=inner_d)


def snapshot_from_obj(obj: Dict) -> Dict[str, FileSnapshot]:
    ret = dict()
    for p, v in obj["files"].items():
        if v["is_dir"]:
            ret[p] = FileSnapshot(True, p, 0, "")
        else:
            ret[p] = FileSnapshot(False, p, v["size"], v["xxh3"])
    return ret


def diff_to_obj(diff: Dict[str, FileChange], old_time: str, new_time: str) -> Dict:
    inner_d = dict()
    for p, fc in diff.items():
        assert p == fc.path, "bug"
        inner_d[p] = {k: v for k, v in fc.__dict__.items() if v is not None and k != "path"}
    return dict(old_time=old_time, new_time=new_time, changes=inner_d)


def diff_from_obj(obj: Dict) -> Dict[str, FileChange]:
    ret = dict()
    for p, v in obj["changes"].items():
        ret[p] = FileChange(path=p, **v)
    return ret


def xxh3_file(fp: Path, progress_bar=None) -> str:
    BUF_SIZE = 65536    # lets read stuff in 64kb chunks!
    xxh3 = xxhash.xxh3_64()
    with open(fp, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            xxh3.update(data)
            if progress_bar:
                progress_bar.update(len(data))
    return xxh3.hexdigest()


# Create a snapshot for path `p`. And optional old snapshot may be
# given to speed up the process. If file has the same path and the same size,
# it will be considered the same file and the hash value will be reused.
def take_snapshot(d: Path, progress_bar=None, old_snapshot: Dict[str, FileSnapshot] = None) -> Dict[str, FileSnapshot]:
    if old_snapshot is None:
        old_snapshot = dict()

    def __take_snapshot(d: Path, root: Path) -> Dict[str, FileSnapshot]:
        assert d.is_dir(), str(d) + " is not a folder"
        ret = dict()
        for infile in d.iterdir():
            fpath = str(infile.relative_to(root))
            if infile.is_dir():
                ret[fpath] = FileSnapshot(is_dir=True, path=fpath, size=0, xxh3="")
                sub_dir_result = __take_snapshot(infile, root)
                ret.update(sub_dir_result)

            elif infile.is_file():
                fsize = infile.stat().st_size
                if fpath in old_snapshot and not old_snapshot[fpath].is_dir and fsize == old_snapshot[fpath].size:
                    # same name and same size, assuming file unchanged
                    ret[fpath] = old_snapshot[fpath]
                    # update progress_bar at once
                    if progress_bar:
                        progress_bar.update(fsize)
                else:
                    # recompute, update progress bar in hash function.
                    snap = FileSnapshot(is_dir=False, path=fpath, size=fsize, xxh3=xxh3_file(infile, progress_bar))
                    ret[fpath] = snap
        return ret

    return __take_snapshot(d, d)


def diff_snapshot(old_snapshot: Dict[str, FileSnapshot], new_snapshot: Dict[str,
                                                                            FileSnapshot]) -> Dict[str, FileChange]:
    ret = dict()
    for k, v in old_snapshot.items():
        if k not in new_snapshot:
            if v.is_dir:
                ret[k] = FileChange(k, "dir", None, None, "absent", None, None)
            else:
                ret[k] = FileChange(k, "file", v.size, v.xxh3, "absent", None, None)

    for k, v in new_snapshot.items():
        # new file or folder
        if k not in old_snapshot:
            if v.is_dir:
                ret[k] = FileChange(k, "absent", None, None, "dir", None, None)
            else:
                ret[k] = FileChange(k, "absent", None, None, "file", v.size, v.xxh3)
            continue

        vold = old_snapshot[k]
        if v.is_dir and vold.is_dir:
            # dir unchanged
            pass
        elif not v.is_dir and not vold.is_dir:
            if vold.xxh3 == v.xxh3 and vold.size == v.size:
                # file unchanged
                pass
            else:
                # file changed
                ret[k] = FileChange(k, "file", vold.size, vold.xxh3, "file", v.size, v.xxh3)
        elif vold.is_dir and not v.is_dir:
            # folder changed to file
            ret[k] = FileChange(k, "dir", None, None, "file", v.size, v.xxh3)
        else:
            # file changed to folder
            ret[k] = FileChange(k, "file", vold.size, vold.xxh3, "dir", None, None)
    return ret


# def quick_scan(d: Path, root: Path) -> Dict[str, int]:
#     global progress_bar
#     assert d.is_dir(), str(d) + " is not a folder"
#     ret = dict()
#     try:
#         for infile in d.iterdir():
#             if infile.is_dir():
#                 sub_dir_result = quick_scan(infile, root)
#                 ret.update(sub_dir_result)
#             elif infile.is_file():
#                 fsize = infile.stat().st_size
#                 relpath = infile.relative_to(root)
#                 ret[str(relpath).replace("\\", "/")] = fsize
#                 progress_bar.update(1)
#     except PermissionError:
#         print("quick_scan permission err:", str(d))
#     return ret


def dir_size(path: Path) -> int:
    total = 0
    for entry in os.scandir(path):
        if entry.is_file():
            total += entry.stat().st_size
        elif entry.is_dir():
            total += dir_size(entry.path)
    return total


def main(argv):
    del argv    # Unused.

    if FLAGS.take_snapshot is not None:
        assert FLAGS.snapshot_out is not None
        old_snapshot = None
        progress_bar = None

        root = Path(FLAGS.take_snapshot)
        if FLAGS.snapshot_in is not None:
            with open(FLAGS.snapshot_in, "r") as f:
                old_snapshot = snapshot_from_obj(json.load(f))

        if FLAGS.progress_bar:
            print("Computing folder total size...")
            total_bytes = dir_size(root)
            progress_bar = tqdm(total=total_bytes, unit='B', unit_scale=True)
        snapshot_data = take_snapshot(root, progress_bar, old_snapshot)
        if progress_bar:
            progress_bar.close()
            progress_bar = None

        with open(FLAGS.snapshot_out, "w") as f:
            json.dump(snapshot_to_obj(snapshot_data), f, ensure_ascii=FLAGS.escape_unicode_in_json, indent=2)
        print("Done")

    elif FLAGS.diff_snapshot is not None:
        assert FLAGS.snapshot_in is not None
        with open(FLAGS.diff_snapshot, "r") as f:
            obj = json.load(f)
            snapshot1_time = obj["time"]
            snapshot1 = snapshot_from_obj(obj)
        with open(FLAGS.snapshot_in, "r") as f:
            obj = json.load(f)
            snapshot2_time = obj["time"]
            snapshot2 = snapshot_from_obj(obj)
        diff = diff_snapshot(snapshot1, snapshot2)
        print(
            json.dumps(diff_to_obj(diff, snapshot1_time, snapshot2_time),
                       ensure_ascii=FLAGS.escape_unicode_in_json,
                       indent=2))

    elif FLAGS.quick_compare is not None:
        assert False, "unimplemented"
        # assert FLAGS.snapshot_in is not None
        # root = Path(FLAGS.quick_compare)
        # json_obj = None
        # with open(FLAGS.snapshot_in, "r") as f:
        #     json_obj = json.load(f)
        # snapshot = from_json_friendly_snapshot(json_obj["files"])

        # print("Collecting folder info")
        # progress_bar = tqdm()
        # scan_result = quick_scan(root, root)
        # progress_bar.close()
        # progress_bar = None

        # extra = [x for x in scan_result if x not in snapshot]
        # missing = [x for x in snapshot if x not in scan_result]
        # diff = [x for x, v in snapshot.items() if x in scan_result and v.size != scan_result[x]]

        # def pprint(title, l):
        #     print(title)
        #     if len(l) == 0:
        #         print("    Not found.")
        #     else:
        #         for x in l:
        #             print("    " + x)

        # pprint("Extra files:", extra)
        # pprint("Missing files:", missing)
        # pprint("Different files:", diff)

    else:
        assert False, "Missing operation mode"


if __name__ == '__main__':
    app.run(main)
