#!/usr/bin/env python3

# SPDX-License-Identifier: MIT
# Copyright 2021, Recursive G
# This project is opensourced under the MIT license.

from absl import app
from absl import flags
from dataclasses import dataclass
from pathlib import Path
from tqdm import tqdm
from typing import Dict

import os
import json
import datetime
import sys
import xxhash


@dataclass
class FileSnapshot:
    path: str
    size: int
    xxh3: str


@dataclass
class FileChange:
    path: str
    change_type: str    # add, delete, overwrite
    old_xxh3: str    # delete/overwrite, nullable
    new_xxh3: str    # add/overwrite, nullable
    old_size: int
    new_size: int


# initial hash: indir -> outfile
# update snapshot: indir, old_snapshot -> new_snapshot, changelist
# copy: src, dst, changelist ->
#   delete if same hash: delete; else skip;
#   add    if not exists: add; elif same hash: skip; else rename then add;
#   overwrite if same hash: overwrite; else rename then add;

# diff: currentdir, snapshotfile
# copy: srcdir dstdir snapshotfile

FLAGS = flags.FLAGS
flags.DEFINE_string("take_snapshot", None, "Folder to take snapshot of. Writes to snapshot_out.")
flags.DEFINE_string("update_snapshot", None,
                    "Folder used to update a snapshot. this+snapshot_in -> snapshot_out+changelist_out")
flags.DEFINE_string("copy_from", None, "Requires copy_to & changelist_in")
flags.DEFINE_string("quick_compare", None, "Compare this folder with snapshot_in")
flags.DEFINE_string("copy_to", None, "")
flags.DEFINE_string("snapshot_in", None, "")
flags.DEFINE_string("snapshot_out", None, "The snapshot file to write into")
flags.DEFINE_string("changelist_in", None, "")
flags.DEFINE_string("changelist_out", None, "")

progress_bar = None


def xxh3_file(fp: Path) -> str:
    BUF_SIZE = 65536    # lets read stuff in 64kb chunks!
    xxh3 = xxhash.xxh3_64()
    with open(fp, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            xxh3.update(data)
    return xxh3.hexdigest()


def snapshot_dir(d: Path, root: Path) -> Dict:
    global progress_bar
    assert d.is_dir(), str(d) + " is not a folder"
    ret = dict()
    try:
        for infile in d.iterdir():
            if infile.is_dir():
                sub_dir_result = snapshot_dir(infile, root)
                ret.update(sub_dir_result)
            elif infile.is_file():
                snap = FileSnapshot(path=str(infile.relative_to(root)),
                                    size=infile.stat().st_size,
                                    xxh3=xxh3_file(infile))
                ret[snap.path] = snap

                progress_bar.update(snap.size)
    except PermissionError:
        print("snapshot_dir permission err:", str(d))

    return ret


def update_snapshot(d: Path, root: Path, old_snapshot: Dict[str, FileSnapshot]) -> Dict:
    global progress_bar
    assert d.is_dir(), str(d) + " is not a folder"
    ret = dict()
    try:
        for infile in d.iterdir():
            if infile.is_dir():
                sub_dir_result = update_snapshot(infile, root, old_snapshot)
                ret.update(sub_dir_result)
            elif infile.is_file():
                fpath = str(infile.relative_to(root))
                fsize = infile.stat().st_size
                if fpath in old_snapshot and fsize == old_snapshot[fpath].size:
                    # same name and same size, assuming file unchanged
                    ret[fpath] = old_snapshot[fpath]
                else:
                    # recompute
                    snap = FileSnapshot(path=fpath, size=fsize, xxh3=xxh3_file(infile))
                    ret[fpath] = snap
                progress_bar.update(fsize)
    except PermissionError:
        print("update_snapshot permission err:", str(d))
    return ret


def diff_snapshot(old_snapshot: Dict[str, FileSnapshot], new_snapshot: Dict[str,
                                                                            FileSnapshot]) -> Dict[str, FileChange]:
    ret = dict()
    for k, v in old_snapshot.items():
        if k not in new_snapshot:
            ret[k] = FileChange(path=k,
                                change_type="delete",
                                old_xxh3=v.xxh3,
                                new_xxh3=None,
                                old_size=v.size,
                                new_size=None)
    for k, v in new_snapshot.items():
        if k not in old_snapshot:
            ret[k] = FileChange(path=k,
                                change_type="add",
                                old_xxh3=None,
                                new_xxh3=v.xxh3,
                                old_size=None,
                                new_size=v.size)
            continue

        vold = old_snapshot[k]
        if vold.xxh3 == v.xxh3 and vold.size == v.size:
            continue
        ret[k] = FileChange(path=k,
                            change_type="overwrite",
                            old_xxh3=vold.xxh3,
                            new_xxh3=v.xxh3,
                            old_size=vold.size,
                            new_size=v.size)
    return ret


def quick_scan(d: Path, root: Path) -> Dict[str, int]:
    global progress_bar
    assert d.is_dir(), str(d) + " is not a folder"
    ret = dict()
    try:
        for infile in d.iterdir():
            if infile.is_dir():
                sub_dir_result = quick_scan(infile, root)
                ret.update(sub_dir_result)
            elif infile.is_file():
                fsize = infile.stat().st_size
                relpath = infile.relative_to(root)
                ret[str(relpath).replace("\\", "/")] = fsize
                progress_bar.update(1)
    except PermissionError:
        print("quick_scan permission err:", str(d))
    return ret


def dir_size(path: Path) -> int:
    total = 0
    try:
        for entry in os.scandir(path):
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += dir_size(entry.path)
    except PermissionError:
        print("dir_size Permission error:", str(path))
    return total


def to_json_friendly(m: Dict):
    return {k: v.__dict__ for k, v in m.items()}


def from_json_friendly_snapshot(d) -> Dict[str, FileSnapshot]:
    return {k: FileSnapshot(**v) for k, v in d.items()}


def time_str() -> str:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()


def main(argv):
    global progress_bar
    del argv    # Unused.

    if FLAGS.take_snapshot is not None:
        assert FLAGS.snapshot_out is not None
        root = Path(FLAGS.take_snapshot)

        print("Computing folder total size...")
        total_bytes = dir_size(root)
        progress_bar = tqdm(total=total_bytes, unit='B', unit_scale=True)
        snapshot_data = snapshot_dir(root, root)
        progress_bar.close()
        progress_bar = None

        with open(FLAGS.snapshot_out, "w") as f:
            json_obj = dict(time=time_str(), files=to_json_friendly(snapshot_data))
            json.dump(json_obj, f, ensure_ascii=False, indent=4)
        print("Done")
    elif FLAGS.update_snapshot is not None:
        assert FLAGS.snapshot_in is not None
        assert FLAGS.snapshot_out is not None
        assert FLAGS.changelist_out is not None
        root = Path(FLAGS.update_snapshot)
        old_json_obj = None
        with open(FLAGS.snapshot_in, "r") as f:
            old_json_obj = json.load(f)
        old_snapshot = from_json_friendly_snapshot(old_json_obj["files"])

        print("Computing folder total size...")
        total_bytes = dir_size(root)
        progress_bar = tqdm(total=total_bytes, unit='B', unit_scale=True)
        snapshot_data = update_snapshot(root, root, old_snapshot)
        progress_bar.close()
        progress_bar = None

        print("Compute difference...")
        changelist = diff_snapshot(old_snapshot, snapshot_data)

        new_time = time_str()
        with open(FLAGS.snapshot_out, "w") as f:
            json_obj = dict(time=new_time, files=to_json_friendly(snapshot_data))
            json.dump(json_obj, f, ensure_ascii=False, indent=4)

        with open(FLAGS.changelist_out, "w") as f:
            json_obj = dict(old_time=old_json_obj["time"], new_time=new_time, changes=to_json_friendly(changelist))
            json.dump(json_obj, f, ensure_ascii=False, indent=4)
    elif FLAGS.copy_from is not None:
        assert FLAGS.copy_to is not None
        assert FLAGS.changelist_in is not None
        assert False, "not implemented"
    elif FLAGS.quick_compare is not None:
        assert FLAGS.snapshot_in is not None
        root = Path(FLAGS.quick_compare)
        json_obj = None
        with open(FLAGS.snapshot_in, "r") as f:
            json_obj = json.load(f)
        snapshot = from_json_friendly_snapshot(json_obj["files"])

        print("Collecting folder info")
        progress_bar = tqdm()
        scan_result = quick_scan(root, root)
        progress_bar.close()
        progress_bar = None

        extra = [x for x in scan_result if x not in snapshot]
        missing = [x for x in snapshot if x not in scan_result]
        diff = [x for x, v in snapshot.items() if x in scan_result and v.size != scan_result[x]]

        def pprint(title, l):
            print(title)
            if len(l) == 0:
                print("    Not found.")
            else:
                for x in l:
                    print("    " + x)

        pprint("Extra files:", extra)
        pprint("Missing files:", missing)
        pprint("Different files:", diff)

    else:
        assert False, "Missing operation mode"


if __name__ == '__main__':
    app.run(main)
