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

Patch folder:
Apply file operations in a diff file to another folder. In case of conflict, backups will be created.
Diffs are generally assumed to be small, thus files will be actively checksumed if necessary.
The patch will be applied to "patch_on" folder. Actual files will be copied from "data_source".
The umask of copied files can be specified. Patch application results will be printed to stdout.

  fsnapshot.py --apply_patch=<snapshot_json> --patch_on=<folder> --data_source=<folder>
               [--chmod=<0o777>]
'''

from absl import app
from absl import flags
from dataclasses import dataclass
from pathlib import Path
from tqdm import tqdm
from typing import Dict, Optional

import os
import json
import sys
import xxhash
import shutil

FLAGS = flags.FLAGS
flags.DEFINE_string("take_snapshot", None, "")
flags.DEFINE_string("diff_snapshot", None, "")
flags.DEFINE_string("quick_compare", None, "")
flags.DEFINE_string("apply_patch", None, "")

flags.DEFINE_boolean("progress_bar", True, "Display a progress bar when possible")
flags.DEFINE_boolean("escape_unicode_in_json", False, "Escape unicode chars in JSON output as \\uXXXX")
flags.DEFINE_string("testonly_json_time_override", None, "Deterministic timestamp value")
flags.DEFINE_string("snapshot_in", None, "")
flags.DEFINE_string("snapshot_out", None, "")
flags.DEFINE_string("patch_on", None, "")
flags.DEFINE_string("data_source", None, "")
flags.DEFINE_string("chmod", None, "Don't chmod by default. Use 0o777 format for octal numbers.")

flags.mark_flags_as_mutual_exclusive(["take_snapshot", "diff_snapshot", "quick_compare", "apply_patch"], required=True)


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
    new_xxh3: str


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
        vv = v.copy()
        if "old_size" not in vv: vv["old_size"] = 0
        if "new_size" not in vv: vv["new_size"] = 0
        if "old_xxh3" not in vv: vv["old_xxh3"] = ""
        if "new_xxh3" not in vv: vv["new_xxh3"] = ""
        ret[p] = FileChange(path=p, **vv)
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
            if progress_bar is not None:
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
                    if progress_bar is not None:
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


def apply_patch(diff: Dict[str, FileChange], src: Path, dst: Path, chmod: Optional[str]):
    # Two pass: The first pass handles all file->dir changes.
    # The second pass handles others.

    def make_backup(p: Path, suffix='bak') -> str:
        # Renames file foo.txt to foo.txt.bak
        # or foo.txt.bak2 if bak is already used.
        # Returns the relative new path to dst.
        n = p.name
        if len(n.encode()) >= 200:
            n = n[:int(len(n)/2)] + "(omit)"
        newpath = p.with_name(n + "." + suffix)
        i = 2
        while newpath.exists():
            newpath = p.with_name(f"{n}.{suffix}{i}")
            i += 1
        p.rename(newpath)
        return str(newpath.relative_to(dst))
    
    def make_parent_dir(p: Path):
        p.parent.mkdir(parents=True, exist_ok=True)

    # Ensure stable log output order
    ordered_path = sorted(diff.keys(), reverse=True)

    # First pass
    for p in ordered_path:
        fc = diff[p]
        if fc.old_type == "file" and fc.new_type == "dir":
            filep = dst/p
            if not filep.exists():
                print(f"file->dir:not_exists_skip:{p}")
                filep.mkdir(parents=True)
            elif filep.is_dir():
                print(f"file->dir:exists_skip:{p}")
            else:
                real_hash = xxh3_file(filep)
                if fc.old_size == filep.stat().st_size and fc.old_xxh3 == real_hash:
                    filep.unlink()
                    filep.mkdir()
                    print(f"file->dir:ok:{p}")
                else:
                    backup_p = make_backup(filep)
                    filep.mkdir()
                    print(f"file->dir:conflict:{p} ==> {backup_p}")
    

    # Second pass
    for p in ordered_path:
        fc = diff[p]
        srcf = src/p
        dstf = dst/p
        op = (fc.old_type, fc.new_type)

        if op == ("file", "absent"):
            # Remove file
            if not dstf.exists():
                print("file->absent:skip:" + p)
            elif dstf.is_dir():
                newp = make_backup(dstf)
                print(f"file->absent:type_conflict:{p} ==> {newp}")
            else:
                actual_size = dstf.stat().st_size
                actual_hash = xxh3_file(dstf)
                if actual_hash == fc.old_xxh3 and actual_size == fc.old_size:
                    dstf.unlink()
                    print("file->absent:ok:" + p)
                else:
                    newp = make_backup(dstf)
                    print(f"file->absent:content_conflict:{p} ==> {newp}")

        elif op == ("file", "file"):
            # File changed
            if not dstf.exists():
                make_parent_dir(dstf)
                shutil.copy(srcf, dstf)
                print("file->file:ok_added:" + p)
            elif dstf.is_dir():
                newp = make_backup(dstf)
                shutil.copy(srcf, dstf)
                print(f"file->file:type_conflict:{p} ==> {newp}")
            else:
                actual_size = dstf.stat().st_size
                actual_hash = xxh3_file(dstf)
                if actual_hash == fc.old_xxh3 and actual_size == fc.old_size:
                    shutil.copy(srcf, dstf)
                    print("file->file:ok_changed:" + p)
                elif actual_hash == fc.new_xxh3 and actual_size == fc.new_size:
                    print("file->file:ok_unchanged:" + p)
                else:
                    newp = make_backup(dstf)
                    shutil.copy(srcf, dstf)
                    print(f"file->file:content_conflict:{p} ==> {newp}")

        elif op == ("absent", "file"):
            # New file
            if not dstf.exists():
                make_parent_dir(dstf)
                shutil.copy(srcf, dstf)
                print("absent->file:ok:" + p)
            elif dstf.is_dir():
                newp = make_backup(dstf)
                shutil.copy(srcf, dstf)
                print(f"absent->file:type_conflict:{p} ==> {newp}")
            else:
                actual_size = dstf.stat().st_size
                actual_hash = xxh3_file(dstf)
                if actual_hash == fc.new_xxh3 and actual_size == fc.new_size:
                    print("absent->file:ok_unchanged:" + p)
                else:
                    newp = make_backup(dstf)
                    shutil.copy(srcf, dstf)
                    print(f"absent->file:content_conflict:{p} ==> {newp}")

        elif op == ("absent", "dir"):
            # New folder
            if not dstf.exists():
                dstf.mkdir(parents=True)
                print("absent->dir:ok:" + p)
            elif dstf.is_dir():
                print(f"absent->dir:ok_exists:{p}")
            else:
                newp = make_backup(dstf)
                dstf.mkdir(parents=True)
                print(f"absent->dir:conflict:{p} ==> {newp}")

        elif op == ("dir", "absent"):
            # Remove dir
            if not dstf.exists():
                print("dir->absent:ok_skip:" + p)
            elif dstf.is_dir():
                if any(os.scandir(dstf)):
                    newp = make_backup(dstf)
                    print(f"dir->absent:conflict_nonempty:{p} ==> {newp}")
                else:
                    dstf.rmdir()
                    print("dir->absent:ok:" + p)
            else:
                newp = make_backup(dstf)
                print(f"dir->absent:type_conflict:{p} ==> {newp}")

        elif op == ("dir", "file"):
            # Remove dir and put a file
            if not dstf.exists():
                make_parent_dir(dstf)
                shutil.copy(srcf, dstf)
                print("dir->file:ok_added:" + p)
            elif dstf.is_dir():
                if any(os.scandir(dstf)):
                    newp = make_backup(dstf)
                    shutil.copy(srcf, dstf)
                    print(f"dir->file:conflict_nonempty:{p} ==> {newp}")
                else:
                    dstf.rmdir()
                    shutil.copy(srcf, dstf)
                    print("dir->file:ok:" + p)
            else:
                actual_size = dstf.stat().st_size
                actual_hash = xxh3_file(dstf)
                if actual_hash == fc.new_xxh3 and actual_size == fc.new_size:
                    print("dir->file:ok_unchanged:" + p)
                else:
                    newp = make_backup(dstf)
                    shutil.copy(srcf, dstf)
                    print(f"dir->file:content_conflict:{p} ==> {newp}")
        elif op == ("file", "dir"):
            # Already handled in pass one.
            pass
        else:
            # absent->absent or dir->dir
            assert False, f"Invalid op: {op}"
        
    # chmod files
    if chmod:
        val = int(chmod, 0)
        for p in ordered_path:
            os.chmod(dst/p, val)


def quick_scan(d: Path, progress_bar=None) -> Dict[str, Optional[int]]:
    # Collect name and file size in the folder. size==None if it is a folder.
    # Return Dict[relative_path, file_size]
    assert d.is_dir(), str(d) + " is not a folder"
    def _quick_scan(d: Path, root: Path) ->  Dict[str, Optional[int]]:
        ret = dict()
        for infile in d.iterdir():
            relpath = str(infile.relative_to(root)).replace("\\", "/")
            if infile.is_dir():
                ret[relpath] = None
                sub_dir_result = _quick_scan(infile, root)
                ret.update(sub_dir_result)
            elif infile.is_file():
                fsize = infile.stat().st_size
                ret[relpath] = fsize
            if progress_bar is not None:
                progress_bar.update(1)
        return ret
    return _quick_scan(d, d)


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
        if progress_bar is not None:
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
        #assert False, "unimplemented"
        assert FLAGS.snapshot_in is not None
        root = Path(FLAGS.quick_compare)
        with open(FLAGS.snapshot_in, "r") as f:
            json_obj = json.load(f)
            snapshot = snapshot_from_obj(json_obj)

        print("Collecting folder info")
        if FLAGS.progress_bar:
            progress_bar = tqdm()
            scan_result = quick_scan(root, progress_bar)
            progress_bar.close()
            progress_bar = None
        else:
            scan_result = quick_scan(root)

        def same_size(fs: FileSnapshot, size: Optional[int]) -> bool:
            if fs.is_dir:
                return size is None
            else:
                return size is not None and fs.size == size

        extra = [x for x in scan_result if x not in snapshot]
        missing = [x for x in snapshot if x not in scan_result]
        diff = [x for x, v in snapshot.items() if x in scan_result and not same_size(v, scan_result[x])]

        def pretty_print(title, arr):
            print(title)
            if len(arr) == 0:
                print("    Not found.")
            else:
                for x in arr:
                    print("    " + x)

        pretty_print("Extra files:", extra)
        pretty_print("Missing files:", missing)
        pretty_print("Different files:", diff)

    elif FLAGS.apply_patch is not None:
        assert FLAGS.patch_on is not None
        assert FLAGS.data_source is not None
        with open(FLAGS.apply_patch, "r") as f:
            obj = json.load(f)
            snapshot = diff_from_obj(obj)
        apply_patch(snapshot, Path(FLAGS.data_source), Path(FLAGS.patch_on), FLAGS.chmod)

    else:
        assert False, "Missing operation mode"


if __name__ == '__main__':
    app.run(main)
