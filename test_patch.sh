#!/usr/bin/bash

PASS=$'\e[32mPASS\e[0m'
FAIL=$'\e[31mFAIL\e[0m'

function json_eq {
    if cmp --silent <(jq --sort-keys . "$1") <(jq --sort-keys . "$2"); then
        return 0
    else
        return 1
    fi
}

function dir_eq {
    if diff -r "$1" "$2" >& /dev/null; then
        return 0
    else
        return 1
    fi
}

set +e

echo "=== Test 1 : add file"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    # make src
    mkdir src
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=before.json --noprogress_bar --testonly_json_time_override=
    # modify src
    echo a > src/a.txt
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=after.json --noprogress_bar --testonly_json_time_override=
    # make dst
    mkdir dst
    echo b > dst/b.txt
    # make expected
    mkdir expected
    echo a > expected/a.txt
    echo b > expected/b.txt
    # do patch
    python ../fsnapshot.py --diff_snapshot=before.json --snapshot_in=after.json > diff.json
    python ../fsnapshot.py --apply_patch=diff.json --patch_on=dst --data_source=src > patch.log
    if dir_eq dst expected; then
        echo $PASS: content
    else
        echo $FAIL: content
        exit
    fi
    if [[ "$(cat patch.log)" == $'absent->file:ok:a.txt' ]]; then
        echo $PASS: log
    else
        echo $FAIL: log
        exit
    fi
popd > /dev/null

echo "=== Test 2 : add file in folder"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    # make src
    mkdir src
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=before.json --noprogress_bar --testonly_json_time_override=
    # modify src
    mkdir src/inner
    echo a > src/inner/a.txt
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=after.json --noprogress_bar --testonly_json_time_override=
    # make dst
    mkdir dst
    echo b > dst/b.txt
    # make expected
    mkdir -p expected/inner
    echo a > expected/inner/a.txt
    echo b > expected/b.txt
    # do patch
    python ../fsnapshot.py --diff_snapshot=before.json --snapshot_in=after.json > diff.json
    python ../fsnapshot.py --apply_patch=diff.json --patch_on=dst --data_source=src > patch.log
    if dir_eq dst expected; then
        echo $PASS: content
    else
        echo $FAIL: content
        exit
    fi
    if [[ "$(cat patch.log)" == $'absent->file:ok:inner/a.txt\nabsent->dir:ok_exists:inner' ]]; then
        echo $PASS: log
    else
        echo $FAIL: log
        exit
    fi
popd > /dev/null

echo "=== Test 3 : add file when exists"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    # make src
    mkdir src
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=before.json --noprogress_bar --testonly_json_time_override=
    # modify src
    echo a > src/a.txt
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=after.json --noprogress_bar --testonly_json_time_override=
    # make dst
    mkdir dst
    echo a > dst/a.txt
    # make expected
    mkdir expected
    echo a > expected/a.txt
    # do patch
    python ../fsnapshot.py --diff_snapshot=before.json --snapshot_in=after.json > diff.json
    python ../fsnapshot.py --apply_patch=diff.json --patch_on=dst --data_source=src > patch.log
    if dir_eq dst expected; then
        echo $PASS: content
    else
        echo $FAIL: content
        exit
    fi
    if [[ "$(cat patch.log)" == $'absent->file:ok_unchanged:a.txt' ]]; then
        echo $PASS: log
    else
        echo $FAIL: log
        exit
    fi
popd > /dev/null

echo "=== Test 4 : add file conflict file"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    # make src
    mkdir src
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=before.json --noprogress_bar --testonly_json_time_override=
    # modify src
    echo a > src/a.txt
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=after.json --noprogress_bar --testonly_json_time_override=
    # make dst
    mkdir dst
    echo conflict > dst/a.txt
    echo placeholder > dst/a.txt.bak
    # make expected
    mkdir expected
    echo a > expected/a.txt
    echo placeholder > expected/a.txt.bak
    echo conflict > expected/a.txt.bak2
    # do patch
    python ../fsnapshot.py --diff_snapshot=before.json --snapshot_in=after.json > diff.json
    python ../fsnapshot.py --apply_patch=diff.json --patch_on=dst --data_source=src > patch.log
    if dir_eq dst expected; then
        echo $PASS: content
    else
        echo $FAIL: content
        exit
    fi
    if [[ "$(cat patch.log)" == $'absent->file:content_conflict:a.txt ==> a.txt.bak2' ]]; then
        echo $PASS: log
    else
        echo $FAIL: log
        exit
    fi
popd > /dev/null

echo "=== Test 5 : add file conflict dir"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    # make src
    mkdir src
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=before.json --noprogress_bar --testonly_json_time_override=
    # modify src
    echo a > src/a.txt
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=after.json --noprogress_bar --testonly_json_time_override=
    # make dst
    mkdir -p dst/a.txt
    echo foo > dst/a.txt/foo.txt
    # make expected
    mkdir -p expected/a.txt.bak
    echo a > expected/a.txt
    echo foo > expected/a.txt.bak/foo.txt
    # do patch
    python ../fsnapshot.py --diff_snapshot=before.json --snapshot_in=after.json > diff.json
    python ../fsnapshot.py --apply_patch=diff.json --patch_on=dst --data_source=src > patch.log
    if dir_eq dst expected; then
        echo $PASS: content
    else
        echo $FAIL: content
        exit
    fi
    if [[ "$(cat patch.log)" == $'absent->file:type_conflict:a.txt ==> a.txt.bak' ]]; then
        echo $PASS: log
    else
        echo $FAIL: log
        exit
    fi
popd > /dev/null

echo "=== Test 6 : remove file w/o dir"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    # make src
    mkdir -p src/foo
    echo a > src/foo/a.txt
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=before.json --noprogress_bar --testonly_json_time_override=
    # modify src
    rm src/foo/a.txt
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=after.json --noprogress_bar --testonly_json_time_override=
    # make dst
    mkdir -p dst/foo
    echo a > dst/foo/a.txt
    echo b > dst/b.txt
    # make expected
    mkdir -p expected/foo
    echo b > expected/b.txt
    # do patch
    python ../fsnapshot.py --diff_snapshot=before.json --snapshot_in=after.json > diff.json
    python ../fsnapshot.py --apply_patch=diff.json --patch_on=dst --data_source=src > patch.log
    if dir_eq dst expected; then
        echo $PASS: content
    else
        echo $FAIL: content
        exit
    fi
    if [[ "$(cat patch.log)" == $'file->absent:ok:foo/a.txt' ]]; then
        echo $PASS: log
    else
        echo $FAIL: log
        exit
    fi
popd > /dev/null

echo "=== Test 7 : remove file w/ dir"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    # make src
    mkdir -p src/foo/bar
    echo a > src/foo/bar/a.txt
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=before.json --noprogress_bar --testonly_json_time_override=
    # modify src
    rm -r src/foo/bar
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=after.json --noprogress_bar --testonly_json_time_override=
    # make dst
    mkdir -p dst/foo/bar
    echo a > dst/foo/bar/a.txt
    echo b > dst/b.txt
    # make expected
    mkdir -p expected/foo
    echo b > expected/b.txt
    # do patch
    python ../fsnapshot.py --diff_snapshot=before.json --snapshot_in=after.json > diff.json
    python ../fsnapshot.py --apply_patch=diff.json --patch_on=dst --data_source=src > patch.log
    if dir_eq dst expected; then
        echo $PASS: content
    else
        echo $FAIL: content
        exit
    fi
    if [[ "$(cat patch.log)" == $'file->absent:ok:foo/bar/a.txt\ndir->absent:ok:foo/bar' ]]; then
        echo $PASS: log
    else
        echo $FAIL: log
        exit
    fi
popd > /dev/null

echo "=== Test 8 : remove file w/ dir w/ conflict"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    # make src
    mkdir -p src/foo
    echo a > src/foo/a.txt
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=before.json --noprogress_bar --testonly_json_time_override=
    # modify src
    rm -r src/foo
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=after.json --noprogress_bar --testonly_json_time_override=
    # make dst
    mkdir -p dst/foo
    echo conflict > dst/foo/a.txt
    echo b > dst/b.txt
    # make expected
    mkdir -p expected/foo.bak
    echo conflict > expected/foo.bak/a.txt.bak
    echo b > expected/b.txt
    # do patch
    python ../fsnapshot.py --diff_snapshot=before.json --snapshot_in=after.json > diff.json
    python ../fsnapshot.py --apply_patch=diff.json --patch_on=dst --data_source=src > patch.log
    if dir_eq dst expected; then
        echo $PASS: content
    else
        echo $FAIL: content
        exit
    fi
    if [[ "$(cat patch.log)" == $'file->absent:content_conflict:foo/a.txt ==> foo/a.txt.bak\ndir->absent:conflict_nonempty:foo ==> foo.bak' ]]; then
        echo $PASS: log
    else
        echo $FAIL: log
        exit
    fi
popd > /dev/null

echo "=== Test 9 : modify file"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    # make src
    mkdir -p src
    echo a > src/a.txt
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=before.json --noprogress_bar --testonly_json_time_override=
    # modify src
    echo modified > src/a.txt
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=after.json --noprogress_bar --testonly_json_time_override=
    # make dst
    mkdir -p dst
    echo a > dst/a.txt
    echo b > dst/b.txt
    # make expected
    mkdir -p expected
    echo modified > expected/a.txt
    echo b > expected/b.txt
    # do patch
    python ../fsnapshot.py --diff_snapshot=before.json --snapshot_in=after.json > diff.json
    python ../fsnapshot.py --apply_patch=diff.json --patch_on=dst --data_source=src > patch.log
    if dir_eq dst expected; then
        echo $PASS: content
    else
        echo $FAIL: content
        exit
    fi
    if [[ "$(cat patch.log)" == $'file->file:ok_changed:a.txt' ]]; then
        echo $PASS: log
    else
        echo $FAIL: log
        exit
    fi
popd > /dev/null

echo "=== Test 10 : modify file w/ conflict"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    # make src
    mkdir -p src
    echo a > src/a.txt
    echo b > src/b.txt
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=before.json --noprogress_bar --testonly_json_time_override=
    # modify src
    echo modified > src/a.txt
    echo modified_b > src/b.txt
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=after.json --noprogress_bar --testonly_json_time_override=
    # make dst
    mkdir -p dst
    echo conflict > dst/a.txt
    # make expected
    mkdir -p expected
    echo modified > expected/a.txt
    echo conflict > expected/a.txt.bak
    echo modified_b > expected/b.txt
    # do patch
    python ../fsnapshot.py --diff_snapshot=before.json --snapshot_in=after.json > diff.json
    python ../fsnapshot.py --apply_patch=diff.json --patch_on=dst --data_source=src > patch.log
    if dir_eq dst expected; then
        echo $PASS: content
    else
        echo $FAIL: content
        exit
    fi
    if [[ "$(cat patch.log)" == $'file->file:ok_added:b.txt\nfile->file:content_conflict:a.txt ==> a.txt.bak' ]]; then
        echo $PASS: log
    else
        echo $FAIL: log
        exit
    fi
popd > /dev/null

echo "=== Test 11 : complex dir struc change"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    # make src
    mkdir -p src
    echo f1 > src/f1
    mkdir src/f2 && echo ff2 > src/f2/ff2
    echo f3 > src/f3
    mkdir src/f6
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=before.json --noprogress_bar --testonly_json_time_override=
    # make dst
    cp -r src dst

    # modify src
    mv src/f3 src/f4
    mv src/f2 src/f3
    cp src/f1 src/f2
    echo f1_modified > src/f1
    mkdir src/f5
    rm -r src/f6
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=after.json --noprogress_bar --testonly_json_time_override=
    # make expected
    cp -r src expected
    
    # do patch
    python ../fsnapshot.py --diff_snapshot=before.json --snapshot_in=after.json > diff.json
    python ../fsnapshot.py --apply_patch=diff.json --patch_on=dst --data_source=src > patch.log
    if dir_eq dst expected; then
        echo $PASS: content
    else
        echo $FAIL: content
        exit
    fi
    expected_log='file->dir:ok:f3
dir->absent:ok:f6
absent->dir:ok:f5
absent->file:ok:f4
absent->file:ok:f3/ff2
file->absent:ok:f2/ff2
dir->file:ok:f2
file->file:ok_changed:f1'
    if [[ "$(cat patch.log)" == "$expected_log" ]]; then
        echo $PASS: log
    else
        echo $FAIL: log
        exit
    fi
popd > /dev/null

echo "=== Test 12 : long file name"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    # utf8 85 * 3 = 255 bytes, plus NULL terminated
    long_name=$(perl -E 'say "啊" x 85')
    short_name=$(perl -E 'say "啊" x 42')
    # make src
    mkdir -p src
    echo a > src/$long_name
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=before.json --noprogress_bar --testonly_json_time_override=
    # modify src
    echo modified > src/$long_name
    python ../fsnapshot.py --take_snapshot=src --snapshot_out=after.json --noprogress_bar --testonly_json_time_override=
    # make dst
    mkdir -p dst
    echo conflict > dst/$long_name
    # make expected
    mkdir -p expected
    echo modified > expected/${long_name}
    echo conflict > expected/${short_name}"(omit).bak"
    # do patch
    python ../fsnapshot.py --diff_snapshot=before.json --snapshot_in=after.json > diff.json
    python ../fsnapshot.py --apply_patch=diff.json --patch_on=dst --data_source=src > patch.log
    if dir_eq dst expected; then
        echo $PASS: content
    else
        echo $FAIL: content
        exit
    fi
    if [[ "$(cat patch.log)" == "file->file:content_conflict:${long_name} ==> ${short_name}(omit).bak" ]]; then
        echo $PASS: log
    else
        echo $FAIL: log
        exit
    fi
popd > /dev/null

rm -rf testdir
