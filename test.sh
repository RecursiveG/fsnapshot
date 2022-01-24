#!/usr/bin/bash
#source assert.sh

PASS=$'\e[32mPASS\e[0m'
FAIL=$'\e[31mFAIL\e[0m'

function json_eq {
    if cmp --silent <(jq --sort-keys . "$1") <(jq --sort-keys . "$2"); then
        return 0
    else
        return 1
    fi
}

set +e

#if false; then

echo "=== Test 1 : Snapshot one small file"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    echo "hello" > file.txt
    python ../fsnapshot.py --take_snapshot=. --snapshot_out=../testdir.json --testonly_json_time_override=
    if json_eq ../testdir.json ../testdata/test1.json; then
        echo $PASS progress bar
    else
        echo $FAIL progress bar
        exit
    fi

    python ../fsnapshot.py --take_snapshot=. --snapshot_out=../testdir.json --testonly_json_time_override= --noprogress_bar
    if json_eq ../testdir.json ../testdata/test1.json; then
        echo $PASS no progress bar
    else
        echo $FAIL no progress bar
        exit
    fi
popd > /dev/null

echo "=== Test 2 : Snapshot one small file in nested dir"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    mkdir inner
    echo "hello" > inner/file.txt
    python ../fsnapshot.py --take_snapshot=. --snapshot_out=../testdir.json --testonly_json_time_override=
    if json_eq ../testdir.json ../testdata/test2.json; then
        echo $PASS
    else
        echo $FAIL
        exit
    fi
popd > /dev/null

echo "=== Test 3 : Snapshot one huge (10G) file"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    echo "Creating file"
    dd if=/dev/zero of=10g.bin bs=1M count=10000
    python ../fsnapshot.py --take_snapshot=. --snapshot_out=../testdir.json --testonly_json_time_override=
    if json_eq ../testdir.json ../testdata/test3.json; then
        echo $PASS
    else
        echo $FAIL
        exit
    fi
popd > /dev/null

echo "=== Test 4 : Snapshot complex folder structure"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    touch file1
    mkdir dir1
    mkdir dir2 && touch dir2/file2
    mkdir dir2/dir3
    touch dir2/dir3/file3
    python ../fsnapshot.py --take_snapshot=. --snapshot_out=../testdir.json --testonly_json_time_override=
    if json_eq ../testdir.json ../testdata/test4.json; then
        echo $PASS
    else
        echo $FAIL
        exit
    fi
popd > /dev/null


echo "=== Test 5 : Update skips known file test"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    echo "Creating file"
    dd if=/dev/zero of=10g.bin bs=1M count=10000
    python ../fsnapshot.py --take_snapshot=. --snapshot_out=../testdir.json --testonly_json_time_override=
    touch 10g.bin
    mkdir folder1
    echo hello > folder1/file.txt
    python ../fsnapshot.py --take_snapshot=. --snapshot_in=../testdir.json --snapshot_out=../testdir.json --testonly_json_time_override=

    if json_eq ../testdir.json ../testdata/test5.json; then
        echo $PASS
    else
        echo $FAIL
        exit
    fi
popd > /dev/null

#fi

echo "=== Test 6 : Diff test"
rm -rf testdir || true
mkdir testdir
pushd testdir > /dev/null
    # remove dir1, add dir2, change dir3, tofile dir4, todir dir5
    mkdir dir1 && touch dir1/file1
    mkdir dir3 && touch dir3/file3
    mkdir dir4 && touch dir4/file4
    touch dir5
    python ../fsnapshot.py --take_snapshot=. --snapshot_out=../testdir1.json --testonly_json_time_override=1 --noprogress_bar
    rm -r dir1
    mkdir dir2 && touch dir2/file2
    echo hello > dir3/file3
    rm -r dir4 && touch dir4
    rm dir5 && mkdir dir5 && touch dir5/file5
    python ../fsnapshot.py --take_snapshot=. --snapshot_out=../testdir2.json --testonly_json_time_override=2 --noprogress_bar
    python ../fsnapshot.py --diff_snapshot=../testdir1.json --snapshot_in=../testdir2.json > ../testdir_diff.json
    if json_eq ../testdir_diff.json ../testdata/test6.json; then
        echo $PASS
    else
        echo $FAIL
        exit
    fi
popd > /dev/null
