#!/bin/bash

## function define
# shell syntax
syntax() {
    echo "SYNTAX: $0 <dir_path> [force]"
}

## arguments process
# args number verify
if [ $# -lt 1 ] ; then
    syntax
    exit 1
fi

TAR_DIR_PATH="$1"
if [ ! -d $TAR_DIR_PATH ] ; then
    echo "[ERROR] $TAR_DIR_PATH is not directory"
    exit 1
fi
TAR_DIR_PATH=$(cd $TAR_DIR_PATH; pwd)
shift 1

## backup tables
./backup_table.sh $@


## do task
for FILE_PATH in $(ls $TAR_DIR_PATH/*.tar.Z)
do
    if [ ! -f $FILE_PATH ] ; then
        echo "[ERROR] [$FILE_PATH] is not exists"
        continue
    fi

    echo "[INFO] [$0] CURRENT FILE: $FILE_PATH"
    ./main.sh $FILE_PATH $@
    if [ $? -ne 0 ] ; then
        echo "[ERROR] [$FILE_PATH] failed"
        break
    fi
done

## shell end
echo "================================================================================"
echo "====          Program is completed, you can close the window.               ===="
echo "================================================================================"
