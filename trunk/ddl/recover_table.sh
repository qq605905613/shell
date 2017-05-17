#!/bin/bash

## TAB_LIST
TAB_LIST="mmgm.tbl_mmgm_audit_task mmgm.tbl_mmgm_mchnt_audit_info"
TAB_LIST="${TAB_LIST} mgmhis.tbl_mgmhis_audit_task mgmhis.tbl_mgmhis_mchnt_audit_info"
TAB_LIST="${TAB_LIST} mgmhis.tbl_mgmhis_unaudit_mchnt mgmhis.tbl_mgmhis_audited_mchnt"
TAB_LIST="${TAB_LIST} mgmhis.tbl_mgmhis_reject_mchnt"

## ARGS
if [ $# -lt 1 ] ; then
    echo "SYNTAX: $0 <export_ts> [force]"
    return 1
fi
EXPORT_TS=$1

shift 1

## task detail
for TAB_NM in ${TAB_LIST} 
do
    DEL_FILE_NM=${TAB_NM}.${EXPORT_TS}.del
    IMPORT_SQL="import from ${DEL_FILE_NM} of del commitcount 50000 replace into ${TAB_NM}"

    echo ${IMPORT_SQL}

    if [ "force" = "$1" ] ; then
        if [ -f "${DEL_FILE_NM}" ] ; then
            db2 "${IMPORT_SQL}"
        else
            echo "[WARN] ${DEL_FILE_NM} do not exists"
        fi
    fi
done
