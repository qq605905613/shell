#!/bin/bash

## function define
# shell syntax
syntax() {
    echo "SYNTAX: $0 [force]"
}

# shell home directory
shell_home_dir() {
    # script name
    SCRIPT_NAME=$1
    if [ ! -f "$SCRIPT_NAME" ] ; then
        echo "[$SCRIPT_NAME] is not exists"
        return 1
    fi

    # script file directory
    SCRIPT_DIR=$(cd $(dirname $SCRIPT_NAME); pwd)

    if [ -h "$SCRIPT_NAME" ] ; then
    # script file is a symbol link
        LINK_NAME=$(readlink "$SCRIPT_NAME");
        if [ ${LINK_NAME:0:1} = "/" ] ; then
            # symbol link is a absolute path
            SCRIPT_PATH=$LINK_NAME
        else
            SCRIPT_PATH="${SCRIPT_DIR}/${LINK_NAME}"
        fi
        SCRIPT_DIR=$(cd $(dirname $SCRIPT_PATH); pwd)
    fi

    # print script home dir
    echo "$SCRIPT_DIR"
}
# shell home directory
SH_HOME=$(shell_home_dir $0)

## source lib
source $SH_HOME/lib/utils.lib
source $SH_HOME/lib/log.lib
source $SH_HOME/lib/db2.lib
## source configuration
source $SH_HOME/etc/config

test -f ${TAB_BACKUP_DIR} && rm ${TAB_BACKUP_DIR}
test ! -d ${TAB_BACKUP_DIR} && mkdir -p ${TAB_BACKUP_DIR}

## export cmd
NOW_TS=$(date +%Y%m%d%H%M%S)

## BACKUP_TAB_LIST
BACKUP_TAB_LIST="mmgm.tbl_mmgm_audit_task mmgm.tbl_mmgm_mchnt_audit_info"
BACKUP_TAB_LIST="${BACKUP_TAB_LIST} mgmhis.tbl_mgmhis_audit_task mgmhis.tbl_mgmhis_mchnt_audit_info"
BACKUP_TAB_LIST="${BACKUP_TAB_LIST} mgmhis.tbl_mgmhis_unaudit_mchnt mgmhis.tbl_mgmhis_audited_mchnt"
BACKUP_TAB_LIST="${BACKUP_TAB_LIST} mgmhis.tbl_mgmhis_reject_mchnt"

## connect to database
RET_MESG="SUCCESS"
connect_to_db $DB_NAME $DB_USER $DB_PASS $@
if [ $? -ne 0 ] ; then
    RET_MESG="[ERROR] connect to database[$DB_NAME] failed!!!"
    logger $RET_MESG
    exit 1
fi

for TAB_NM in ${BACKUP_TAB_LIST}
do
    EXPORT_FILE=${TAB_BACKUP_DIR}/${TAB_NM}.${NOW_TS}.del
    EXPORT_CMD="export to ${EXPORT_FILE} of del select * from ${TAB_NM}"
    exec_db2_cmd "$EXPORT_CMD" $@
    if [ $? -gt 2 ] ; then
        RET_MESG="[ERROR] SQL[$EXPORT_AUDIT_TASK] failed!!!"
        logger $RET_MESG
    fi
    # compress
    [ "$1" = "force" ] && [ -f ${EXPORT_FILE} ] && gzip ${EXPORT_FILE}
done

if [ ! "$RET_MESG" = "SUCCESS" ] ; then
    logger "[ERROR] more than 1 error ocurr when execute sql"
    exit 2
fi
