#!/bin/bash

## function define
# shell syntax
syntax() {
    echo "SYNTAX: $0 <csv_file> [force]"
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

## agruments verify
if [ $# -lt 1 ] ; then
    syntax
    exit 1
fi

## process arguments
CSV_FILE=$1
CSV_FILE_PATH=$(cd $(dirname $CSV_FILE); pwd)/$(basename $CSV_FILE)
if [ ! -f "${CSV_FILE_PATH}" ] ; then
    logger "[ERROR] [${CSV_FILE_PATH}] is not exists"
    exit 1
fi
logger "[INFO] CURRENT PROCESS FILE is [${CSV_FILE_PATH}]"

## csv file export time
CSV_FILE_NM=$(basename ${CSV_FILE})
EXPORT_TS=$(date +"%Y%m%d%H%M%S" -d@${CSV_FILE_NM:9:10})
if [ $? -ne 0 ] ; then
    logger "[ERROR] export timestamp parse failed[${CSV_FILE_NM}]"
    exit 2
fi
# completed timestamp
COMP_TS_FILE_PATH=$SH_HOME/dat/completed.ts
if [ ! -f "$COMP_TS_FILE_PATH" ] ; then
    touch $COMP_TS_FILE_PATH
fi
COMPLETED_TS=$(cat $COMP_TS_FILE_PATH)
# export_ts <= completed_ts
if [ "$(echo -e "$EXPORT_TS\n$COMPLETED_TS" | sort | head -n1)" = "$EXPORT_TS" ] ; then
    logger "[INFO] ${CSV_FILE_PATH} is processed"
    exit 0
fi


## delete $1
shift 1

## one instance
LOCK_DIR=$SH_HOME/tmp/main.lock
# trap exit
# return message
RET_MESG=SUCCESS
trap_exit() {
    # 释放数据库连接
    db2 terminate
    if [ "$IS_DEBUG" = "TRUE" ] ; then
        TMP_BAK_DIR=$SH_HOME/tmp/$EXPORT_TS
        mkdir -p ${TMP_BAK_DIR}
        cp ${LOCK_DIR}/*.sql ${TMP_BAK_DIR}
        cp ${LOCK_DIR}/*.sed ${TMP_BAK_DIR}
    fi
    rm -rf ${LOCK_DIR}
    #$SH_HOME/bin/send_mail liming@umail.com "IMPORT_AC_FLOWD[DATE:$Z_DATE;RET:${RET_MESG}]"
}
# assurce only one instance is running
if [ "$1" == "force" ] ; then
    mkdir $LOCK_DIR
    if [ $? -ne 0 ] ; then
        logger "ERROR: Other instance is running"
        exit 3
    fi
#    trap "rm -rf ${LOCK_DIR}; exit" 0 1 2 3 9 15
    trap "trap_exit; exit" 0 1 2 3 9 15
fi


## uncompress file
if [ "$1" == "force" ] ; then
    tar -xzvf $CSV_FILE_PATH -C $SH_HOME/tmp/main.lock
    if [ $? -ne 0 ] ; then
        logger "[ERROR] uncompress file failed[$CSV_FILE_PATH]"
        exit 4
    fi
fi
UNAUDIT_CSV_FILE=$SH_HOME/tmp/main.lock/${CSV_FILE_NM:0:23}unauditMchnt.csv
AUDITED_CSV_FILE=$SH_HOME/tmp/main.lock/${CSV_FILE_NM:0:23}auditedMchnt.csv
REJECT_CSV_FILE=$SH_HOME/tmp/main.lock/${CSV_FILE_NM:0:23}unpassMchnt.csv

## format csv
# 设置默认编码为批量文件编码GBK
export LANG=GBK
UNAUDIT_DEL=$SH_HOME/tmp/main.lock/unauditMchnt.del
AUDITED_DEL=$SH_HOME/tmp/main.lock/auditedMchnt.del
REJECT_DEL=$SH_HOME/tmp/main.lock/unpassMchnt.del
if [ -f "$UNAUDIT_CSV_FILE" ] ; then
    awk -f $SH_HOME/script/unaudit_format.awk $UNAUDIT_CSV_FILE > $UNAUDIT_DEL
else
    logger "[WARN] $UNAUDIT_CSV_FILE is not exists"
fi
if [ -f "$AUDITED_CSV_FILE" ] ; then
    awk -f $SH_HOME/script/audited_format.awk $AUDITED_CSV_FILE > $AUDITED_DEL
else
    logger "[WARN] $AUDITED_CSV_FILE is not exists"
fi
if [ -f "$REJECT_CSV_FILE" ] ; then
    sed -n -f $SH_HOME/script/del_rn.sed $REJECT_CSV_FILE | awk -f $SH_HOME/script/reject_format.awk > $REJECT_DEL
else
    touch $REJECT_DEL
    logger "[WARN] $REJECT_CSV_FILE is not exists"
fi
# 还原默认编码设置
export LANG=zh_CN.UTF-8

## import sql
IMPORT_UNAUDIT_SQL="import from ${UNAUDIT_DEL} of del modified by usedefaults commitcount 50000 replace into mmgm.tbl_mmgm_unaudit_mchnt(MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL,IS_WHITE_MCHNT,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR,AUDIT_ST,BUSS_TP,PROD_FUNC,D_ALLOT_CD,C_ALLOT_CD,SINGLE_AT_LIMIT,SINGLE_CARD_DAY_AT_LIMIT)"
IMPORT_AUDITED_SQL="import from ${AUDITED_DEL} of del modified by usedefaults commitcount 50000 replace into mmgm.tbl_mmgm_audited_mchnt(MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL,IS_WHITE_MCHNT,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR,CUP_2ND_AUDITED_TS,CUP_2ND_AUDITOR,BUSS_TP,PROD_FUNC,D_ALLOT_CD,C_ALLOT_CD,SINGLE_AT_LIMIT,SINGLE_CARD_DAY_AT_LIMIT)"
IMPORT_REJECT_SQL="import from ${REJECT_DEL} of del modified by usedefaults commitcount 50000 replace into mmgm.tbl_mmgm_reject_mchnt(MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL,IS_WHITE_MCHNT,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR,CUP_2ND_AUDITED_TS,CUP_2ND_AUDITOR,ACQ_AUDIT_MEMO,CUP_BRANCH_MEMO,CUP_HEAD_MEMO,BUSS_TP,PROD_FUNC,D_ALLOT_CD,C_ALLOT_CD,SINGLE_AT_LIMIT,SINGLE_CARD_DAY_AT_LIMIT)"

## connect to database
connect_to_db $DB_NAME $DB_USER $DB_PASS $@
if [ $? -ne 0 ] ; then
    RET_MESG="[ERROR] connect to database[$DB_NAME] failed!!!"
    logger $RET_MESG
    exit 5
fi

## exec import unaudit
if [ ! -f "$UNAUDIT_DEL" ] ; then
    logger "[WARN] $UNAUDIT_DEL is not exists"
fi
exec_db2_cmd "$IMPORT_UNAUDIT_SQL" $@
RET=$?
if [ $RET -ne 0 ] ; then
    RET_MESG="[ERROR:$RET] SQL[$IMPORT_REJECT_SQL] failed!!!"
    logger $RET_MESG
    exit 6
fi

## exec import audited
if [ ! -f "$AUDITED_DEL" ] ; then
    logger "[WARN] $AUDITED_DEL is not exists"
fi
exec_db2_cmd "$IMPORT_AUDITED_SQL" $@
RET=$?
if [ $RET -ne 0 ] ; then
    RET_MESG="[ERROR:$RET] SQL[$IMPORT_REJECT_SQL] failed!!!"
    logger $RET_MESG
    exit 6
fi

## exec import reject
if [ ! -f "$REJECT_DEL" ] ; then
    logger "[WARN] $REJECT_DEL is not exists"
fi
exec_db2_cmd "$IMPORT_REJECT_SQL" $@
RET=$?
if [ $RET -ne 0 ] ; then
    RET_MESG="[ERROR:$RET] SQL[$IMPORT_REJECT_SQL] failed!!!"
    logger $RET_MESG
    exit 6
fi

## 0.0 data clean: tbl_mmgm_audit_task, tbl_mmgm_task_assign
SQL_FILE="$SH_HOME/sql/s0_0_data_clean.sql"
exec_sql_script "$SQL_FILE" $@
if [ $? -gt 2 ] ; then
    RET_MESG="[ERROR] SQL[$SQL_FILE] failed!!!"
    logger $RET_MESG
    exit 7
fi
## 0.1 preporc reject mchnt
TMP_SED_SCRIPT="$LOCK_DIR/s0_1_var_replace.sed"
TMP_SQL_FILE="$LOCK_DIR/s0_1_preproc.sql"
echo 's/\${\(EXPORT_TS\)}/'${EXPORT_TS}/g > $TMP_SED_SCRIPT
sed -f $TMP_SED_SCRIPT $SH_HOME/sql/s0_1_preproc_with_template.sql > $TMP_SQL_FILE
exec_sql_script "$TMP_SQL_FILE" $@
if [ $? -gt 2 ] ; then
    RET_MESG="[ERROR] SQL[$TMP_SQL_FILE] failed!!!"
    logger $RET_MESG
    exit 7
fi

## 1. update mchnt audit info
TMP_SED_SCRIPT="$LOCK_DIR/s1_0_var_replace.sed"
TMP_SQL_FILE="$LOCK_DIR/s1_0_update_mchnt_audit_info.sql"
echo 's/\${\(EXPORT_TS\)}/'${EXPORT_TS}/g > $TMP_SED_SCRIPT
sed -f $TMP_SED_SCRIPT $SH_HOME/sql/s1_0_update_mchnt_audit_info_with_template.sql > $TMP_SQL_FILE
exec_sql_script "$TMP_SQL_FILE" $@
if [ $? -gt 2 ] ; then
    RET_MESG="[ERROR] SQL[$TMP_SQL_FILE] failed!!!"
    logger $RET_MESG
    exit 7
fi

## 2.0 update audit task
TMP_SED_SCRIPT="$LOCK_DIR/s2_0_var_replace.sed"
TMP_SQL_FILE="$LOCK_DIR/s2_0_update_audit_task.sql"
echo 's/\${\(EXPORT_TS\)}/'${EXPORT_TS}/g > $TMP_SED_SCRIPT
sed -f $TMP_SED_SCRIPT $SH_HOME/sql/s2_0_update_audit_task_with_template.sql > $TMP_SQL_FILE
exec_sql_script "$TMP_SQL_FILE" $@
if [ $? -gt 2 ] ; then
    RET_MESG="[ERROR] SQL[$TMP_SQL_FILE] failed!!!"
    logger $RET_MESG
    exit 7
fi
## 2.1 assign data clean
TMP_SED_SCRIPT="$LOCK_DIR/s2_1_var_replace.sed"
TMP_SQL_FILE="$LOCK_DIR/s2_1_assign_data_clean.sql"
echo 's/\${\(EXPORT_TS\)}/'${EXPORT_TS}/g > $TMP_SED_SCRIPT
sed -f $TMP_SED_SCRIPT $SH_HOME/sql/s2_1_assign_data_clean_with_template.sql > $TMP_SQL_FILE
exec_sql_script "$TMP_SQL_FILE" $@
if [ $? -gt 2 ] ; then
    RET_MESG="[ERROR] SQL[$TMP_SQL_FILE] failed!!!"
    logger $RET_MESG
    exit 7
fi


## 3. backup mchnt audit info and audit task
SQL_FILE="$SH_HOME/sql/s3_0_backup_mchnt_audit_info_and_audit_task.sql"
exec_sql_script "$SQL_FILE" $@
if [ $? -gt 2 ] ; then
    RET_MESG="[ERROR] SQL[$SQL_FILE] failed!!!"
    logger $RET_MESG
    exit 7
fi

## 4. backup bat file
TMP_SED_SCRIPT="$LOCK_DIR/s4_0_var_replace.sed"
TMP_SQL_FILE="$LOCK_DIR/s4_0_backup_bat_file.sql"
echo 's/\${\(EXPORT_TS\)}/'${EXPORT_TS}/g > $TMP_SED_SCRIPT
sed -f $TMP_SED_SCRIPT $SH_HOME/sql/s4_0_backup_bat_file_with_template.sql > $TMP_SQL_FILE
exec_sql_script "$TMP_SQL_FILE" $@
if [ $? -gt 2 ] ; then
    RET_MESG="[ERROR] SQL[$TMP_SQL_FILE] failed!!!"
    logger $RET_MESG
    exit 7
fi


# db2 cmd result
if [ ! "$RET_MESG" = "SUCCESS" ] ; then
    exit 8
fi

## update completed timestamp
if [ "$1" == "force" ] ; then
    echo $EXPORT_TS > $COMP_TS_FILE_PATH
fi
