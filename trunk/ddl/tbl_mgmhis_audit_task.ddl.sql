SET CURRENT SCHEMA MGMHIS;

CREATE TABLE
    MGMHIS.TBL_MGMHIS_AUDIT_TASK
    (   
        TASK_ID VARCHAR(30) NOT NULL WITH DEFAULT,
        TASK_TP CHARACTER(2) NOT NULL WITH DEFAULT,
        TASK_PRI BIGINT NOT NULL WITH DEFAULT,
        TASK_COMMIT_TS TIMESTAMP,
        TASK_COMMIT_USR VARCHAR(32) NOT NULL WITH DEFAULT,
        TASK_ST CHARACTER(1) NOT NULL WITH DEFAULT,
        TASK_RESULT CHARACTER(1) NOT NULL WITH DEFAULT,
        TASK_PLAN_USR VARCHAR(32) NOT NULL WITH DEFAULT,
        TASK_EXEC_USR VARCHAR(32) NOT NULL WITH DEFAULT,
        TASK_ASSIGN_TS TIMESTAMP,
        TASK_PLAN_DONE_TS TIMESTAMP,
        TASK_EXEC_DONE_TS TIMESTAMP,
        MCHNT_SRV_TP CHARACTER(2) NOT NULL WITH DEFAULT,
        MCHNT_CD CHARACTER(15) NOT NULL WITH DEFAULT,
        MCHNT_CN_NM VARCHAR(100) NOT NULL WITH DEFAULT,
        ACQ_INS_ID_CD VARCHAR(11) NOT NULL WITH DEFAULT,
        ACPT_INS_ID_CD VARCHAR(11) NOT NULL WITH DEFAULT,
        CUP_BRANCH_INS_ID_CD VARCHAR(11) NOT NULL WITH DEFAULT,
        CONN_MD CHARACTER(1) NOT NULL WITH DEFAULT,
        MCHNT_TP CHARACTER(4) NOT NULL WITH DEFAULT,
        SPEC_DISC_TP CHARACTER(2) NOT NULL WITH DEFAULT,
        SPEC_DISC_LVL CHARACTER(1) NOT NULL WITH DEFAULT,
        IS_WHITE_MCHNT CHARACTER(1) NOT NULL WITH DEFAULT,
        ACQ_COMMIT_AUDIT_TS TIMESTAMP,
        AUDIT_ST CHARACTER(1) NOT NULL WITH DEFAULT,
        AVOID_USR VARCHAR(32) NOT NULL WITH DEFAULT,
        COMMISSION_TP CHARACTER(4) NOT NULL WITH DEFAULT,
        ACQ_PRI CHARACTER(1) NOT NULL WITH DEFAULT,
        AUDIT_TP_PRI CHARACTER(2) NOT NULL WITH DEFAULT,
        MCHNT_AUDIT_ID CHARACTER(30) NOT NULL WITH DEFAULT,
        REC_CRT_TS TIMESTAMP NOT NULL WITH DEFAULT,
        REC_UPD_TS TIMESTAMP NOT NULL WITH DEFAULT,
        EXPORT_TS TIMESTAMP NOT NULL,
        CONSTRAINT IND_MGMHIS_AT_PK PRIMARY KEY (TASK_ID)
    );

CREATE INDEX MGMHIS.IND_MGMHIS_AT_I1
ON MGMHIS.TBL_MGMHIS_AUDIT_TASK (
    MCHNT_CD ASC
    ,TASK_COMMIT_TS ASC
);

CREATE INDEX MGMHIS.IND_MGMHIS_AT_I2
ON MGMHIS.TBL_MGMHIS_AUDIT_TASK (
    MCHNT_AUDIT_ID DESC
);

GRANT ALL ON MGMHIS.TBL_MGMHIS_AUDIT_TASK TO USER OP_MGMAP;
GRANT SELECT ON MGMHIS.TBL_MGMHIS_AUDIT_TASK TO USER OP_MGMMN;
