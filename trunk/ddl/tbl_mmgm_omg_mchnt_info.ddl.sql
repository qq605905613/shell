SET CURRENT SCHEMA MMGM;

CREATE TABLE
    MMGM.TBL_MMGM_OMG_MCHNT_INFO
    (   
        MCHNT_SRV_TP CHARACTER(2) NOT NULL WITH DEFAULT,
        MCHNT_CD CHARACTER(15) NOT NULL WITH DEFAULT,
        MCHNT_CN_ABBR VARCHAR(100) NOT NULL WITH DEFAULT,
        MCHNT_CN_NM VARCHAR(100) NOT NULL WITH DEFAULT,
        ACQ_INS_ID_CD VARCHAR(11) NOT NULL WITH DEFAULT,
        ACPT_INS_ID_CD VARCHAR(11) NOT NULL WITH DEFAULT,
        FWD_INS_ID_CD VARCHAR(11) NOT NULL WITH DEFAULT,
        CUP_BRANCH_INS_ID_CD VARCHAR(11) NOT NULL WITH DEFAULT,
        CONN_MD CHARACTER(1) NOT NULL WITH DEFAULT,
        MCHNT_TP CHARACTER(4) NOT NULL WITH DEFAULT,
        SPEC_DISC_TP CHARACTER(2) NOT NULL WITH DEFAULT,
        SPEC_DISC_LVL CHARACTER(1) NOT NULL WITH DEFAULT,
        IS_WHITE_MCHNT CHARACTER(1) NOT NULL WITH DEFAULT,
        D_ALLOT_CD CHARACTER(5) NOT NULL WITH DEFAULT,
        C_ALLOT_CD CHARACTER(5) NOT NULL WITH DEFAULT,
        MCHNT_MEMO VARCHAR(128) NOT NULL WITH DEFAULT,
        CONSTRAINT IND_OMG_MI_PK PRIMARY KEY (MCHNT_CD)
    );

--CREATE INDEX MMGM.IND_OMG_DI_I1
--ON MMGM.TBL_MMGM_OMG_MCHNT_INFO (
--    INS_ID_CD
--);

GRANT ALL ON MMGM.TBL_MMGM_OMG_MCHNT_INFO TO USER OP_MGMAP;
GRANT SELECT ON MMGM.TBL_MMGM_OMG_MCHNT_INFO TO USER OP_MGMMN;