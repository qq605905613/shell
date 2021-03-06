ALTER TABLE MMGM.TBL_MMGM_UNAUDIT_MCHNT ADD COLUMN BUSS_TP CHARACTER(2) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_UNAUDIT_MCHNT ADD COLUMN PROD_FUNC CHARACTER(4) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_UNAUDIT_MCHNT ADD COLUMN D_ALLOT_CD CHARACTER(5) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_UNAUDIT_MCHNT ADD COLUMN C_ALLOT_CD CHARACTER(5) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_UNAUDIT_MCHNT ADD COLUMN SINGLE_AT_LIMIT VARCHAR(16) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_UNAUDIT_MCHNT ADD COLUMN SINGLE_CARD_DAY_AT_LIMIT VARCHAR(16) DEFAULT '' NOT NULL;

ALTER TABLE MMGM.TBL_MMGM_AUDITED_MCHNT ADD COLUMN BUSS_TP CHARACTER(2) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_AUDITED_MCHNT ADD COLUMN PROD_FUNC CHARACTER(4) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_AUDITED_MCHNT ADD COLUMN D_ALLOT_CD CHARACTER(5) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_AUDITED_MCHNT ADD COLUMN C_ALLOT_CD CHARACTER(5) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_AUDITED_MCHNT ADD COLUMN SINGLE_AT_LIMIT VARCHAR(16) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_AUDITED_MCHNT ADD COLUMN SINGLE_CARD_DAY_AT_LIMIT VARCHAR(16) DEFAULT '' NOT NULL;

ALTER TABLE MMGM.TBL_MMGM_REJECT_MCHNT ADD COLUMN BUSS_TP CHARACTER(2) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_REJECT_MCHNT ADD COLUMN PROD_FUNC CHARACTER(4) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_REJECT_MCHNT ADD COLUMN D_ALLOT_CD CHARACTER(5) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_REJECT_MCHNT ADD COLUMN C_ALLOT_CD CHARACTER(5) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_REJECT_MCHNT ADD COLUMN SINGLE_AT_LIMIT VARCHAR(16) DEFAULT '' NOT NULL;
ALTER TABLE MMGM.TBL_MMGM_REJECT_MCHNT ADD COLUMN SINGLE_CARD_DAY_AT_LIMIT VARCHAR(16) DEFAULT '' NOT NULL;
