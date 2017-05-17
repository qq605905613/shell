-- 备份评估通过商户列表
-- 4.1 B表备份，关联条件：导出时间+商户代码
insert into mgmhis.tbl_mgmhis_audited_mchnt
select MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD
    ,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL
    ,IS_WHITE_MCHNT,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR
    ,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR,CUP_2ND_AUDITED_TS,CUP_2ND_AUDITOR
    ,REC_CRT_TS,REC_UPD_TS
    -- mchnt_audit_id
    ,'${EXPORT_TS}' || mchnt_cd || '4'    as mchnt_audit_id
    -- export_ts
    ,'${EXPORT_TS}'
from mmgm.tbl_mmgm_audited_mchnt ot
where not exists (
    select 1 from mgmhis.tbl_mgmhis_audited_mchnt 
    where mchnt_cd = ot.mchnt_cd and export_ts = '${EXPORT_TS}'
)
;

-- 备份评估拒绝商户列表
-- 4.2 C - C_HIS => C：备份C表至C_HIS
insert into mgmhis.tbl_mgmhis_reject_mchnt
select MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD
    ,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL
    ,IS_WHITE_MCHNT,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR
    ,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR,CUP_2ND_AUDITED_TS,CUP_2ND_AUDITOR
    ,ACQ_AUDIT_MEMO,CUP_BRANCH_MEMO,CUP_HEAD_MEMO
    ,REC_CRT_TS,REC_UPD_TS
    -- mchnt_audit_id = export_ts + mchnt_cd + '9'
    ,'${EXPORT_TS}' || mchnt_cd || '9'    as mchnt_audit_id
    -- export_ts
    ,'${EXPORT_TS}'
    -- exits_ts
    , '9999-12-31 23:59:59'
from mmgm.tbl_mmgm_reject_mchnt ot
where not exists (
        select 1 from mgmhis.tbl_mgmhis_reject_mchnt 
        where mchnt_cd = ot.mchnt_cd
            and exists_ts = '9999-12-31 23:59:59'
    )
;

-- 备份待审商户列表
-- 4.3.1 A_HIS - A：更新A_HIS表存在时间，更新条件：商户代码+商户状态+消失时间
update mgmhis.tbl_mgmhis_unaudit_mchnt ot
set task_exists_ts = '${EXPORT_TS}'
    ,rec_upd_ts = current timestamp
where not exists (
        select 1
        from mmgm.tbl_mmgm_unaudit_mchnt
        where mchnt_cd = ot.mchnt_cd
            and audit_st = ot.audit_st
    ) 
    and task_exists_ts = '9999-12-31 23:59:59'
;

-- 4.3.2 A - A_HIS => A_HIS：新增记录进行插入
insert into mgmhis.tbl_mgmhis_unaudit_mchnt
select
    MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD
    ,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL
    ,IS_WHITE_MCHNT,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR
    ,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR,AUDIT_ST
    ,REC_CRT_TS,REC_UPD_TS
    -- mchnt_audit_id
    ,'${EXPORT_TS}' || mchnt_cd || '4'    as mchnt_audit_id
    -- task_id
    ,'${EXPORT_TS}' || mchnt_cd || case when audit_st in ('1','2','3') then '4' else audit_st end
    -- task_commit_ts
    ,case
        when audit_st in ('1','2','3','4') and acq_commit_audit_ts + 1 day > '${EXPORT_TS}' then acq_commit_audit_ts
        when audit_st = 'G' and cup_1st_audited_ts + 1 day > '${EXPORT_TS}' then cup_1st_audited_ts
        when audit_st = 'F' and cup_branch_audited_ts + 1 day > '${EXPORT_TS}' then cup_branch_audited_ts
        else '${EXPORT_TS}'
    end                     as task_commit_ts
    -- task_export_ts
    ,'${EXPORT_TS}'         as task_export_ts
    -- task_exists_ts
    ,'9999-12-31 23:59:59'  as task_exists_ts
from mmgm.tbl_mmgm_unaudit_mchnt ot
where not exists (
        select 1 
        from mgmhis.tbl_mgmhis_unaudit_mchnt
        where mchnt_cd = ot.mchnt_cd
            and audit_st = ot.audit_st
            and task_exists_ts = '9999-12-31 23:59:59'
    )
;
