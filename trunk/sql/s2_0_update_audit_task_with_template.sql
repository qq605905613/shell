-- 评估任务表 <= 待评估商户列表
-- 2.1 A => T：待评估商户列表更新评估任务表
-- 2.1.1 T - A：任务状态更新为完成
update mmgm.tbl_mmgm_audit_task ot
set task_st = '0'
    ,rec_upd_ts = current timestamp
where not exists (
        select 1
        from mmgm.tbl_mmgm_unaudit_mchnt
        where mchnt_cd = ot.mchnt_cd
            and (case when audit_st in ('1','2','3','4') then '4' else audit_st end) = ot.audit_st
    ) and task_st = '1'
;
-- 2.1.2 A - T：在T表中新增记录
insert into mmgm.tbl_mmgm_audit_task(
    TASK_ID,TASK_TP,TASK_COMMIT_TS,TASK_COMMIT_USR,TASK_ST,TASK_RESULT,MCHNT_SRV_TP
    ,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD,CUP_BRANCH_INS_ID_CD,CONN_MD
    ,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL,IS_WHITE_MCHNT,ACQ_COMMIT_AUDIT_TS,AUDIT_ST
    ,AVOID_USR,COMMISSION_TP,ACQ_PRI,AUDIT_TP_PRI,MCHNT_AUDIT_ID
    ,EXPORT_TS
)
select
    -- task_id，对于初级（分公司）评估，仅关注一次
    '${EXPORT_TS}' || mchnt_cd || case when audit_st in ('1','2','3') then '4' else audit_st end
    -- task_tp
    ,case
        when audit_st in ('1','2','3','4') then 'BA'
        when audit_st in ('F','G') then 'HA'
        else '--'
    end as task_tp
    -- task_commit_ts
    ,case
        when audit_st in ('1','2','3','4') and acq_commit_audit_ts + 1 day > '${EXPORT_TS}' then acq_commit_audit_ts
        when audit_st = 'G' and cup_1st_audited_ts + 1 day > '${EXPORT_TS}' then cup_1st_audited_ts
        when audit_st = 'F' and cup_branch_audited_ts + 1 day > '${EXPORT_TS}' then cup_branch_audited_ts
        else '${EXPORT_TS}'
    end as task_commit_ts
    -- task_commit_usr
    ,case 
        when audit_st in ('1','2','3','4') then 'ACQ'
        when audit_st = 'F' then cup_branch_auditor
        when audit_st = 'G' then cup_1st_auditor
        else 'NA'
    end as task_commit_usr
    -- task_st
    ,'1'    as task_st
    ,'1'    as task_result
    ,MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD
    ,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL
    ,IS_WHITE_MCHNT,ACQ_COMMIT_AUDIT_TS
    -- audit_st：一、二、三、四审归并成四审
    ,case
        when audit_st in ('1','2','3','4') then '4'
        else audit_st
    end     as AUDIT_ST
    -- avoid_usr
    ,case
        when audit_st = 'G' then cup_branch_auditor
        else ''
    end as avoid_usr
    -- commission_tp
    ,'' as commission_tp
    -- acq_pri
    ,'1'    as acq_pri
    -- audit_tp_pri
    ,case
        when audit_st in ('1','2','3','4') then 'B4'
        when audit_st in ('F','G') then 'A' || audit_st
        else '--'
    end as audit_tp_pri
    -- mchnt_audit_id
    ,'${EXPORT_TS}' || mchnt_cd || '4'  mchnt_audit_id
    -- export_ts
    ,'${EXPORT_TS}' as export_ts
from mmgm.tbl_mmgm_unaudit_mchnt ot
where not exists (
        select 1
        from mmgm.tbl_mmgm_audit_task
        where mchnt_cd = ot.mchnt_cd 
            and audit_st = case when ot.audit_st in ('1','2','3') then '4' else ot.audit_st end
    )
;

-- 评估任务表 <= 评估通过商户列表
-- 2.2 B & T：任务表状态更新为评估通过
merge into mmgm.tbl_mmgm_audit_task dst
using (
    select mchnt_cd
        ,'0'        as task_st
        ,'0'        as task_result
        ,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR
        ,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR
        ,CUP_2ND_AUDITED_TS,CUP_2ND_AUDITOR
    from mmgm.tbl_mmgm_audited_mchnt
) src
on src.mchnt_cd = dst.mchnt_cd
when matched and dst.task_result <> '0' then
update set (
    task_st,task_result,task_exec_usr,task_exec_done_ts,rec_upd_ts
) = (
    src.task_st,src.task_result
    ,case
        when dst.audit_st = '4' then src.cup_branch_auditor
        when dst.audit_st = 'F' then src.cup_1st_auditor
        when dst.audit_st = 'G' then src.cup_2nd_auditor
        else 'NA'
    end
    ,case
        when dst.audit_st = '4' then src.cup_branch_audited_ts
        when dst.audit_st = 'F' then src.cup_1st_audited_ts
        when dst.audit_st = 'G' then src.cup_2nd_audited_ts
        else '${EXPORT_TS}'
    end
    ,current timestamp
)
;


-- 评估任务表 <= 评估拒绝商户列表
-- 2.3 C & T：任务表状态更新为评估拒绝
merge into mmgm.tbl_mmgm_audit_task dst
using (
    select mchnt_cd
        ,'0'        as task_st
        ,'2'        as task_result
        ,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR
        ,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR
        ,CUP_2ND_AUDITED_TS,CUP_2ND_AUDITOR
    from mmgm.tbl_mmgm_reject_mchnt
) src
on src.mchnt_cd = dst.mchnt_cd
when matched and dst.task_result not in ('0','2') then
update set (
    task_st,task_result,task_exec_usr,task_exec_done_ts,rec_upd_ts
) = (
    src.task_st,src.task_result
    ,case
        when dst.audit_st = '4' then src.cup_branch_auditor
        when dst.audit_st = 'F' then src.cup_1st_auditor
        when dst.audit_st = 'G' then src.cup_2nd_auditor
        else 'NA'
    end
    ,case
        when dst.audit_st = '4' then src.cup_branch_audited_ts
        when dst.audit_st = 'F' then src.cup_1st_audited_ts
        when dst.audit_st = 'G' then src.cup_2nd_audited_ts
        else '${EXPORT_TS}'
    end
    ,current timestamp
)
;
