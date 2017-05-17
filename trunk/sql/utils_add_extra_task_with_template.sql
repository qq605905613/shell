-- 更新派单表，对应任务状态为已派单且未处理完成，重新派发
update
(
    select task_id,mchnt_cd,task_result,rec_upd_ts
    from mmgm.tbl_mmgm_task_assign
    where task_result <> '3' 
        and rec_upd_ts < '${EXPORT_TS}'
        -- and rec_upd_ts < '20160904151000'
        and task_id in (
            select task_id
            from mmgm.tbl_mmgm_audit_task
            where task_st = '1' and task_result = '3' 
        )   
)
set task_result = '3',rec_upd_ts = current timestamp
;
insert into mmgm.tbl_mmgm_audit_task(
    TASK_ID,TASK_TP,TASK_COMMIT_TS,TASK_COMMIT_USR,TASK_ST,TASK_RESULT,MCHNT_SRV_TP
    ,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD,CUP_BRANCH_INS_ID_CD,CONN_MD
    ,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL,IS_WHITE_MCHNT,ACQ_COMMIT_AUDIT_TS,AUDIT_ST
    ,AVOID_USR,COMMISSION_TP,ACQ_PRI,AUDIT_TP_PRI,MCHNT_AUDIT_ID
    ,EXPORT_TS
)
select 
    -- task_id
    varchar_format(current timestamp, 'YYYYMMDDHH24MISS') || mchnt_cd 
        || case when audit_st = '4' then 'F' when audit_st = 'F' then 'G' else '*' end
    -- task_tp
    ,'HA'
    -- task_commit_ts
    ,actual_done_ts
    -- task_commit_usr
    ,assign_usr_cd
    -- task_st,task_result
    ,'1','1'
    -- mchnt info
    ,mchnt_srv_tp,mchnt_cd,mchnt_cn_nm,acq_ins_id_cd,acpt_ins_id_cd
    ,cup_branch_ins_id_cd,conn_md,mchnt_tp,spec_disc_tp,spec_disc_lvl
    ,is_white_mchnt,acq_commit_audit_ts
    -- audit_st
    ,case when audit_st = '4' then 'F' when audit_st = 'F' then 'G' else '*' end
    -- avoid_usr: bug, can not find avoid user
    ,''
    -- commission_tp
    ,''
    -- acq_pri
    ,'2'
    -- audit_tp_pri
    ,'A' || case when audit_st = '4' then 'F' when audit_st = 'F' then 'G' else '*' end
    -- mchnt_audit_id
    ,mchnt_audit_id
    -- export_ts
    ,current timestamp
from mmgm.tbl_mmgm_task_assign
where task_id in (
        select task_id
        from mmgm.tbl_mmgm_audit_task
    )
    -- 审核通过的任务
    and audit_st in ('4','F') and task_result = '0'
;
