-- 商户评估信息表 <= 待评估商户列表
-- 1.1 A => M：待评估商户列表合入商户评估信息表
merge into mmgm.tbl_mmgm_mchnt_audit_info dst
using (
    select MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD
        ,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL
        ,IS_WHITE_MCHNT
        ,''                 as commission_tp
        ,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR
        ,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR
        -- cup_2nd_audited_ts
        ,null              as CUP_2ND_AUDITED_TS
        ,''                 as CUP_2ND_AUDITOR
        -- mchnt_audit_st
        ,case when audit_st in ('1','2','3','4') then '4' else audit_st end      as mchnt_audit_st
        -- mchnt_audit_id = export_ts + mchnt_cd + '4'
        ,'${EXPORT_TS}' || mchnt_cd || '4'  as mchnt_audit_id
        -- export_ts
        ,'${EXPORT_TS}'         as export_ts
        -- finish_ts
        ,'9999-12-31 23:59:59'  as finish_ts
    from mmgm.tbl_mmgm_unaudit_mchnt
    where is_white_mchnt = '1'
) src 
on src.mchnt_cd = dst.mchnt_cd
when not matched then
insert (
    MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD
    ,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL
    ,IS_WHITE_MCHNT,COMMISSION_TP,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR
    ,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR,CUP_2ND_AUDITED_TS,CUP_2ND_AUDITOR
    ,MCHNT_AUDIT_ST,MCHNT_AUDIT_ID
    ,EXPORT_TS,FINISH_TS
) values (
    MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD
    ,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL
    ,IS_WHITE_MCHNT,COMMISSION_TP,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR
    ,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR,CUP_2ND_AUDITED_TS,CUP_2ND_AUDITOR
    ,MCHNT_AUDIT_ST,MCHNT_AUDIT_ID
    ,EXPORT_TS,FINISH_TS
)
when matched and src.mchnt_audit_st = 'F' and dst.mchnt_audit_st = '4' then
update set (
        cup_branch_audited_ts,cup_branch_auditor
        ,mchnt_audit_st,rec_upd_ts
    ) = (
        src.cup_branch_audited_ts,src.cup_branch_auditor
        ,'F',current timestamp
    )
when matched and src.mchnt_audit_st = 'G' and dst.mchnt_audit_st = 'F' then
update set (
        cup_1st_audited_ts,cup_1st_auditor
        ,mchnt_audit_st,rec_upd_ts
    ) = (
        src.cup_1st_audited_ts,src.cup_1st_auditor
        ,'G',current timestamp
    )
when matched and src.mchnt_audit_st = 'G' and dst.mchnt_audit_st = '4' then
update set (
        cup_branch_audited_ts,cup_branch_auditor
        ,cup_1st_audited_ts,cup_1st_auditor
        ,mchnt_audit_st,rec_upd_ts
    ) = (
        src.cup_branch_audited_ts,src.cup_branch_auditor
        ,src.cup_1st_audited_ts,src.cup_1st_auditor
        ,'G',current timestamp
    )
;

-- 商户评估信息表 <= 商评估通过商户列表
-- 1.2 B => M：评估通过商户列表更新商户评估信息表
merge into mmgm.tbl_mmgm_mchnt_audit_info dst
using (
    select MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD
        ,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL
        ,IS_WHITE_MCHNT
        ,''                 as commission_tp
        ,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR
        ,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR,CUP_2ND_AUDITED_TS,CUP_2ND_AUDITOR
        -- mchnt_audit_st
        ,'8'                as mchnt_audit_st
        -- mchnt_audit_id
        ,'${EXPORT_TS}' || mchnt_cd || '4'  as mchnt_audit_id
        -- export_ts
        ,'${EXPORT_TS}' as export_ts
        -- finish_ts
        ,cup_2nd_audited_ts as finish_ts
    from mmgm.tbl_mmgm_audited_mchnt
    where is_white_mchnt = '1'
) src 
on src.mchnt_cd = dst.mchnt_cd
when not matched then
insert (
    MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD
    ,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL
    ,IS_WHITE_MCHNT,COMMISSION_TP,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR
    ,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR,CUP_2ND_AUDITED_TS,CUP_2ND_AUDITOR
    ,MCHNT_AUDIT_ST,MCHNT_AUDIT_ID
    ,EXPORT_TS,FINISH_TS
) values (
    MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD
    ,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL
    ,IS_WHITE_MCHNT,COMMISSION_TP,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR
    ,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR,CUP_2ND_AUDITED_TS,CUP_2ND_AUDITOR
    ,MCHNT_AUDIT_ST,MCHNT_AUDIT_ID
    ,EXPORT_TS,FINISH_TS
)
when matched and dst.mchnt_audit_st = 'G' then
update set (
        cup_2nd_audited_ts,cup_2nd_auditor
        ,mchnt_audit_st,finish_ts,rec_upd_ts
    ) = (
        src.cup_2nd_audited_ts,src.cup_2nd_auditor
        ,'8',src.cup_2nd_audited_ts,current timestamp
    )
when matched and dst.mchnt_audit_st = 'F' then
update set (
        cup_2nd_audited_ts,cup_2nd_auditor
        ,cup_1st_audited_ts,cup_1st_auditor
        ,mchnt_audit_st,finish_ts,rec_upd_ts
    ) = (
        src.cup_2nd_audited_ts,src.cup_2nd_auditor
        ,src.cup_1st_audited_ts,src.cup_1st_auditor
        ,'8',src.cup_2nd_audited_ts,current timestamp
    )
when matched and dst.mchnt_audit_st = '4' then
update set (
        cup_2nd_audited_ts,cup_2nd_auditor
        ,cup_1st_audited_ts,cup_1st_auditor
        ,cup_branch_audited_ts,cup_branch_auditor
        ,mchnt_audit_st,finish_ts,rec_upd_ts
    ) = (
        src.cup_2nd_audited_ts,src.cup_2nd_auditor
        ,src.cup_1st_audited_ts,src.cup_1st_auditor
        ,src.cup_branch_audited_ts,src.cup_branch_auditor
        ,'8',src.cup_2nd_audited_ts,current timestamp
    )
;

-- 商户评估信息表 <= 评估拒绝商户列表
-- 1.3 C => M：评估拒绝商户列表更新商户评估信息表
merge into mmgm.tbl_mmgm_mchnt_audit_info dst
using (
    select MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD
        ,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL
        ,IS_WHITE_MCHNT
        ,''                 as commission_tp
        ,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR
        ,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR,CUP_2ND_AUDITED_TS,CUP_2ND_AUDITOR
        -- mchnt_audit_st
        ,case
            when cup_2nd_audited_ts >= cup_1st_audited_ts 
                and cup_1st_audited_ts >= cup_branch_audited_ts
            then '='
            when cup_1st_audited_ts >= cup_branch_audited_ts
            then '-'
            else '_'
        end                 as mchnt_audit_st
        -- mchnt_audit_id
        ,'${EXPORT_TS}' || mchnt_cd || '4'  as mchnt_audit_id
        -- export_ts
        ,'${EXPORT_TS}'     as export_ts
        -- finish_ts
        ,case
            when cup_2nd_audited_ts >= cup_1st_audited_ts 
                and cup_1st_audited_ts >= cup_branch_audited_ts 
            then cup_2nd_audited_ts
            when cup_1st_audited_ts >= cup_branch_audited_ts 
            then cup_1st_audited_ts
            else cup_branch_audited_ts
        end                 as finish_ts
        -- reject_memo
        ,case
            when cup_branch_audited_ts > cup_1st_audited_ts then cup_branch_memo
            else cup_head_memo
        end                 as reject_memo
    from mmgm.tbl_mmgm_reject_mchnt
    where is_white_mchnt = '1'
) src 
on src.mchnt_cd = dst.mchnt_cd
when not matched then
insert (
    MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD
    ,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL
    ,IS_WHITE_MCHNT,COMMISSION_TP,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR
    ,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR,CUP_2ND_AUDITED_TS,CUP_2ND_AUDITOR
    ,MCHNT_AUDIT_ST,MCHNT_AUDIT_ID
    ,EXPORT_TS,FINISH_TS,REJECT_MEMO
) values (
    MCHNT_SRV_TP,MCHNT_CD,MCHNT_CN_NM,ACQ_INS_ID_CD,ACPT_INS_ID_CD
    ,CUP_BRANCH_INS_ID_CD,CONN_MD,MCHNT_TP,SPEC_DISC_TP,SPEC_DISC_LVL
    ,IS_WHITE_MCHNT,COMMISSION_TP,ACQ_COMMIT_AUDIT_TS,CUP_BRANCH_AUDITED_TS,CUP_BRANCH_AUDITOR
    ,CUP_1ST_AUDITED_TS,CUP_1ST_AUDITOR,CUP_2ND_AUDITED_TS,CUP_2ND_AUDITOR
    ,MCHNT_AUDIT_ST,MCHNT_AUDIT_ID
    ,EXPORT_TS,FINISH_TS,REJECT_MEMO
)
when matched then
update set (
        mchnt_audit_st,finish_ts,rec_upd_ts,reject_memo
        ,cup_branch_audited_ts,cup_branch_auditor
        ,cup_1st_audited_ts,cup_1st_auditor
        ,cup_2nd_audited_ts,cup_2nd_auditor
    ) = (
        -- mchnt_audit_st：商户评估状态，区分拒绝时间的状态
        case
            when dst.is_white_mchnt = '0' then '|'
            when dst.mchnt_audit_st = 'G' 
                or (dst.mchnt_audit_st = 'F' and src.cup_2nd_audited_ts > src.cup_1st_audited_ts)
                or (dst.mchnt_audit_st = '4' and src.cup_2nd_audited_ts > src.cup_branch_audited_ts)
            then '='
            when dst.mchnt_audit_st = 'F' 
                or (dst.mchnt_audit_st = '4' and src.cup_1st_audited_ts > src.cup_branch_audited_ts)
            then '-'
            when dst.mchnt_audit_st = '4' then '_'
            else src.mchnt_audit_st
        end
        ,src.finish_ts,current timestamp,src.reject_memo
        ,case 
            when dst.mchnt_audit_st = '4' then src.cup_branch_audited_ts
            else dst.cup_branch_audited_ts
        end
        ,case 
            when dst.mchnt_audit_st = '4' then src.cup_branch_auditor
            else dst.cup_branch_auditor
        end
        ,case
            when dst.mchnt_audit_st = 'F' then src.cup_1st_audited_ts
            when dst.mchnt_audit_st = '4' 
                and src.cup_1st_audited_ts > src.cup_branch_audited_ts then src.cup_1st_audited_ts
            else dst.cup_1st_audited_ts
        end
        ,case
            when dst.mchnt_audit_st = 'F' then src.cup_1st_auditor
            when dst.mchnt_audit_st = '4' 
                and src.cup_1st_audited_ts > src.cup_branch_audited_ts then src.cup_1st_auditor
            else dst.cup_1st_auditor
        end
        ,case 
            when dst.mchnt_audit_st = 'G' then src.cup_2nd_audited_ts
            when dst.mchnt_audit_st = 'F' 
                and src.cup_2nd_audited_ts > src.cup_1st_audited_ts then src.cup_2nd_audited_ts
            when dst.mchnt_audit_st = '4' 
                and src.cup_2nd_audited_ts > src.cup_branch_audited_ts then src.cup_2nd_audited_ts
            else dst.cup_2nd_audited_ts
        end
        ,case 
            when dst.mchnt_audit_st = 'G' then src.cup_2nd_auditor
            when dst.mchnt_audit_st = 'F' 
                and src.cup_2nd_audited_ts > src.cup_1st_audited_ts then src.cup_2nd_auditor
            when dst.mchnt_audit_st = '4' 
                and src.cup_2nd_audited_ts > src.cup_branch_audited_ts then src.cup_2nd_auditor
            else dst.cup_2nd_auditor
        end
    )
;

-- 商户评估信息表 <= 待评估商户列表
-- 1.4 M - A：不在待评估商户列表中的商户评估信息记录更新为拒绝
update mmgm.tbl_mmgm_mchnt_audit_info ot
set (
        mchnt_audit_st,finish_ts,reject_memo,rec_upd_ts
    ) = (
        -- mchnt_audit_st：白名单商户才会出现在评估通过或拒绝列表，才能明确知道结果
        case
            when is_white_mchnt = '1' and mchnt_audit_st = '4' then '_'
            when is_white_mchnt = '1' and mchnt_audit_st = 'F' then '-'
            when is_white_mchnt = '1' and mchnt_audit_st = 'G' then '='
            when is_white_mchnt = '1' then '9'
            -- 非白名单商户结束
            else '|'
        end
        ,'${EXPORT_TS}'
        ,'UPD_BY_DEFAULT'
        ,current timestamp
    )
where not exists (
        select 1
        from mmgm.tbl_mmgm_unaudit_mchnt
        where mchnt_cd = ot.mchnt_cd
    ) 
    and mchnt_audit_st in ('4','F','G')
;
