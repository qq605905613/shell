-- 商户评估信息表备份
-- 3.1.1 将已经完成的商户评估信息表记录备份至历史表
insert into mgmhis.tbl_mgmhis_mchnt_audit_info
select *
from mmgm.tbl_mmgm_mchnt_audit_info ot
where not exists (
        select 1
        from mgmhis.tbl_mgmhis_mchnt_audit_info
        where mchnt_cd = ot.mchnt_cd
            and varchar_format(finish_ts, 'YYYYMMDDHH24MISS') = varchar_format(ot.finish_ts, 'YYYYMMDDHH24MISS')
    )
    --and mchnt_audit_st not in ('4','F','G')
    and mchnt_audit_st in ('9','|','8','_','-','=')
; 
-- 3.1.2 删除已经完成的商户评估信息表记录
delete from mmgm.tbl_mmgm_mchnt_audit_info
--where mchnt_audit_st not in ('4','F','G')
where mchnt_audit_st in ('9','|','8','_','-','=')
;

-- 商户评估任务表备份
-- 3.2.1份audit_task中已经结束且结果已经明确的任务
insert into mgmhis.tbl_mgmhis_audit_task
select *
from mmgm.tbl_mmgm_audit_task ot
where task_st = '0' and task_result in ('0','2')
    and not exists (
        select 1 from mgmhis.tbl_mgmhis_audit_task
        where task_id = ot.task_id
    )
;
-- 3.2.2 删除已经结束且结果已经明确的任务
delete from mmgm.tbl_mmgm_audit_task 
where task_st = 0 and task_result in ('0','2')
;
-- 3.2.3 删除已经结束且不申请非标价格的任务
delete from mmgm.tbl_mmgm_audit_task 
where task_st = 0 and is_white_mchnt = '0'
;
