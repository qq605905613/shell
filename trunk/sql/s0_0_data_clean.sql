-- 删除派单记录，其对应的任务记录已完成但未派单
select * from old table
(
    delete from mmgm.tbl_mmgm_task_assign
    where task_id in (
            select task_id
            from mmgm.tbl_mmgm_audit_task
            where task_st = '0' and task_result = '1'
        )
)
;
-- 删除任务记录，其状态为已完成，未派单或已派单
insert into mgmhis.tbl_mgmhis_audit_task
select *
from mmgm.tbl_mmgm_audit_task ot
where task_st = '0' and task_result in ('1','3')
    and not exists (
        select 1 from mgmhis.tbl_mgmhis_audit_task
        where task_id = ot.task_id
    )   
;
delete from
(
    select task_id
    from mmgm.tbl_mmgm_audit_task
    where task_st = '0' and task_result in ('1','3')
)
;
-- 删除派单记录，其对应的任务记录未完成、未派单
select * from old table
(
    delete from mmgm.tbl_mmgm_task_assign
    where task_id in (
            select task_id
            from mmgm.tbl_mmgm_audit_task
            where task_st = '1' and task_result = '1'
        )
)
;
