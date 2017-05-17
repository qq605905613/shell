-- 全量导出的拒绝商户列表处理成增量新增拒绝商户列表
-- 0.1.1 在C_HIS但不在C中，更新表存在时间
update mgmhis.tbl_mgmhis_reject_mchnt ot
set exists_ts = '${EXPORT_TS}'
where not exists (
        select 1
        from mmgm.tbl_mmgm_reject_mchnt
        where mchnt_cd = ot.mchnt_cd
    )
    and exists_ts = '9999-12-31 23:59:59'
;
-- 0.1.2 在C中也在C_HIS中，删除
delete from mmgm.tbl_mmgm_reject_mchnt ot
where exists (
        select 1
        from mgmhis.tbl_mgmhis_reject_mchnt
        where mchnt_cd = ot.mchnt_cd
            and exists_ts = '9999-12-31 23:59:59'
    )
;


-- 一个审核通过的商户会出现在当天及下一天的【审核通过商户列表】中
-- 审核通过商户列表 => 增量审核通过列表
-- 0.2.1 在B中也在B_HIS中，删除
delete from mmgm.tbl_mmgm_audited_mchnt ot
where exists (
        select 1
        from mgmhis.tbl_mgmhis_audited_mchnt
        where mchnt_cd = ot.mchnt_cd
            and varchar_format(cup_2nd_audited_ts, 'YYYYMMDDHH24MISS') = varchar_format(ot.cup_2nd_audited_ts, 'YYYYMMDDHH24MISS')
    )
;
