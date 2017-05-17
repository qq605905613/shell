-- 更新派单表，对应任务状态为已派单且未处理完成，重新派发
update
(
    select task_id,task_result,rec_upd_ts
    from mmgm.tbl_mmgm_task_assign
    where task_result not in ('3','4')
        and rec_upd_ts < '${EXPORT_TS}'
        and task_id in (
            select task_id
            from mmgm.tbl_mmgm_audit_task
            where task_st = '1' and task_result = '3'
        )
)
set task_result = '3',rec_upd_ts = current timestamp
;

-- 拦截商户
update 
(
    select task_id,task_result,rec_upd_ts
    from mmgm.tbl_mmgm_audit_task
    where is_white_mchnt in ('1','2')
        and task_st <> '0' and task_result = '1'
        and (
            (cup_branch_ins_id_cd in ('0800010000','0800019800') and audit_st = '4')
            or audit_st in ('F','G')
        )
        and mchnt_cd in (
            select mchnt_cd from mmgm.tbl_mmgm_omg_mchnt_info
        )
)
set task_result = 'Z', rec_upd_ts = current timestamp
;
----瑞银信海科融通拦截 20170320
 update 
        (select  task_result ,rec_upd_ts from  mmgm.TBL_MMGM_AUDIT_TASK where ACQ_INS_ID_CD in('0848330000','0848870000','0848680000','0848380000','0948680000','0848230000') 
                 and  task_result ='1' and task_st='1') 
        set task_result ='Z ',  rec_upd_ts = current timestamp
;

-- 商户分组
-- 修改日志
-- 20170106 政府服务类商户合并到减免商户组M3
-- 20170306 zhiliu 4887 4833 4864
update (
    -- 商户组(M1)：重点机构+浙江地区非金机构
    select commission_tp
    from mmgm.tbl_mmgm_audit_task
    where is_white_mchnt = '1'
        -- 总公司审核或0000、9800分公司审核
        and (audit_st in ('F','G') or (audit_st = '4' and cup_branch_ins_id_cd in ('0800010000','0800019800')))
        and (
            -- 重点机构
            acq_ins_id_cd in ('0848870000'
                ,'0848330000','0848640000','0848230000','0848380000','0848680000')
    
        )
)
set commission_tp = 'M1'
;

update (
    -- 商户组(M2)：除M1以外的所有批量注册的商户（MMGM.TBL_MMGM_BATCH_MCHNT_INFO)
    select commission_tp
    from mmgm.tbl_mmgm_audit_task
    where is_white_mchnt = '1'
        -- 总公司审核或0000、9800分公司审核
        and (audit_st in ('F','G') or (audit_st = '4' and cup_branch_ins_id_cd in ('0800010000','0800019800')))
        -- 批量现场注册
        and commission_tp = '' and mchnt_cd in (select mchnt_cd from mmgm.tbl_mmgm_batch_mchnt_info)
)
set commission_tp = 'M2'
;

-- 20170106 政府服务类商户合并到减免商户组M3
update (
    -- 商户组：政府服务类
    select commission_tp
    from mmgm.tbl_mmgm_audit_task
    where is_white_mchnt = '1'
        -- 总公司审核或0000、9800分公司审核
        and (audit_st in ('F','G') or (audit_st = '4' and cup_branch_ins_id_cd in ('0800010000','0800019800')))
        -- 政府服务类
        and mchnt_tp in ('8651','9211','9222','9223','9311','9399')
)
set commission_tp = 'M4'
;

update (
    -- 商户组(M3)：除M1、M2以外的特殊计费商户
    select commission_tp
    from mmgm.tbl_mmgm_audit_task
    where is_white_mchnt = '1'
        -- 总公司审核或0000、9800分公司审核
        and (audit_st in ('F','G') or (audit_st = '4' and cup_branch_ins_id_cd in ('0800010000','0800019800')))
        -- 除政府服务类以外的特殊计费
        and commission_tp = '' and (spec_disc_tp in ('02','03') or (spec_disc_tp = '01' and spec_disc_lvl = '1'))
        and mchnt_tp not in ('8651','9211','9222','9223','9311','9399')
)
set commission_tp = 'M3'
;

update (
    -- 商户组(M4)：除M1、M2、M3以外的减免类商户
    select commission_tp
    from mmgm.tbl_mmgm_audit_task
    where is_white_mchnt = '1'
        -- 总公司审核或0000、9800分公司审核
        and (audit_st in ('F','G') or (audit_st = '4' and cup_branch_ins_id_cd in ('0800010000','0800019800')))
        -- 减免类
        and commission_tp = '' and mchnt_tp in ('8062','8011','8021','8031','8041','8042','8049'
                ,'8099','8211','8220','8351','8241','8398')
)
set commission_tp = 'M4'
;

update (
    -- 商户组(M5)：其它商户，包括优惠类商户等
    select commission_tp
    from mmgm.tbl_mmgm_audit_task
    where is_white_mchnt = '1'
        -- 总公司审核或0000、9800分公司审核
        and (audit_st in ('F','G') or (audit_st = '4' and cup_branch_ins_id_cd in ('0800010000','0800019800')))
        -- 其它
        and commission_tp = ''
)
set commission_tp = 'M5'
;

 --易宝支付
 update (select TASK_RESULT from mmgm.TBL_MMGM_AUDIT_TASK where 
             substr(ACQ_INS_ID_CD,3,4) ='4825'  and 
                  substr(ACPT_INS_ID_CD,7,2) in('49','50','51','42','43','44','24','25','29','55','56','57','33','34','35') and    substr(ACPT_INS_ID_CD,7,3)<>'332')
                  set TASK_RESULT='A';
  update (select TASK_RESULT from mmgm.TBL_MMGM_AUDIT_TASK where 
      substr(ACQ_INS_ID_CD,3,4) ='4825'  and 
       substr(ACPT_INS_ID_CD,7,3) ='584' and    substr(ACPT_INS_ID_CD,7,3)<>'332')
     set TASK_RESULT='A';

      --王府井 北京银联商务 网银在线  和融通 恒信通 
 update (select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
               substr(ACPT_INS_ID_CD,7,2) <>'10' and    ACQ_INS_ID_CD   in ('0848031000','0848021000','0849911003'))
             set TASK_RESULT='A';
             
   update (select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
               substr(ACPT_INS_ID_CD,7,2) <>'10' and   substr( ACQ_INS_ID_CD,3,4) in ('4864','4873','4815'))
             set TASK_RESULT='A';           
      
       --申鑫电子 
 update (select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
     substr(ACQ_INS_ID_CD,3,4) ='4854'  and substr(ACPT_INS_ID_CD,7,2) <>'29')
     set TASK_RESULT='A';
     update (select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
     ACQ_INS_ID_CD ='0848542900'  and substr(ACPT_INS_ID_CD,7,2) <>'29')
     set TASK_RESULT='A';
     --拉卡拉
  update (  select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            substr(ACQ_INS_ID_CD,3,4)  ='4822' and substr(ACPT_INS_ID_CD,7,3)='332')
            set TASK_RESULT='A';
 --银联网络
 update ( select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
          ACQ_INS_ID_CD ='084025800' and substr(ACPT_INS_ID_CD,7,2) not in ('58','59','60'))
            set TASK_RESULT='A';
     --海南新生信息
 update (    select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            substr(ACQ_INS_ID_CD,3,4) in ('4852','4910')  and substr(ACPT_INS_ID_CD,7,2) not in('33','34','35','30','31','32','39','40','41','66','67','68','64','65'))
            set TASK_RESULT='A';
 --汇付数据
 update (  select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            substr(ACQ_INS_ID_CD,3,4) ='4823' and  substr(ACPT_INS_ID_CD,7,2) in
            ('70','71','72','55','56','57','79','80','81','49','50','51','33','34','35','73','74','75','76','52','53','54','40','41','87','24','25,','26','27','28','30','31','32','64','85','86','39','69') and  substr(ACPT_INS_ID_CD,7,3) not in ('393'  ))
            set  TASK_RESULT='A';
 update (  select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            substr(ACQ_INS_ID_CD,3,4) ='4823' and  substr(ACPT_INS_ID_CD,7,3) ='653')
            set  TASK_RESULT='A';
    --讯付
   update (    select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            ACQ_INS_ID_CD='0848131100' and   substr(ACPT_INS_ID_CD,7,2) not  in ('30','31','32','33','34','35','45','46','47','48','39','40','41','11'))
            set TASK_RESULT='A';
       
         --银结通 银联①易办事 易票联  银联金融
     update (   select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            substr(ACQ_INS_ID_CD,3,4)  =  '4841'  OR
             ACQ_INS_ID_CD  in ('0848455800','0848615840','0848615800','0848025840') and substr(ACPT_INS_ID_CD,7,2)  not in ('58','59','60'))
            set TASK_RESULT='A'; 

           --上海德颐
       update (          select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            substr(ACQ_INS_ID_CD,3,4)='4850' and  substr(ACPT_INS_ID_CD,7,2)   in ('36','37','38','85','86'))
            set TASK_RESULT='A';
            --富友
       update(     select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            substr(ACQ_INS_ID_CD,3,4)='4818'  and  substr(ACPT_INS_ID_CD,7,2)   in('34','35','39','40','41','11','42','43','44','24','25','55','56','57','33','49','50','51') and  substr(ACPT_INS_ID_CD,7,3) <>'332' )
            set TASK_RESULT='A';
            
            --现代金融控股
          update (   select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            substr(ACQ_INS_ID_CD,3,4)='4834' and substr(ACPT_INS_ID_CD,7,2)   in('24','25') )
            set TASK_RESULT='A';
            
               update (   select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            substr(ACQ_INS_ID_CD,3,4)='4834' and  substr(ACPT_INS_ID_CD,7,3)='452')
            set TASK_RESULT='A';
            
            --宁波银联商务
        update (    select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            ACQ_INS_ID_CD='0849917712'    and substr(ACPT_INS_ID_CD,7,3)  <>'332')
            set TASK_RESULT='A';
            
            --随行付
         update (      select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            substr(ACQ_INS_ID_CD,3,4)='4836'  and substr(ACPT_INS_ID_CD,7,2) in('24','25','22','23','33','34','35','39','40','41','26','27','28')  )
            set TASK_RESULT='A';
            --易联支付
            update (   select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
          substr ( ACQ_INS_ID_CD,3,4) ='4856' and   substr(ACPT_INS_ID_CD,7,2) not in ('16','17','18'))
            set TASK_RESULT='A';
            
            --汇卡商务
          update (  select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
          substr ( ACQ_INS_ID_CD,3,4) ='4866' and  substr(ACPT_INS_ID_CD,7,2) not in ('58','59','60'))
            set TASK_RESULT='A';
            --支付通
                update (    select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            ACQ_INS_ID_CD ='0848626500' and  substr(ACPT_INS_ID_CD,7,2) not in ('65','66','67','68'))
            set TASK_RESULT='A';
            --中汇电子
                         update (    select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
          substr ( ACQ_INS_ID_CD,3,4) ='4868' and substr(ACPT_INS_ID_CD,7,2)  in ('26','27','28','24','25','87','82','83','84','85','86','88','89','90','77','78','64') )
            set TASK_RESULT='A'; 
            
       update (    select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
          substr ( ACQ_INS_ID_CD,3,4) ='4868' and substr(ACPT_INS_ID_CD,7,3)  in ('584','393','332','222'))   set TASK_RESULT='A'; 
            
            
            --信汇电子
      update (       select TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            ACQ_INS_ID_CD ='0848815800' and substr(ACPT_INS_ID_CD,7,2) not in ('58','59','60'))
            set TASK_RESULT='A';
            --运达电子
        update (         select  TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            ACQ_INS_ID_CD = '0849913802' and substr(ACPT_INS_ID_CD,7,2) not in ('45','46','47','48'))
            set TASK_RESULT='A';
              update (         select  TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            substr(ACQ_INS_ID_CD,3,4)='4897'  and substr(ACPT_INS_ID_CD,7,2) not in ('45','46','47','48'))
            set TASK_RESULT='A';
            --瑞银信
          update (    select  * from  mmgm.TBL_MMGM_AUDIT_TASK where 
           substr ( ACQ_INS_ID_CD,3,4) ='4887' and  substr(ACPT_INS_ID_CD,7,2) in ('36','37','38','19','20','21','87'))
            set TASK_RESULT='A';
            --盛迪嘉
       update (     select  TASK_RESULT from  mmgm.TBL_MMGM_AUDIT_TASK where 
            substr ( ACQ_INS_ID_CD,3,4) ='4893'  and    substr(ACPT_INS_ID_CD,7,2)  not in ('10','29','58','59','60','64','61','62','63'))
            set TASK_RESULT='A';
     
             
            
