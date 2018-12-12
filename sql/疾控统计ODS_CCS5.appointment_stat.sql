 insert overwrite table  ODS_CCS5.appointment_stat partition(dt='%s')  
        select  cast(t2.region_id as int) region_id,cast(t1.hospital_id as int) hospital_id,t2.name,t2.code,
        t1.cr_child_cnt,t1.cr_total,t1.cr_success_cnt, t1.cr_keep_cnt,t1.cr_break_cnt,t1.cr_cancel_cnt,
        t1.d_child_cnt,t1.d_total,t1.d_success_cnt, t1.d_keep_cnt,t1.d_break_cnt,t1.d_cancel_cnt,
        cast(rm.city_id as int) city_id,cast(rm.province_id as int) province_id
        from (
        select hospital_id,
        sum(if(type='create_time',child_cnt,0)) cr_child_cnt,
        sum(if(type='create_time',total,0)) cr_total,
        sum(if(type='create_time',success_cnt,0)) cr_success_cnt,
        sum(if(type='create_time',keep_cnt,0)) cr_keep_cnt,
        sum(if(type='create_time',break_cnt,0)) cr_break_cnt,
        sum(if(type='create_time',cancel_cnt,0)) cr_cancel_cnt,
        sum(if(type='real_date',child_cnt,0)) d_child_cnt,
        sum(if(type='real_date',total,0)) d_total,
        sum(if(type='real_date',success_cnt,0)) d_success_cnt,
        sum(if(type='real_date',keep_cnt,0)) d_keep_cnt,
        sum(if(type='real_date',break_cnt,0)) d_break_cnt,
        sum(if(type='real_date',cancel_cnt,0)) d_cancel_cnt
        from(
          select 'create_time' type ,hospital_id,count(DISTINCT child_id ) child_cnt, count(1) total,
          sum(if(status=1,1,0)) success_cnt, 
          sum(if(status=5 or status=3 ,1,0)) keep_cnt, 
          sum(if(status=6,1,0)) break_cnt, 
          sum(if(status=4,1,0)) cancel_cnt 
          from org_doumiao.hospital_appointment_log 
          where substr(create_time,1,10)='%s'
          group by hospital_id
          union all
          select 'real_date' type ,hospital_id,count(DISTINCT child_id ) child_cnt, count(1) total,
          sum(if(status=1,1,0)) success_cnt,  
          sum(if(status=5 or status=3 ,1,0)) keep_cnt,
          sum(if(status=6,1,0)) break_cnt,   
          sum(if(status=4,1,0)) cancel_cnt 
          from org_doumiao.hospital_appointment_log 
          where substr(`date`,1,10)='%s'
          group by hospital_id
        ) t0 group by hospital_id   
        ) t1 
        left join org_doumiao.hospital_info t2
        on t1.hospital_id = t2.id
        left join org_doumiao.region_map rm
        on t2.region_id=rm.region_id 