 insert overwrite table  ODS_CCS5.xdm_children_by_current_hospital partition(dt='%s')
        select max(hi.region_id),ttt.hospital_id,max(hi.name),max(hi.code),ttt.user_entry,
            sum(ttt.new_count) new_child_count,
            sum(age_0) age_0,
            sum(age_1) age_1,
            sum(age_2) age_2,
            sum(age_3) age_3,
            sum(age_4) age_4,
            sum(if(ttt.data_type='real',ttt.new_count,0)) real_new_child_count,
            sum(if(ttt.data_type='real',age_0,0)) real_age_0,
            sum(if(ttt.data_type='real',age_1,0)) real_age_1,
            sum(if(ttt.data_type='real',age_2,0)) real_age_2,
            sum(if(ttt.data_type='real',age_3,0)) real_age_3,
            sum(if(ttt.data_type='real',age_4,0)) real_age_4,
            max(rm.city_id) ,max(rm.province_id) 
        from 
        (
            select 'real' data_type,hospital_id,user_entry, count(distinct fchildno) new_count,
              count(distinct if(age<=0,fchildno,null)) age_0,
              count(distinct if(age=1,fchildno,null) ) age_1,
              count(distinct if(age=2,fchildno,null) ) age_2,
              count(distinct if(age=3,fchildno,null) ) age_3,
              count(distinct if(age>=4,fchildno,null) ) age_4
            from 
            (
                select hospital_id,fchildno,age, 
                case when LOWER(app_platform)='wechat' 
                and user_register_type=3 then 1
                when user_register_type=5 then 2 
                else 0 end as user_entry 
                from 
                ( 
                SELECT   cl.user_id,c.hospital_id, c.fchildno,FLOOR(DATEDIFF( '%s',c.birthday )/365.2422) as age ,cl.user_register_type ,ra.app_platform 
                from (select * from child where  substr(create_time,1,10)<='%s')  c
                left join (select user_id,child_id,max(user_register_type) user_register_type from child_log where child_id is not null group by user_id,child_id) cl
                on c.id=cl.child_id 
                left join 
                    ( 
                    select r.app_platform,a.user_id from 
                    user_register_log r 
                    left join user_auth a 
                    on r.user_auth_id = a.id 
                    ) ra 
                    on ra.user_id = cl.user_id  
                ) t
            ) tt
            group by hospital_id,user_entry 
          union all 
            select 'virtual' data_type,hospital_id,user_entry, count(1) new_count,
                        sum(if(age=0,1,0)) age_0,
                        sum(if(age=1,1,0)) age_1,
                        sum(if(age=2,1,0)) age_2,
                        sum(if(age=3,1,0)) age_3,
                        sum(if(age>=4,1,0)) age_4
            from 
            (
                select hospital_id,age, 
                case when LOWER(app_platform)='wechat' 
                and user_register_type=3 then 1 
                when user_register_type=5 then 2 
                else 0 end as user_entry 
                from 
                ( 
                    SELECT   cl.hospital_id, FLOOR(DATEDIFF( '%s',cl.birthday )/365.2422) as age ,cl.user_register_type ,ra.app_platform 
                    from (
                        select user_id,id, hospital_id,region_id,birthday,user_register_type
                        from (
                            select  user_id,id, hospital_id,region_id,birthday,user_register_type,
                              ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY id ) AS rn
                              from
                                 (
                                 select  c.user_id,v.id,v.hospital_id,v.region_id,v.birthday,c.user_register_type 
                                 from 
                                 (select * from v_child  where  substr(create_time,1,10)<='%s' and child_id is null  and is_del=0 )  v 
                                 left join (select user_id,v_child_id,max(user_register_type) user_register_type from child_log where child_id is null group by user_id,v_child_id) c 
                                 on v.id=c.v_child_id
                                 ) t
                        ) tt where tt.rn=1 
                    ) cl
                left join 
                    ( 
                    select r.app_platform,a.user_id from 
                    user_register_log r 
                    left join user_auth a 
                    on r.user_auth_id = a.id 
                    ) ra 
                    on ra.user_id = cl.user_id 
                    where not exists (
                       select 1 from relate_region region where cl.region_id=region.region_id
                    )
                ) t 
            ) tt
            group by hospital_id,user_entry 
        ) ttt 
        left join 
        hospital_info hi
        on ttt.hospital_id = hi.id
        left join org_doumiao.region_map rm
        on hi.region_id=rm.region_id
        where ttt.hospital_id is not null
        group by ttt.hospital_id,ttt.user_entry