 insert overwrite table  ODS_CCS5.zjs_data_total partition(dt='{statTime}')
        select 
        h.region_id ,h.name,tt.hospital_id,reg_cnt,injkuser_count,
        listen_count,listen_user_count,question_info_count,question_user_count,
        rm.city_id ,rm.province_id,
        nvl(course_use.course_use_count,0) as course_use_count,#疾控培训课程使用数   清除type=10
        0 as share_course_use_count,#疾控共享课程使用数   删除
        nvl(notification.notification_count,0) as notification_count,
        {time} as stat_time
        from (
                select 
                    hospital_id,#
                    count(distinct id) as reg_cnt,#注册用户数
                    count(distinct (case when status = 2 then id else null end)) as injkuser_count,#入驻疾控人数
                    count(distinct listen_id) listen_count ,#收听量
                    count(distinct listen_user_id) listen_user_count ,#收听用户数
                    count(distinct question_id) question_info_count ,#提问数量  去除屏蔽提问数，未推送给专家问题
                    count(distinct question_user_id) question_user_count#提问用户数  去除屏蔽提问数，未推送给专家问题
                from 
                (
                     select b.id, #用户id 
                     b.hospital_id,
                     b.status,
                     question.id as question_id,
                     question.user_id as question_user_id,
                     listen.id as listen_id,
                     listen.user_id as listen_user_id                         
                        from (
                            select r.user_id id,r.hospital_id,r.status from zjs.user_register_info r 
                            inner join ( select * from zjs.user_info where type<>0) u on r.user_id = u.id where substr(r.register_time,1,10)<='{statTime}'
                        ) b
                    left join (select q.id,q.user_id from zjs.question_info q left join zjs.user_question_relation uq on q.id=uq.question_id where substr(q.create_time,1,10)<='{statTime}' and q.user_id !={officialUserId}) question on b.id = question.user_id
                    left join (select zl.id,zl.user_id from zjs.listener zl where substr(zl.create_time,1,10)<='{statTime}') listen on b.id = listen.user_id
                    
                ) t
            group by hospital_id
        ) tt
        left join 
            (
                select count(distinct id) as course_use_count , hospital_id from zjs.jk_course course  
                where substr(course.create_time,1,10)<='{statTime}' and course.is_del=0 group by hospital_id
            ) course_use on course_use.hospital_id=tt.hospital_id
       # left join 
         #   (
         #       select count(distinct id) as share_course_use_count,hospital_id as hospital_id  from  zjs.jk_course course
          #      where substr(course.create_time,1,10)<='{statTime}' and course.type=20 and course.is_del=0 group by hospital_id
            #) course_share on course_share.hospital_id=tt.hospital_id
        left join (
                select count(distinct id) as notification_count,hospital_id as hospital_id  from zjs.jk_notification notification
                where substr(notification.create_time,1,10)<='{statTime}' group by hospital_id
            ) notification on notification.hospital_id=tt.hospital_id
        left join 
        zjs.hospital_info h
        on tt.hospital_id = h.id
        left join org_doumiao.region_map rm
        on h.region_id=rm.region_id


#原有SQL
 insert overwrite table  ODS_CCS5.zjs_data_total partition(dt='{statTime}')
        select 
        h.region_id ,h.name,tt.hospital_id,reg_cnt,injkuser_count,
        listen_count,listen_user_count,question_info_count,question_user_count,
        rm.city_id ,rm.province_id,
        nvl(course_use.course_use_count,0) as course_use_count,
       
        nvl(notification.notification_count,0) as notification_count,
        {time} as stat_time
        from (
                select 
                    hospital_id,
                    count(distinct id) as reg_cnt,
                    count(distinct (case when status = 2 then id else null end)) as injkuser_count,
                    count(distinct listen_id) listen_count ,
                    count(distinct listen_user_id) listen_user_count ,
                    count(distinct question_id) question_info_count ,
                    count(distinct question_user_id) question_user_count
                from 
                (
                     select b.id,b.hospital_id,b.status,question.id as question_id,question.user_id as question_user_id,listen.id as listen_id,listen.user_id as listen_user_id
                        from (
                            select r.user_id id,r.hospital_id,r.status from zjs.user_register_info r 
                            inner join ( select * from zjs.user_info where type<>0) u on r.user_id = u.id where substr(r.register_time,1,10)<='{statTime}'
                        ) b
                    left join (select * from zjs.question_info where substr(create_time,1,10)<='{statTime}') question on b.id = question.user_id
                    left join (select * from zjs.listener where substr(create_time,1,10)<='{statTime}') listen on b.id = listen.user_id
                    
                ) t
            group by hospital_id
        ) tt
        left join F
            (
                select count(distinct id) as course_use_count , hospital_id from zjs.jk_course course  
                where substr(course.create_time,1,10)<='{statTime}' and course.type=10 and course.is_del=0 group by hospital_id
            ) course_use on course_use.hospital_id=tt.hospital_id
        left join 
            (
                select count(distinct id) as share_course_use_count,hospital_id as hospital_id  from  zjs.jk_course course
                where substr(course.create_time,1,10)<='{statTime}' and course.type=20 and course.is_del=0 group by hospital_id
            ) course_share on course_share.hospital_id=tt.hospital_id
        left join (
                select count(distinct id) as notification_count,hospital_id as hospital_id  from zjs.jk_notification notification
                where substr(notification.create_time,1,10)<='{statTime}' group by hospital_id
            ) notification on notification.hospital_id=tt.hospital_id
        left join 
        zjs.hospital_info h
        on tt.hospital_id = h.id
        left join org_doumiao.region_map rm
        on h.region_id=rm.region_id