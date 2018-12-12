insert overwrite table ODS_CCS5.{table} partition(dt='{endTime}')
            select 
            cast(sum(if(flag='auth_user',cnt,0)) as int) as auth_user,
            cast(sum(if(flag='reg_user',cnt,0)) as int) as reg_user,
            cast(sum(if(flag='listener',cnt,0)) as int)as listener,
            cast(sum(if(flag='listener_user',cnt,0)) as int) as listener_user,
            cast(sum(if(flag='question',cnt,0)) as int) as question,
            cast(sum(if(flag='question_user',cnt,0)) as int) as question_user,
            cast(sum(if(flag='answer',cnt,0)) as int) as answer,
            cast(sum(if(flag='answer_time',cnt,0)) as int) as answer_time,

            cast(sum(if(flag='settled_cdc_count',cnt,0)) as int) as settled_cdc_count,
            cast(sum(if(flag='settled_cdc_user',cnt,0)) as int) as settled_cdc_user,
            cast(sum(if(flag='cdc_course_use_count',cnt,0)) as int) as cdc_course_use_count, 
            cast(sum(if(flag='cdc_share_course_use_count',cnt,0)) as int) as cdc_share_course_use_count,
            cast(sum(if(flag='notification_count',cnt,0)) as int) as notification_count,
            cast(sum(if(flag='cdc_login_user',cnt,0)) as int) as cdc_login_user, 
            cast(sum(if(flag='certification_user',cnt,0)) as int) as certification_user,

            cast(sum(if(flag='register_active_user',cnt,0)) as int) as register_active_user ,
            cast(sum(if(flag='register_listen_user',cnt,0)) as int) as listen_register_user ,
            cast(sum(if(flag='active_user',cnt,0)) as int) as active_user ,
            {statTime} as stat_time

            from(
                    select 'auth_user' as flag,count(id) cnt  from zjs.user_info   where substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}"
                union all 
                    SELECT 'reg_user' as flag,COUNT(IF (b.type <> 0,1,NULL)) cnt
                        FROM
                        (
                            select * from zjs.user_register_info
                            where substr(register_time ,1,10)>="{startTime}" and substr(register_time ,1,10)<="{endTime}"
                        ) a
                        LEFT JOIN zjs.user_info b
                        ON a.user_id=b.id
                union all 
                    select 'listener' as flag,count(user_id) cnt from zjs.listener where substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}"
                union all 
                    select 'listener_user' as flag, count(DISTINCT user_id) cnt from zjs.listener where substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<='{endTime}'
                union all 
                    select 'question' as flag,count(user_id) cnt from zjs.question_info q left join  zjs.user_question_relation uq on q.id=uq.question_id where substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}" and status=0 and user_id != {officialUserId}
                union all
                    select 'question_user' as flag,count(DISTINCT user_id) cnt  from zjs.question_info q left join zjs.user_question_relation uq on q.id=uq.question_id where substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}" and status=0 and user_id != {officialUserId}
                union all 
                    select 'answer' as flag,count(question_id) cnt from zjs.answer_info an,zjs.question_info qi  where an.question_id=qi.id and  substr(an.create_time ,1,10)>="{startTime}" and substr(an.create_time ,1,10)<="{endTime}" and  qi.status=0 and an.user_id != 0
                union all 
                    select 'answer_time' as flag,floor(sum(answer_times) /count(id)) as cnt 
                        FROM
                        (
                            select q.id,(unix_timestamp(a.create_time)-unix_timestamp(q.create_time)) as  answer_times
                            from zjs.question_info q ,zjs.answer_info a where q.id = a.question_id and substr(a.create_time ,1,10)>="{startTime}" and substr(a.create_time ,1,10)<="{endTime}" and q.status=0 and a.user_id != 0
                        ) t0
                union all 
                    select 'cdc_login_user' as flag,count(distinct user_id) cnt  from zjs.oper_record where oper_type='10' and oper_plantform = 20 and substr(oper_time ,1,10)>="{startTime}" and substr(oper_time ,1,10)<="{endTime}"
                union all
                    select 'certification_user' as flag,count(distinct user_id) cnt from zjs.user_certification_log where type=40 and substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}"
                union all
                    select 'settled_cdc_count' as flag,count(id) cnt from zjs.hospital_info where is_enter=1 and substr(enter_time ,1,10)>="{startTime}" and substr(enter_time ,1,10)<="{endTime}" and type !=0 and is_del=0
                union all
                    select 'settled_cdc_user' as flag,count(DISTINCT user_id) cnt from zjs.user_register_info where substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}" and status=2
                union all
                    select 'cdc_course_use_count' as flag,count(*) cnt from zjs.jk_course where substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}" and type=10 and is_del=0
                union all
                    select 'cdc_share_course_use_count' as flag,count(*) cnt from zjs.jk_course where substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}" and type=20 and is_del=0
                union all
                    select 'notification_count' as flag,count(*) cnt from zjs.jk_notification where substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}" and hospital_id is not null

                union all
                    select "active_user" as flag,count(DISTINCT user_id) cnt from zjs.oper_record where substr(oper_time,1,10)>="{startTime}" and substr(oper_time,1,10)<="{endTime}"
                union all
                    select "register_active_user" as flag,count(DISTINCT a.user_id) cnt from zjs.oper_record a left join zjs.user_info u on a.user_id=u.id where substr(a.oper_time,1,10)>="{startTime}" and substr(a.oper_time,1,10)<="{endTime}" and  u.type!=0
                union all
                    select "register_listen_user" as flag,count(DISTINCT a.user_id) cnt from zjs.listener a left join zjs.user_info u on a.user_id=u.id where substr(a.create_time ,1,10)>="{startTime}" and substr(a.create_time ,1,10)<="{endTime}" and u.type!=0
            )t


 insert overwrite table ODS_CCS5.zjs_data_overview partition(dt='{statTime}')
        select 
        cast(sum(if(flag='auth_user',cnt,0)) as int) as auth_user,
        cast(sum(if(flag='reg_user',cnt,0)) as int) as reg_user,
        cast(sum(if(flag='listener',cnt,0)) as int) as listener,
        cast(sum(if(flag='listener_user',cnt,0)) as int) as listener_user,
        cast(sum(if(flag='question',cnt,0)) as int) as question,
        cast(sum(if(flag='question_user',cnt,0)) as int) as question_user,
        cast(sum(if(flag='answer',cnt,0)) as int)  as answer,
        cast(sum(if(flag='answer_time',cnt,0)) as int) as answer_time ,

        cast(sum(if(flag='cdc_login_user',cnt,0)) as int) as cdc_login_user, 
        cast(sum(if(flag='certification_user',cnt,0)) as int) as certification_user,
        cast(sum(if(flag='settled_cdc_count',cnt,0)) as int) as settled_cdc_count,
        cast(sum(if(flag='settled_cdc_user',cnt,0)) as int) as settled_cdc_user, 
        cast(sum(if(flag='cdc_course_use_count',cnt,0)) as int) as cdc_course_use_count, 
        cast(sum(if(flag='cdc_share_course_use_count',cnt,0)) as int) as cdc_share_course_use_count, 
        cast(sum(if(flag='notification_count',cnt,0)) as int) as notification_count,
        {statDay} as stat_time

        from(
                select 'auth_user' as flag,count(id) cnt  from zjs.user_info  where substr(create_time,1,10)<='{statTime}'
            union all 
                select 'reg_user' as flag,count(id) cnt from zjs.user_info where substr(create_time,1,10)<='{statTime}' and type<>0 
            union all 
                select 'listener' as flag,count(user_id) cnt from zjs.listener where substr(create_time,1,10)<='{statTime}'
            union all 
                select 'listener_user' as flag, count(DISTINCT user_id) cnt from zjs.listener where substr(create_time,1,10)<='{statTime}'
            union all 
                select 'question' as flag,count(user_id) cnt from zjs.question_info q left join zjs.user_question_relation uq on q.id=uq.question_id where substr(create_time,1,10)<='{statTime}' and status=0 and user_id != {officialUserId}
            union all
                select 'question_user' as flag,count(DISTINCT user_id) cnt  from zjs.question_info q left join zjs.user_question_relation uq on q.id=uq.question_id where substr(create_time,1,10)<='{statTime}' and  status=0 and user_id != {officialUserId}
            union all 
                select 'answer' as flag,count(question_id) cnt from zjs.answer_info an,zjs.question_info qi  where an.question_id=qi.id and  substr(an.create_time,1,10) <= '{statTime}' and  qi.status=0 and an.user_id != 0
            union all 
                select 'answer_time' as flag,floor(sum(answer_times) /count(id)) as cnt 
                FROM
                (
                    select q.id,(unix_timestamp(a.create_time)-unix_timestamp(q.create_time)) as  answer_times
                    from zjs.question_info q ,zjs.answer_info a where q.id = a.question_id and substr(a.create_time,1,10)<='{statTime}' and q.status=0 and a.user_id != 0
                ) t0
            union all 
                select 'cdc_login_user' as flag,count(id) cnt  from zjs.oper_record where oper_type='10' and oper_plantform = 20 and substr(oper_time,1,10)<='{statTime}'
            union all
                select 'certification_user' as flag,count(id) cnt from zjs.user_info  where is_del=0 and  certification_status=40 and substr(create_time,1,10)<='{statTime}'
            union all
                select 'settled_cdc_count' as flag,count(id) cnt from zjs.hospital_info where is_enter=1 and substr(enter_time,1,10)<='{statTime}' and type !=0 and is_del=0
            union all
                select 'settled_cdc_user' as flag,count(DISTINCT user_id) cnt from zjs.user_register_info where substr(create_time,1,10)<='{statTime}' and status=2
            union all
                select 'cdc_course_use_count' as flag,count(*) cnt from zjs.jk_course where substr(create_time,1,10)<='{statTime}' and type=10 and is_del=0
            union all
                select 'cdc_share_course_use_count' as flag,count(*) cnt from zjs.jk_course where substr(create_time,1,10)<='{statTime}' and type=20 and is_del=0
            union all
                select 'notification_count' as flag,count(*) cnt from zjs.jk_notification where substr(create_time,1,10)<='{statTime}' and hospital_id is not null
        )t