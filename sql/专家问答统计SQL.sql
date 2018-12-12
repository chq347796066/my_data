select {type} as type,-1 as group_id,null as group_name,-1 as user_id,null as name,sum(t.question_count) question_count,sum(t.answer_count) answer_count, sum(dur)/sum(t.answer_count) AS answer_dur,sum(t.listener_count) listener_count  
                from (
                        select 0 as listener_count,count(distinct t.question_id) question_count,count(t.answer_id) as answer_count,sum(dur) as dur from (
                            select a.question_id,a.user_id,a.group_id,b.id as answer_id,(case  when b.id is not null then (unix_timestamp(b.create_time)-unix_timestamp(a.create_time)) else 0 end) as dur  from (
                                select q.id as question_id,uq.user_id,q.group_id,q.create_time from zjs.question_info q
                                left join   zjs.user_question_relation uq on q.id=uq.question_id  
                                where q.`status`=0 and substr(q.create_time,1,10) >= '{startTime}' and substr(q.create_time,1,10) < '{endTime}'  and q.user_id !={officialUserId}
                            ) a
                            left join 
                            ( 
                                select create_time,user_id,question_id,id from zjs.answer_info where  substr(create_time,1,10) >= '{startTime}' and  substr(create_time,1,10) < '{answerEndTime}'
                            ) b  on b.question_id=a.question_id and a.user_id=b.user_id 
                        ) t
                     union all  
                        select count(a.id) listener_count ,0 as question_count,0 as answer_count ,0 as dur from zjs.listener a 
                        left join zjs.answer_info b on a.answer_id=b.id 
                        left join zjs.question_info c on c.id=b.question_id where  substr(a.create_time,1,10) >= '{startTime}' and  substr(a.create_time,1,10) < '{endTime}' and c.`status`=0 
                ) t

select {type} as type, -2 as group_id,null as group_name,t.user_id,u.name,sum(t.question_count) question_count,sum(t.answer_count) answer_count, sum(dur)/sum(t.answer_count) AS answer_dur,sum(t.listener_count) listener_count 
                from (
                        select 0 as listener_count,t.user_id,count(distinct t.question_id) question_count,count(t.answer_id) as answer_count,sum(dur) as dur from (
                            select a.question_id,a.user_id,a.group_id,b.id as answer_id,(case  when b.id is not null then (unix_timestamp(b.create_time)-unix_timestamp(a.create_time)) else 0 end) as dur  from (
                                select q.id as question_id,uq.user_id,q.group_id,q.create_time from zjs.question_info q
                                left join   zjs.user_question_relation uq on q.id=uq.question_id  
                                where q.`status`=0 and substr(q.create_time,1,10) >= '{startTime}' and substr(q.create_time,1,10) < '{endTime}'  and q.user_id !={officialUserId}
                            ) a
                            left join 
                            ( 
                                select create_time,user_id,question_id,id from zjs.answer_info where  substr(create_time,1,10) >= '{startTime}' and  substr(create_time,1,10) < '{answerEndTime}'
                            ) b  on b.question_id=a.question_id and a.user_id=b.user_id 
                        ) t group by t.user_id
                     union all  
                        select count(a.id) listener_count ,b.user_id,0 as question_count,0 as answer_count ,0 as dur from zjs.listener a 
                        left join zjs.answer_info b on a.answer_id=b.id 
                        left join zjs.question_info c on c.id=b.question_id where  substr(a.create_time,1,10) >= '{startTime}' and  substr(a.create_time,1,10) < '{endTime}' and c.`status`=0 
                       group by b.user_id
                ) t  
                left join zjs.user_info u on u.id = t.user_id group by t.user_id,u.name