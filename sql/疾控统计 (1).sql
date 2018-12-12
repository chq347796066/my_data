按天 周  月


select 	
		a.id,
		hi.type,	    
	    rm.region_id ,
	    rm.province_id,
	   	rm.city_id,	 
		nvl(b.allowed_hospital_cnt,0),
		nvl(c.allowed_jk_user,0),
		nvl(d.jk_course_usecnt,0),		
		nvl(f.notification_cnt,0) 
		from 
               (select id  from zjs.hospital_info where type in (1,2,3) and substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}" group by id) a
        left join (
	     	select s_id,(count(distinct shi_id) + count(id)+count(distinct s_id)) as allowed_hospital_cnt from 
			(select id as shi_id,parent_id as s_id from hospital_info where is_enter= 1 and is_del=0 and is_included =1 and type = 2 and substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}" group by parent_id,id) a 

			left join
			(select id,parent_id from hospital_info where is_enter= 1 and is_del=0 and is_included =1 and type = 1 and substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}" group by parent_id,id) b
			on a.shi_id=b.parent_id group by s_id

			union 
			select parent_id as s_id,count(distinct id) as allowed_hospital_cnt from hospital_info where is_enter= 1 and is_del=0 and is_included =1 and type = 1 and substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}" group by parent_id

		) b on a.id=b.s_id	

		left join
		(
			select id,count(distinct user_id) as allowed_jk_user from 
			(select id,path from zjs.hospital_info where is_enter =1 and is_del =0 and is_included =1 and substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}") a 
			left join 
			(select hospital_path,user_id from user_register_info where `status` =2 and substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}") b on (true) where LOCATE(a.path,b.hospital_path) >0
			group by id
		)	c on a.id=c.id

		left join
		(
			select id,count(jk_id) as jk_course_usecnt from
			(select id from hospital_info where is_del = 0 and is_included =1 and substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}") h
			INNER join
			(select id as jk_id,hospital_id from jk_course where substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}") f

			on h.id= f.hospital_id
			group by id
		) d on a.id = d.id

  		left join
  		(
  			select id,count(jk_noti_id) as notification_cnt from 
  			(select id from hospital_info where is_del =0 and is_included =1 and substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}") a 
  			inner join 
  			(select id as jk_noti_id,hospital_id from jk_notification where substr(create_time ,1,10)>="{startTime}" and substr(create_time ,1,10)<="{endTime}" and hospital_id is not null) b
  			on a.id = b.hospital_id
  			group by id
  		) f on a.id =f.id

  		left join  
  			hospital_info hi
  		on a.id= hi.id
  		left join org_doumiao.region_map rm
  		on hi.region_id =rm.region_id

 总计

select 	
		a.id,
		hi.type,	    
	    rm.region_id ,
	    rm.province_id,
	   	rm.city_id,	 
		nvl(b.allowed_hospital_cnt,0),
		nvl(c.allowed_jk_user,0),
		nvl(d.jk_course_usecnt,0),		
		nvl(f.notification_cnt,0) 
		from 
               (select id  from zjs.hospital_info where type in (1,2,3) and substr(create_time,1,10)<='{statTime}' group by id) a
        left join (
	     	select s_id,(count(distinct shi_id) + count(id)+count(distinct s_id)) as allowed_hospital_cnt from 
			(select id as shi_id,parent_id as s_id from hospital_info where is_enter= 1 and is_del=0 and is_included =1 and type = 2 and substr(create_time,1,10)<='{statTime}' group by parent_id,id) a 

			left join
			(select id,parent_id from hospital_info where is_enter= 1 and is_del=0 and is_included =1 and type = 1 and substr(create_time,1,10)<='{statTime}' group by parent_id,id) b
			on a.shi_id=b.parent_id group by s_id

			union 
			select parent_id as s_id,count(distinct id) as allowed_hospital_cnt from hospital_info where is_enter= 1 and is_del=0 and is_included =1 and type = 1 and substr(create_time,1,10)<='{statTime}' group by parent_id

		) b on a.id=b.s_id	

		left join
		(
			select id,count(distinct user_id) as allowed_jk_user from 
			(select id,path from zjs.hospital_info where is_enter =1 and is_del =0 and substr(create_time,1,10)<='{statTime}' and is_included =1 ) a 
			left join 
			(select hospital_path,user_id from user_register_info where `status` =2 and substr(create_time,1,10)<='{statTime}') b on (true) where LOCATE(a.path,b.hospital_path) >0
			group by id
		)	c on a.id=c.id

		left join
		(
			select id,count(jk_id) as jk_course_usecnt from
			(select id from hospital_info where is_del = 0 and is_included =1 and substr(create_time,1,10)<='{statTime}') h
			INNER join
			(select id as jk_id,hospital_id from jk_course where substr(create_time,1,10)<='{statTime}') f

			on h.id= f.hospital_id
			group by id
		) d on a.id = d.id

  		left join
  		(
  			select id,count(jk_noti_id) as notification_cnt from 
  			(select id from hospital_info where is_del =0 and is_included =1 and substr(create_time,1,10)<='{statTime}') a 
  			inner join 
  			(select id as jk_noti_id,hospital_id from jk_notification where substr(create_time,1,10)<='{statTime}' and hospital_id is not null) b
  			on a.id = b.hospital_id
  			group by id
  		) f on a.id =f.id

  		left join  
  			hospital_info hi
  		on a.id= hi.id
  		left join org_doumiao.region_map rm
  		on hi.region_id =rm.region_id


		 

	先求省下面的市疾控总量  																							#省id    市id
		select id,parent_id from hospital_info where is_enter= 1 and is_del=0 and is_included =1 and type = 2 group by parent_id,id a
		
		left join 
	再求市对应的区下面的疾控总量   																						#市id，区id
		select id,parent_id from hospital_info where is_enter= 1 and is_del=0 and is_included =1 and type = 1 group by parent_id,id b
		on a.id=b.parent_id
	然后求和
		怎么联合起来求和？

	     left join (
     	select s_id,(count(distinct shi_id) + count(id)+count(distinct s_id)) as all_hospitall from 
		(select id as shi_id,parent_id as s_id from hospital_info where is_enter= 1 and is_del=0 and is_included =1 and type = 2 group by parent_id,id) a 

		left join
		(select id,parent_id from hospital_info where is_enter= 1 and is_del=0 and is_included =1 and type = 1 group by parent_id,id) b
		on a.shi_id=b.parent_id group by s_id

		union all
		select parent_id as s_id,count(distinct id) as all_hospitall from hospital_info where is_enter= 1 and is_del=0 and is_included =1 and type = 1 group by parent_id

		) b on a.id=b.s_id			

SELECT * FROM table1 RIGHT JOIN table2 ON(TRUE) WHERE LOCATE(table1.y,table2.x)>0

select id,count(distinct user_id) from 
	(select id,path from zjs.hospital_info where is_enter =1 and is_del =0 and is_included =1 ) a 
	left join 
	(select hospital_path,user_id from user_register_info where `status` =2) b on (true) where LOCATE(a.path,b.hospital_path) >0