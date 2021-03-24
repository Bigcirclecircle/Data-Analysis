
1 
# Write your MySQL query statement below
select player_id, min(event_date) as first_login
from Activity 
group by player_id

2

select player_id, device_id
from (select player_id,device_id,dense_rank() over
(partition by player_id order by event_date asc
) rk from activity ) a 
where a.rk =1

3

select player_id, event_date,sum(games_played) over (partition by player_id order by event_date) as games_played_so_far from activity

4 
select round(sum(if (datediff(event_date,min_date)=1,1,0))/(count(distinct player_id )),2) as fraction
from 
(
select player_id,event_date,min(event_date) over (partition by player_id order by event_date) as min_date
from activity 
) temp


5
select min_date as install_dt,count(distinct player_id) as installs,
round(sum(if (datediff(event_date,min_date)=1,1,0))/count(distinct player_id),2) as Day1_retention
from (
select  player_id,event_date,min(event_date) over(partition by player_id order by event_date) as min_date
from Activity ) temp_tb 
group by min_date

6

select 
ifnull (round(
( 
select 
count(distinct requester_id ,accepter_id ) from requestaccepted )  / 
(select 
count(distinct sender_id ,send_to_id)
from friendrequest
) ,2),0)as accept_rate

7
select id,count(id) as num from (
select requester_id as id
from request_accepted
union all
select accepter_id as id
from request_accepted) mergeaxi0
group by id
order by num desc 
limit 1

8


select product_name , year ,price 

from sales s join product p on s.product_id = p.product_id

order by year desc

9

select product_id ,sum(quantity)  as total_quantity
from sales 
group by product_id


10 

select product_id,year as first_year,quantity,price from 
(
select product_id,year,quantity,price , dense_rank() over (partition by product_id order by year  ) as ranks 

from sales
) temp 
where ranks =1 


11

select project_id , round(avg(experience_years),2) as average_years
from project p  ,employee e
where e.employee_id = p.employee_id
group by project_id


12 

select project_id
from project
group by project_id 
having count(employee_id) >= all(select count(employee_id) from project group by project_id)


13 

select project_id,employee_id from (
select  project_id,p.employee_id ,dense_rank() over (partition by project_id order by experience_years desc ) as ranks 
from project p ,employee e
where p.employee_id = e.employee_id
)temp 
where ranks  = 1

14 
select seller_id from (
select seller_id, dense_rank() over (order by sum(price) desc) ranks 
from sales 
group by seller_id )temp 
where ranks = 1

15

select buyer_id 
from product p ,sales s 
where p.product_id = s.product_id
group by s.buyer_id
having sum(p.product_name = 'S8')> 0 
and sum(p.product_name  = 'iphone') = 0

16 

select product_id, product_name
from sales  join product 
using(product_id)
group by product_id
having 
sum( sale_date between date_format('2019-01-01'
,'%Y-%m-%d') and date_format('2019-03-31'
,'%Y-%m-%d')) =  count(sale_date)


17 

select distinct author_id as id 
from views 
where author_id = viewer_id 
order by author_id

18 


select distinct viewer_id as id 
from views
group by view_date, viewer_id
having count(distinct article_id) >=2 
order by viewer_id


19


select Id,Company,Salary from (
select id,company,salary,row_number() over (partition by company order by salary ) as ranks,count(salary) over(partition by company ) as countx from employee ) temp 
where ranks in  ( countx/2 ,countx/2+1 , countx/2+0.5 )


20 

select Name from employee
where id in (

select managerid 
from employee
group by managerid 
having count(managerid) >= 5 
)


21 

select avg(number) as median
from (
select number ,@c1   as 'c1',(@c1:= @c1+frequency) as 'c2',t2.sumx 
from numbers ,(select @c1 := 0 ) t1 ,
(select sum(frequency ) as sumx from numbers ) t2
order by number ) temp 
where sumx/2 between c1 and c2



21

select name 
from candidate c  join vote v
on c.id = v.candidateid
group by candidateid
order by  count(candidateid) desc
limit 1


22

select name , bonus 
from employee left join bonus 
using (empid) 
where ifnull(bonus,0) < 1000


23

select question_id as survey_log
from survey_log
group by question_id 
order by 
sum(if (action = 'answer',1,0)) /
sum(if (action = 'show',1,0)) desc
limit 1







24


select id,month,Salary from (
select id,month, sum(salary) over (partition by id order by month rows 2 preceding ) as Salary , rank() over(partition by id order by month desc) as ranks 
from employee ) temp 

where ranks  > 1 
order by id asc , month desc


25

select dept_name ,ifnull(count(student_id),0) as student_number 
from   department left join student
using(dept_id)
group by dept_id
order by student_number desc , dept_name asc


25

select name 
from customer 
where ifnull(referee_id,0) <> 2

26

select sum(TIV_2016) as TIV_2016
from (
select TIV_2016 ,count(TIV_2015) over (partition by TIV_2015) cx,count(concat(LAT,'$',LON)) over (partition by concat(LAT,'$',LON)) ll
from insurance 
) temp
where cx <> 1 and ll = 1



27 

select customer_number 
from orders 
group by customer_number 
order by count(customer_number) desc
limit 1


28 

  变量法 ：
  select seat_id from (
  select seat_id , sum(a2) over (order by seat_id rows between 0 preceding and 1 following) label
  from 
  (
  select seat_id , @a as 'a1', (@a:=if(free=1,ifnull(@a,0)+1,0))as 'a2'
  from cinema
  )temp 
  )t2  
  where label > 1
  
  窗函数： 
  
  select seat_id from (
  select *,ifnull(lead(free) over (order by seat_id),0) +free as leadx, ifnull(lag(free) over (order by seat_id) ,0)+free as lagx from cinema
  )temp  where (leadx+lagx) > 2
  
  29 
  
  select name 
  from salesperson 
  where sales_id not in 
  (
  select sales_id 
  from orders 
  where com_id = (select com_id from company where name= 'RED') 
  )


30 

select Id,  (case when p_id is null then 'Root' when id not in (select ifnull(p_id,0) from tree) then 'Leaf'  else 'Inner' end ) as Type
from tree

31 

select * , if (x+y > z and abs(x-y)<z ,'Yes','No') as triangle 
from triangle


32 

select round(min(sqrt(power(a.x-b.x,2)+power(a.y-b.y,2))),2)as shortest
from point_2d a join point_2d b  
where  (a.x,a.y) <> (b.x,b.y)

33

select min(abs(a.x - b.x)) shortest
from point a join point b
where a.x <> b.x

34 

select  a.follower ,count(distinct b.follower) num
from follow a join follow b 
where a.follower = b.followee 
group by a.follower
order by a.follower

35 

select distinct date_format(pay_date,'%Y-%m') as pay_month,department_id,
case when dav > av then 'higher' when dav < av then 'lower' else 'same' end  as comparison
from 
(
select *,avg(amount) over (partition by pay_date) av,
avg(amount) over (partition by department_id,pay_date) dav
from 
salary join employee
using(employee_id))temp

order by pay_date desc


36 
变量法

select 
max(case continent when 'America' then name else null end) America,
max(case continent when 'Asia' then name else null end) Asia,
max(case continent when 'Europe' then name else null end) Europe
from
(select name,continent,
if(@tmp=continent,@rownum:=@rownum+1,@rownum:=1) as rnk,@tmp:=continent from student s1,(select @tmp:=0,@rownum:=1) r1
order by continent,name asc) t
group by rnk

常规

select 
max(case continent when 'America' then name else null end)as America,
max(case continent when 'Asia' then name else null end)
as Asia,
max(case continent when 'Europe' then name else null end)
as Europe
from 
(select name,continent,row_number() over (partition by continent order by name) ranks from student ) t 
group by ranks 


37 

select max(num) as num 
from (
select num, count(num) over (partition by num) cx
from my_numbers) tmp
where cx =1

38 

select customer_id
from customer
group by (customer_id)
having count(distinct product_key) = (select count(distinct product_key) from product)


39

select actor_id  ACTOR_ID ,director_id DIRECTOR_ID
from ActorDirector
group by actor_id ,director_id
having count(*) >= 3

40 

select b.book_id,b.name
from books b left join orders o on ( b.book_id = o.book_id and 
o.dispatch_date >= date_sub(date_format('2019-06-23','%Y-%m-%d'), interval 1  year ))
where available_from < date_sub(date_format('2019-06-23','%Y-%m-%d'), interval 1  month )
group by book_id
having sum(ifnull(quantity,0)) < 10

41 


select login_date,count(distinct user_id) as user_count 
from (
select min(activity_date) as login_date,user_id
from traffic 
where activity = 'login'
group by user_id
having datediff('2019-06-30',login_date) <= 90
) t 

group by login_date

42 

select student_id,course_id,grade  from 
(
select *,dense_rank() over(partition by student_id order by grade desc ,course_id asc) label 
from enrollments 
) t 
where label = 1

43 

select  extra as report_reason , count(distinct post_id)
from actions 　
where action = 'report' and datediff('2019-07-05',action_date) = 1 
group by report_reason

44



select business_id 
from (

select business_id,occurences ,avg(occurences) over (partition by event_type ) as f
from events 
) t 

group by business_id
having sum(occurences > f) >= 2 

45 

select t1.spend_date, t1.platform, 
ifnull(sum(amount), 0) as total_amount,
ifnull(count(distinct user_id), 0) as total_users
from 
(
select distinct spend_date , 'desktop' as platform
from spending 
union
select distinct spend_date , 'mobile' as platform
from spending 
union
select distinct spend_date , 'both' as platform
from spending ) t1

left join 

(  select  spend_date,user_id ,sum(amount) as amount,
case count(*) when  1  then platform else 'both' end  as platform  from spending 
group by spend_date,user_id ) t2
on t1.spend_date = t2.spend_date and t1.platform = t2.platform

group by t1.spend_date, t1.platform

46

select round (avg(y/x)*100 ,2 )  as average_daily_percent from 
(
select  count(distinct a.post_id) x , count(distinct m.post_id) y 

from actions a  left join removals  m on a.post_id = m.post_id
where extra = 'spam'
group by action_date
) t1 

47 

select activity_date as day, count(distinct user_id) as active_users
from activity 
where  datediff('2019-07-27',activity_date)< 30
group by activity_date

48

select ifnull(round(sum(x)/count(user_id),2),0) as average_sessions_per_user
from 
(
select user_id,count(distinct session_id) x
from activity 
where datediff('2019-07-27',activity_date )< 30 
group by user_id) t


49 


select user_id as buyer_id, join_date ,ifnull(orders_in_2019,0) as orders_in_2019
from users left join (

select buyer_id,count(item_id) as orders_in_2019
from orders 
where date_format(order_date,'%Y') = 2019
group by buyer_id ) t 

on buyer_id = user_id

50 

select user_id seller_id, if(favorite_brand = item_brand, 'yes', 'no') 2nd_item_fav_brand
from users left join (
select seller_id, item_brand
from (
select o1.seller_id, o1.item_id
from orders o1 join orders o2
on o1.seller_id = o2.seller_id
group by o1.order_id
having sum(o1.order_date > o2.order_date) = 1
) o join items i
on o.item_id = i.item_id
) tmp
on user_id = seller_id

51 

select distinct p.product_id,
if (t.product_id is null ,10,t.new_price) price 
from 
products p left join 
(
select new_price,product_id
from products
where (product_id,change_date) in 

(select product_id , max(change_date) as dt
from products 
where change_date < '2019-08-17'
group by product_id )) t 

on p.product_id = t.product_id


52 


select 
round(sum(if( order_date = customer_pref_delivery_date,1,0))/ count(delivery_id)*100,2) as immediate_percentage
from delivery

53 


select
round(sum(if (order_date =customer_pref_delivery_date,1,0))/ count(delivery_id)*100
,2) immediate_percentage 

from delivery
where (customer_id,order_date) in (
select customer_id ,min(order_date) dt 
from delivery 
group by customer_id )

54 

country , 
count(amount) as trans_count,
sum(if(state = 'approved',1,0)) as approved_count,
sum(amount) as trans_total_amount , 
sum(if(state = 'approved',amount,0)) as approved_total_amount

from transactions 
group by month, country

55

select group_id,player_id
from (
select players.* ,sum(if(player_id = first_player,first_score,second_score)) as sc
from players join matches
on player_id = first_player 
or player_id = second_player
group by player_id 
order by sc desc, player_id
) t 
group by group_id


56

select person_name , if (sum(weight) over (order by turn ) <= 1000 ,turn,null) label 
from queue
order by label desc
limit 1


57


select date_format(trans_date,'%Y-%m') as month ,
country , 
sum(if(state = 'approved',1,0)) as approved_count,
sum(if(state = 'approved',amount,0)) as approved_amount,
sum(if(state = 'chargebacked',1,0)) as chargeback_count,
sum(if(state = 'chargebacked',amount,0)) as chargeback_amount

from (
select * from Transactions
union all 
select id, country,'chargebacked' as  state , amount,Chargebacks.trans_date
from  Chargebacks left join
Transactions 
on trans_id = id 

) t2 
group by month ,country
HAVING approved_count>0 OR chargeback_count>0

58 

SELECT 
query_name, 
ROUND(AVG(rating/position), 2) quality,
ROUND(avg(rating < 3) * 100,2) poor_query_percentage
FROM Queries
GROUP BY query_name


59 

select team_id , team_name, sum(case when team_id =  host_team then hsc when team_id =  guest_team then gsc else 0  end  ) num_points 
from 
teams left join 
(
select host_team ,guest_team ,
sum(if(host_goals > guest_goals,3,(if(host_goals = guest_goals,1 ,0 )))) as hsc
,
sum(if(host_goals < guest_goals,3,(if( host_goals = guest_goals,1 ,0 )))) as gsc
from 
matches 
group by match_id ) t 

on (team_id = host_team or team_id = guest_team )
group by team_id 

order by num_points desc ,team_id asc


60 

变量


SELECT period_state, MIN(date) as start_date, MAX(date) as end_date
FROM (
SELECT
success_date AS date,
"succeeded" AS period_state,
IF(DATEDIFF(@pre_date, @pre_date := success_date) = -1, @id, @id := @id+1) AS id 
FROM Succeeded, (SELECT @id := 0, @pre_date := NULL) AS temp
UNION
SELECT
fail_date AS date,
"failed" AS period_state,
IF(DATEDIFF(@pre_date, @pre_date := fail_date) = -1, @id, @id := @id+1) AS id 
FROM Failed, (SELECT @id := 0, @pre_date := NULL) AS temp
) T  WHERE date BETWEEN "2019-01-01" AND "2019-12-31"
GROUP BY T.id
ORDER BY start_date ASC

常规

select distinct label period_state, min(success_date) over (partition by r3,label ) start_date,
max(success_date) over (partition by  r3,label) end_date 

from (
select *, r2-r1 r3
from(
select *, row_number() over (partition by label order by success_date) r1 ,
row_number() over (order by success_date) r2
from 
(
select *, 'succeeded' as label from succeeded 
union 
select * , 'failed' as label from failed 
order by success_date
)  t1

) t2) t3

where year(success_date) = 2019
order by start_date 


61


select a.sub_id post_id, count(distinct b.sub_id) as number_of_comments 

from submissions  a left join submissions  b
on a.sub_id= b.parent_id
where a.parent_id is null 

group by post_id
order by post_id


62 

select p.product_id, round(sum(units*price)/sum(units),2) as average_price
from (
prices p  join unitssold u  
on p.product_id  = u.product_id)
where purchase_date between start_date and end_date
group by p.product_id

63 


select distinct page_id as recommended_page 
from likes 
where user_id in 
(
select 
case when user1_id = 1 then user2_id 
when user2_id = 1 then user1_id end 
from friendship 
)
and 
page_id not in (select page_id from likes where user_id= 1 )


64 

select employee_id from employees 
where manager_id = 1 and employee_id <> 1 

union 

select employee_id from employees 
where manager_id in (select employee_id from employees 
where manager_id = 1 and employee_id <> 1 )

union 
select employee_id from employees where manager_id in 
(
select employee_id from employees 
where manager_id in (select employee_id from employees 
where manager_id = 1 and employee_id <> 1 ))

65


select s1.student_id , s1.student_name ,s2.subject_name,count(e.subject_name) as attended_exams

from students s1 join subjects s2 left join examinations  e 
on s1.student_id = e.student_id and s2.subject_name =e.subject_name 

group by s1.student_id ,s2.subject_name 
order by s1.student_id

66

select 
min(log_id)  as 'start_id',
max(log_id) as 'end_id'
from (
select log_id ,log_id - row_number() over (order by log_id ) as label 
from logs ) t 
group by label

67 

select country_name , case when avg(weather_state) <= 15 then 'Cold' when avg(weather_state) >= 25 then 'Hot' else 'Warm' end as weather_type 

from countries join weather using(country_id)
where  date_format(day,'%Y-%m') = '2019-11'
group by country_id

68 

select employee_id  ,team_size 
from employee e  join  ( select team_id, count(employee_id) team_size  from employee  group by team_id ) t 
on e.team_id = t.team_id 

69 

select gender ,day , sum(score_points) over (partition by gender order by day) total 
from scores

变量法

select gender,day ,
case when gender  = 'F' 
# fsc 代表男得分  msc代表女得分
then @fsc := @fsc + score_points 
else @msc := @msc + score_points
end total 
# 这里from 后边是在给声明和初始化变量
from scores ,  (select @fsc:= 0 , @msc := 0 ) t
order by gender ,day



70 

select  visited_on,amount,average_amount from (
select visited_on,
sum(amount) over (order by visited_on rows 6                preceding  ) amount,
round(avg(amount) over (order by visited_on rows 6            preceding  ),2)  average_amount,
min(visited_on) over (order by visited_on ) label 
from 
(
select visited_on,sum(amount) amount from customer group by visited_on) t1
) t2
where datediff(visited_on,label) >= 6
