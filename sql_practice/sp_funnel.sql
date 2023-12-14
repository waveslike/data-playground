use funnel;

## data set 1. (cosmetics shop)
-- create table (monthly)
create table 2019_Oct (
event_time datetime
, event_type VARCHAR(255)
, product_id VARCHAR(255)
, category_id VARCHAR(255)
, category_code VARCHAR(255)
, brand VARCHAR(255)
, price VARCHAR(255)
, user_id VARCHAR(255)
, user_session VARCHAR(255) 
);

create table 2019_Nov (
event_time datetime
, event_type VARCHAR(255)
, product_id VARCHAR(255)
, category_id VARCHAR(255)
, category_code VARCHAR(255)
, brand VARCHAR(255)
, price VARCHAR(255)
, user_id VARCHAR(255)
, user_session VARCHAR(255) 
);

create table 2019_Dec (
event_time datetime
, event_type VARCHAR(255)
, product_id VARCHAR(255)
, category_id VARCHAR(255)
, category_code VARCHAR(255)
, brand VARCHAR(255)
, price VARCHAR(255)
, user_id VARCHAR(255)
, user_session VARCHAR(255) 
);

create table 2020_Jan (
event_time datetime
, event_type VARCHAR(255)
, product_id VARCHAR(255)
, category_id VARCHAR(255)
, category_code VARCHAR(255)
, brand VARCHAR(255)
, price VARCHAR(255)
, user_id VARCHAR(255)
, user_session VARCHAR(255) 
);

create table 2020_Feb (
event_time datetime
, event_type VARCHAR(255)
, product_id VARCHAR(255)
, category_id VARCHAR(255)
, category_code VARCHAR(255)
, brand VARCHAR(255)
, price VARCHAR(255)
, user_id VARCHAR(255)
, user_session VARCHAR(255) 
);


# terminal
-- local file road (monthly)
load data local infile '/Users/w/Documents/DataSet/funnel/cosmetics_shop/2019_Oct.csv'
into table 2019_Oct
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session);


load data local infile '/Users/w/Documents/DataSet/funnel/cosmetics_shop/2019_Nov.csv'
into table 2019_Nov
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session);


load data local infile '/Users/w/Documents/DataSet/funnel/cosmetics_shop/2019_Dec.csv'
into table 2019_Dec
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session);


load data local infile '/Users/w/Documents/DataSet/funnel/cosmetics_shop/2020_Jan.csv'
into table 2020_Jan
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session);


load data local infile '/Users/w/Documents/DataSet/funnel/cosmetics_shop/2020_Feb.csv'
into table 2020_Feb
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session);

-- test (rows: 4,102,283 ?)
select *
from 2019_Oct
limit 100;

select count(*)
from 2019_Oct;


-- test (rows: 4,635,837 ?)
select *
from 2019_Nov;

select count(*)
from 2019_Nov;

-- test (rows: 3,533,286 ?)
select *
from 2019_Dec;

select count(*)
from 2019_Dec;

-- test (rows: 4,264,752 ?)
select *
from 2020_Jan;

select count(*)
from 2020_Jan;

-- test (rows: 4,156,682 ?)
select *
from 2020_Feb;

select count(*)
from 2020_Feb;

# union 작업하기


## data set 2 (sales activity)
-- data import or terminal
create table sales_activity_events (
ts datetime
, customer VARCHAR(255) PRIMARY KEY
, event VARCHAR(255)
, feature_json VARCHAR(255)
, revenue_impact VARCHAR(255)
, link VARCHAR(255)
);


load data local infile '/Users/w/Documents/DataSet/funnel/sales_activity/sales_activity_events.csv'
into table sales_activity
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(ts, customer, event, feature_json, revenue_impact, link);


-- test (rows: 2130, columns: 6)
select * 
from sales_activity_events
limit 10;


select count(*)
from sales_activity_events;

drop table sales_activity_events;



# start
use funnel;

select *
from 2019_nov
limit 10;


# Baseline 강의자료
WITH pv AS (
  SELECT user_pseudo_id
       , ga_session_id
       , event_timestamp_kst AS pv_at
       , source
       , medium
  FROM ga
  WHERE page_title = '백문이불여일타 SQL 캠프 입문반'
    AND event_name = 'page_view'
), scroll AS (
  SELECT user_pseudo_id
       , ga_session_id
       , event_timestamp_kst AS scroll_at
  FROM ga
  WHERE page_title = '백문이불여일타 SQL 캠프 입문반'
    AND event_name = 'scroll'
), click AS (
  SELECT user_pseudo_id
       , ga_session_id
       , event_timestamp_kst AS click_at
  FROM ga 
  WHERE page_title = '백문이불여일타 SQL 캠프 입문반'
		AND event_name IN ('SQL_basic_form_click', 'SQL_basic_1day_form_click', 'SQL_package_form_click')
)

SELECT pv.source
     , pv.medium
     , COUNT(DISTINCT pv.user_pseudo_id, pv.ga_session_id) AS pv
     , COUNT(DISTINCT scroll.user_pseudo_id, scroll.ga_session_id) AS scroll
     , COUNT(DISTINCT click.user_pseudo_id, click.ga_session_id) AS click
     , COUNT(DISTINCT scroll.user_pseudo_id, scroll.ga_session_id) / COUNT(DISTINCT pv.user_pseudo_id, pv.ga_session_id) AS pv_scroll_rate
     , COUNT(DISTINCT click.user_pseudo_id, click.ga_session_id) / COUNT(DISTINCT scroll.user_pseudo_id, scroll.ga_session_id) AS scroll_click_rate
     , COUNT(DISTINCT click.user_pseudo_id, click.ga_session_id) / COUNT(DISTINCT pv.user_pseudo_id, pv.ga_session_id) AS pv_click_rate
FROM pv
     LEFT JOIN scroll ON pv.user_pseudo_id = scroll.user_pseudo_id
                      AND pv.ga_session_id = scroll.ga_session_id
                      AND pv.pv_at <= scroll.scroll_at
     LEFT JOIN click ON scroll.user_pseudo_id = click.user_pseudo_id
                     AND scroll.ga_session_id = click.ga_session_id
                     AND scroll.scroll_at <= click.click_at
GROUP BY pv.source, pv.medium
ORDER BY click DESC, pv DESC;
 

# 1. 분석의 목적
-- event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session
select *
from 2019_nov
limit 10;


-- 퍼널별 이벤트 수
SELECT event_type, COUNT(DISTINCT user_id, user_session) AS session_count
FROM 2019_Nov
GROUP BY event_type
ORDER BY session_count DESC;


-- 상품 매출 top10, bottom10
-- top10
select product_id
	 , round(sum(price)) sales
from 2019_Nov
where event_type = 'purchase'
group by product_id
order by sales desc
limit 10;


-- bottom10
select product_id
	 , round(count(*) * avg(price)) sales
from 2019_Nov
where event_type = 'purchase'
group by product_id
having sales > 0
order by sales asc
limit 10;


-- 매출 상위 브랜드 top10
SELECT brand
	 , round(count(*) * avg(price)) AS sum_price
FROM 2019_Nov
WHERE event_type = 'purchase'
GROUP BY brand
ORDER BY sum_price DESC
LIMIT 10;


-- 구매금액 추이
SELECT DAY(event_time) AS event_day
	 , ROUND(SUM(price),0)
FROM 2019_Nov
WHERE event_type = 'purchase'
GROUP BY event_day;

-- weekday 활용도 가능, monday = 0 ~ sunday = 6
SELECT weekday(event_time) AS event_day
	 , ROUND(sum(price),0) tot_price
FROM 2019_Nov
WHERE event_type = 'purchase'
GROUP BY event_day 
order by event_day;


-- 구매로 이어지는 사용자 세션 패턴
with A as (
	select concat(user_id, '_' ,user_session) session
         , event_time, event_type, product_id, category_id, category_code, brand, price
	from 2019_Nov
	where event_type in ('view', 'cart', 'purchase')
    ), B as (
    select session
		 , event_type
	  -- , min(event_time)
         , count(*) cnt
    from A
	group by session, event_type
    order by cnt
    )
select event_type
	 , count(*) cnt
     , ROUND(count(*) / lead(count(*), 1) OVER () * 100, 2) AS conversion_rate
from B
group by B.event_type;

with A as (
	select *
	     , concat(user_id, '_' , user_session) session
	from 2019_Nov
	where event_type in ('view', 'cart', 'purchase')
    )    
select event_type
	 , count(*) cnt
from (
	select session
		 , event_type
	  -- , min(event_time)
  from A
	group by session, event_type
	) B
group by B.event_type
order by cnt desc;


-- 인기 카테고리의 사용자 상호작용
