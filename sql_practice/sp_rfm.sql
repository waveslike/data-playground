use rfm;

select *
from join_data
limit 10;



drop table order_details;
drop table orders;
drop table pizzas;
drop table pizza_types;


-- create table
create table order_details (
	order_details_id INT PRIMARY KEY,
	order_id int,
	pizza_id VARCHAR(255),
	quantity int);
    
create table orders (
`order_id` INT PRIMARY KEY,
`date` date,
`time` time);

create table pizzas (
`pizza_id` varchar(50)  PRIMARY KEY,
`pizza_type_id` varchar(50),
`size` varchar(50),
`price` float);

create table pizza_types (
`pizza_type_id` varchar(255)  PRIMARY KEY,
`name` varchar(255),
`category` varchar(255),
`ingredients` varchar(500)
);



# terminal setting
mysql -u root;
use rfm;


-- pc file load
load data local infile '/Users/w/Documents/DataSet/rfm/pizza_place/csv/orders.csv'
into table orders
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(order_id, date, time);


load data local infile '/Users/w/Documents/DataSet/rfm/pizza_place/csv/order_details.csv'
into table order_details
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(order_details_id, order_id, pizza_id, quantity);


load data local infile '/Users/w/Documents/DataSet/rfm/pizza_place/csv/pizzas.csv'
into table pizzas
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(pizza_id, pizza_type_id, size, price);


load data local infile '/Users/w/Documents/DataSet/rfm/pizza_place/csv/pizza_types.csv'
into table pizza_types
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(pizza_type_id, name, category, ingredients);


# test
use rfm;


-- rows: 21350 ok
select *
from orders
limit 20;

select count(*)
from orders;

-- row: 48620 ok
select *
from order_details
limit 10;

select count(*)
from order_details;


-- rows: 96 ok
select *
from pizzas
limit 10;

select count(*)
from pizzas;


-- rows: 32 ok
select *
from pizza_types
limit 100;

select count(*)
from pizza_types;

select *
from pizza_types
limit 10;
### null ###

select count(*)
from pizza_types;
### null ###


create table join_data
select o1.order_id, o1.order_details_id, o1.pizza_id, o1.quantity, o2.date, o2.time, p1.pizza_type_id, p1.size, p1.price, p2.name, p2.category, p2.ingredients
from order_details o1
left join orders o2
		on o1.order_id = o2.order_id
left join pizzas p1
		on o1.pizza_id = p1.pizza_id
left join pizza_types p2
		on p1.pizza_type_id = p2.pizza_type_id;


select column_name
from information_schema.columns
where table_name = 'join_data' and table_schema = 'test'
and is_nullable = 'no';

-- check null
select *
from join_data
limit 10;

select count(*) 
from join_data
where order_id is null;

select count(*) 
from join_data
where order_details_id is null;

select count(*) 
from join_data
where pizza_id is null;

select count(*) 
from join_data
where quantity is null;

select count(*) 
from join_data
where date is null;

select count(*) 
from join_data
where time is null;

select count(*) 
from join_data
where pizza_type_id is null;

select count(*) 
from join_data
where size is null;

select count(*) 
from join_data
where price is null;

-- error (name, category, ingredients)
select count(*) 
from join_data
where name is null;

select count(*) 
from join_data
where name is null;

select *
from join_data
limit 10;

select *
from orders;

select *
from pizza_types;

select count(*)
from pizza_types;


-- work
SELECT category
	 , pizza_id
	 , ROUND(SUM(quantity * price)) sales
     , SUM(quantity) qnt
FROM join_data
GROUP BY pizza_id, category
ORDER BY sales DESC
LIMIT 10;


SELECT ROW_NUMBER() OVER (ORDER BY sales DESC) AS rownum
	 , caegory
	 , pizza_type_id
	 , ROUND(SUM(quantity * price)) sales
     , SUM(quantity) qnt
FROM join_data
GROUP BY pizza_type_id, category
ORDER BY sales DESC
LIMIT 10;

SELECT
    category,
    pizza_type_id,
    ROUND(SUM(quantity * price)) AS sales,
    SUM(quantity) AS qnt,
    DENSE_RANK() OVER (ORDER BY sales DESC) AS ranking
FROM join_data
GROUP BY pizza_type_id, category
ORDER BY sales DESC
LIMIT 10;


use rfm;

-- Category별 피자당 매출액, 피자당 주문수 등 기본정보
SELECT category
     , product
		 , sales
		 , quantity
		 , ROUND(sales / product) AS amt_per_pizza
	 	 , ROUND(quantity / product) AS cnt_per_pizza
FROM (
	SELECT category
			 , COUNT(DISTINCT pizza_type_id) AS product
			 , ROUND(SUM(quantity*price)) AS sales
			 , SUM(quantity) quantity
	FROM join_data
	GROUP BY category
	) AS a
ORDER BY product DESC, sales DESC;