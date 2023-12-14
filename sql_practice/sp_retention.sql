use retention;


## data set 2. electronics_store (workbench)
# create table (workbench)
create table electronics_store (
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


# local file road (workbench)
load data local infile '/Users/w/Documents/DataSet/retention/electronics_store/electronics_store.csv'
into table electronics_store
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(event_time, event_type, product_id, category_id, category_code, brand,	price, user_id, user_session);



-- test (rows: 885,130 = 885,130 / columns: 9)
select *
from electronics_store
limit 10;

select count(*)
from electronics_store;

select distinct count(*)
from electronics_store;

select min(event_time)
	 , max(event_time)
from electronics_store;













# data set 1. create table (workbench)
create table online_retail (
  InvoiceNo VARCHAR(255)
, StockCode VARCHAR(255)
, Description VARCHAR(255)
, Quantity INT
, InvoiceDate TIMESTAMP
, UnitPrice FLOAT
, CustomerID VARCHAR(255)
, Country VARCHAR(255)
);

# local file road (terminal)
load data local infile '/Users/w/Documents/DataSet/retention/online_retail/online_retail.csv'
into table online_retail
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country);



-- local file road (work bench)
use retention;
drop table online_retail;

-- test(rows: 541,909 = 541,909, columns: 8)
select *
from online_retail
where country like 'United%'
limit 3;



select count(*)
from online_retail;

select min(invoicedate)
	 , max(invoicedate)
from online_retail;



-- 날짜 데이터 형태 변경(일단 사용안함)
-- ALTER TABLE online_retail ADD NewColumn DATE;                    -- 1. 새로운 컬럼을 추가합니다.
-- UPDATE online_retail_daily SET NewColumn = DATE(InvoiceDate);    -- 2. 기존 데이터를 새로운 컬럼으로 복사합니다. error
-- SET SQL_SAFE_UPDATES = 0;                                        -- 2-1. error 1175. safe mode 해제 (다시 2.)
-- ALTER TABLE online_retail DROP COLUMN InvoiceDate;               -- 3. 기존 컬럼을 삭제하거나 남겨둡니다.
-- ALTER TABLE online_retail CHANGE NewColumn InvoiceDate DATE;     -- 4. 새로운 컬럼의 이름을 변경하여 기존 컬럼과 동일하게 만들 수 있습니다.


-- 여러 컬럼 순서 변경
ALTER TABLE online_retail
MODIFY COLUMN InvoiceNo VARCHAR(255) AFTER CustomerID,
MODIFY COLUMN InvoiceDate VARCHAR(255) AFTER InvoiceNo,
MODIFY COLUMN Country VARCHAR(255) AFTER InvoiceDate,
MODIFY COLUMN StockCode VARCHAR(255) AFTER Country,
MODIFY COLUMN Description VARCHAR(255) AFTER StockCode,
MODIFY COLUMN UnitPrice FLOAT AFTER Description,
MODIFY COLUMN Quantity INT AFTER UnitPrice;


-- InvoiceDate, (+date_month), InvoiceNo, CustomerID, Country, StockCode, Description, UnitPrice, Quantity
-- 새로운 테이블 생성
CREATE TABLE retail_monthly AS
SELECT CustomerID, InvoiceNo, InvoiceDate
     , (DATE_FORMAT(InvoiceDate, '%Y-%m')) AS date_month  -- 리텐션 기준을 '월 단위'로 정해서 생성한 컬럼
     , Country, StockCode, Description, UnitPrice, Quantity
FROM online_retail
WHERE CustomerID <> 0;





# 테이블 생성 (중복 인스턴스 제거)
-- InvoiceDate, InvoiceNo, CustomerID, Country, StockCode, Description, UnitPrice, Quantity
CREATE VIEW online_retail_monthly AS
SELECT 
    SUBSTR(CAST(`invoicedate` AS CHAR), 1, 4) AS year,
    SUBSTR(CAST(`invoicedate` AS CHAR), 6, 2) AS month,
    customerid,
    SUM(quantity) AS quantity
FROM online_retail
WHERE customerid <> 0
GROUP BY year, month, customerid
HAVING SUM(quantity) > 0
ORDER BY year, month;

select *
from online_retail_monthly
limit 5;



# 회원(CustomerId)들의 연평균 방문수
# ERD
-- 인스턴스 수
select count(*)
from online_retail;


-- 회원 고객 수
select count(CustomerID) members
from online_retail
where CustomerID <> 0;


-- 비회원 주문 수
select count(CustomerID) non_members
from online_retail
where CustomerID = 0;


-- 전체 기간
select min(InvoiceDate), max(InvoiceDate)
from online_retail;


-- 날짜 수
-- select DATEDIFF('2011.12.09', '2010.12.01');
select DATEDIFF(max(InvoiceDate), min(InvoiceDate))
from online_retail;


-- 평균 방문 수 (*수정필요)
select round(avg(visit_cnt), 1) avg_visit
  from (
	select CustomerID
		 , count(distinct InvoiceDate) visit_cnt
	from online_retail
	group by CustomerID
	having CustomerID <> 0
    ) sub;


-- 방문 횟수 최대, 최소
SELECT min(Visit_Cnt) max_visit
	 , max(Visit_Cnt) min_visit
FROM(   
    SELECT
        CustomerID,
        COUNT(DISTINCT InvoiceDate) AS Visit_Cnt
    FROM online_retail
    WHERE CustomerID <> 0
    GROUP BY CustomerID) min_max;
    


-- InvoiceDate, InvoiceNo, CustomerID, Country, StockCode, Description, UnitPrice, Quantity
-- 코호트 (1)구매 횟수 기준
WITH order_sort AS (
    SELECT
        CASE
            WHEN order_cnt <= 20 THEN '20 less than'
            WHEN order_cnt <= 30 THEN '30 less than'
            WHEN order_cnt <= 40 THEN '40 less than'
            ELSE '50 greater than'
        END AS order_cnt_group,
        COUNT(*) AS order_cnt
    FROM (
        SELECT
            CustomerID,
            COUNT(DISTINCT InvoiceDate) AS order_cnt
        FROM online_retail
        WHERE CustomerID <> 0
		AND UnitPrice * Quantity > 0
        GROUP BY CustomerID
    ) AS sub
    GROUP BY order_cnt_group
)

SELECT
    order_cnt_group
  , order_cnt
  , CONCAT(ROUND((order_cnt * 100.0 / SUM(order_cnt) OVER ()), 1), '%') AS order_ratio
  , CONCAT(ROUND(SUM(order_cnt) OVER (ORDER BY order_cnt_group) * 100.0 / SUM(order_cnt) OVER (), 1), '%') AS order_ratio_accum
FROM order_sort
ORDER BY order_cnt_group;


-- InvoiceDate, InvoiceNo, CustomerID, Country, StockCode, Description, UnitPrice, Quantity
-- 코호트 (2)매출 기준
WITH sales_sort AS (
    SELECT
    CASE
        WHEN order_cnt <= 20 THEN '20 less than'
        WHEN order_cnt <= 30 THEN '30 less than'
        WHEN order_cnt <= 40 THEN '40 less than'
        ELSE '50 greater than'
    END AS order_cnt_group,
    SUM(total_sales) AS total_sales
FROM (
    SELECT
        CustomerID,
        COUNT(DISTINCT InvoiceDate) AS order_cnt,
        SUM(UnitPrice * Quantity) AS total_sales
    FROM online_retail
    WHERE CustomerID <> 0
   -- AND UnitPrice * Quantity > 0
    GROUP BY CustomerID
) AS sub
GROUP BY order_cnt_group
ORDER BY order_cnt_group
)

SELECT
    order_cnt_group,
    ROUND(SUM(total_sales)) AS total_sales,
    CONCAT(ROUND((SUM(total_sales) * 100.0 / SUM(total_sales) OVER ()), 1), '%') AS sales_ratio,
    CONCAT(ROUND(SUM(total_sales) OVER (ORDER BY order_cnt_group) * 100.0 / SUM(total_sales) OVER (), 1), '%') AS sales_ratio_accum
FROM sales_sort
GROUP BY order_cnt_group
ORDER BY order_cnt_group;







# 1. 분석의 목적
-- 분석방법
-- InvoiceDate, InvoiceNo, CustomerID, Country, StockCode, Description, UnitPrice, Quantity



## 국가별 구매량 비율
SELECT Country
     , sum_customers
     , round((sum_customers * 100.0) / sum(sum_customers) OVER (), 1) AS pct_customers
     , round(sum_sales)
     , round((sum_sales * 100.0) / sum(sum_sales) OVER (), 1) AS pct_sales
FROM (
   SELECT Country
         , SUM(distinct CustomerID) AS sum_customers
         , SUM(UnitPrice * Quantity) AS sum_sales
	FROM online_retail
    GROUP BY Country
    ORDER BY sum_customers DESC
) AS subquery;



-- 국가별 구매량 비율
SELECT Country
     , sum_qnt
     , round((sum_qnt * 100.0) / sum(sum_qnt) OVER (), 1) AS pct_qnt
     , unitprice * quantity
FROM (
    SELECT Country
         , SUM(Quantity) AS sum_qnt
    FROM online_retail
    GROUP BY Country
    ORDER BY sum_qnt DESC
) AS subquery;







-- 전체 기간
SELECT DATEDIFF(MAX(InvoiceDate), MIN(InvoiceDate)) AS date_diff
		FROM online_retail
		WHERE CustomerID <> 0;



-- 평균 재구매 주기
SELECT ROUND(AVG(reorder_days), 1) avg_reorder
FROM (
	SELECT CustomerID
		 , date_diff
		 , order_count
		 , (date_diff / order_count-1) reorder_days
	FROM (
		SELECT CustomerID
			 , MIN(InvoiceDate) AS first_order_date
			 , MAX(InvoiceDate) AS last_order_date
			 , DATEDIFF(MAX(InvoiceDate), MIN(InvoiceDate)) AS date_diff
			 , COUNT(DISTINCT InvoiceNo) AS order_count
		FROM online_retail
		WHERE CustomerID <> 0	
        AND country LIKE 'United%'
		GROUP BY CustomerID
		 ) sub1_diff
	WHERE (date_diff / order_count) <> 0
	GROUP BY CustomerID
    ) sub2_reorder;




# 결과
-- InvoiceDate, InvoiceNo, CustomerID, Country, StockCode, Description, UnitPrice, Quantity
-- Step1. 리텐션 계산하기
-- 구매횟수 20번 이하 코호트
CREATE TABLE retail_formatted_20 AS
SELECT *
FROM retail_formatted
WHERE country LIKE 'United%'    -- 영국만
      AND customer_id <> 0      -- 회원만
      AND sum_sales > 0        -- 환불건 제외
      AND customer_id IN (
          SELECT customer_id
          FROM retail_formatted
          GROUP BY customer_id
          HAVING COUNT(customer_id) <= 20  -- 구매 횟수가 20회 이하
      );


-- 첫 구매한 달로부터 월별 구매 고객 수 계산하기
WITH retail_cohort AS (
    SELECT customer_id, order_id
		 , order_date
		 , MIN(DATE_FORMAT(order_date, '%Y-%m-%d')) OVER (PARTITION BY customer_id) AS first_order_date
         , DATE_FORMAT(order_date, '%Y-%m-01') AS order_month
         , MIN(DATE_FORMAT(order_date, '%Y-%m-01')) OVER (PARTITION BY customer_id) AS first_order_month
         , Country, StockCode, Description, UnitPrice, Quantity, sum_sales
    FROM retail_formatted
)
SELECT first_order_month
     , COUNT(DISTINCT customer_id) AS month0
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 1 MONTH = order_month THEN customer_id ELSE NULL END) AS month1  -- date_add() 생략
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 2 MONTH = order_month THEN customer_id ELSE NULL END) AS month2  
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 3 MONTH = order_month THEN customer_id ELSE NULL END) AS month3  
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 4 MONTH = order_month THEN customer_id ELSE NULL END) AS month4
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 5 MONTH = order_month THEN customer_id ELSE NULL END) AS month5  
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 6 MONTH = order_month THEN customer_id ELSE NULL END) AS month6  
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 7 MONTH = order_month THEN customer_id ELSE NULL END) AS month7  
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 8 MONTH = order_month THEN customer_id ELSE NULL END) AS month8 
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 9 MONTH = order_month THEN customer_id ELSE NULL END) AS month9  
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 10 MONTH = order_month THEN customer_id ELSE NULL END) AS month10  
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 11 MONTH = order_month THEN customer_id ELSE NULL END) AS month11 
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 12 MONTH = order_month THEN customer_id ELSE NULL END) AS month12  
FROM retail_cohort 
GROUP BY first_order_month
ORDER BY first_order_month;




-- Step2. 리텐션 시각화
-- gsheet 작업


## 전체 리텐션
-- 전체 코호트
CREATE TABLE retail_formatted_all AS
SELECT *
FROM retail_formatted
WHERE country LIKE 'United%'    -- 영국만
      AND customer_id <> 0      -- 회원만
      AND sum_sales > 0;        -- 환불건 제외



-- 첫 구매한 달로부터 월별 구매 고객 수 계산하기
WITH retail_cohort AS (
    SELECT customer_id, order_id
		 , order_date
		, MIN(DATE_FORMAT(order_date, '%Y-%m-%d')) OVER (PARTITION BY customer_id) AS first_order_date
        , DATE_FORMAT(order_date, '%Y-%m-01') AS order_month
        , MIN(DATE_FORMAT(order_date, '%Y-%m-01')) OVER (PARTITION BY customer_id) AS first_order_month
        , Country, StockCode, Description, UnitPrice, Quantity, sum_sales
    FROM retail_formatted
)
SELECT first_order_month
     , COUNT(DISTINCT customer_id) AS month0
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 1 MONTH = order_month THEN customer_id ELSE NULL END) AS month1  -- date_add() 생략
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 2 MONTH = order_month THEN customer_id ELSE NULL END) AS month2  
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 3 MONTH = order_month THEN customer_id ELSE NULL END) AS month3  
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 4 MONTH = order_month THEN customer_id ELSE NULL END) AS month4
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 5 MONTH = order_month THEN customer_id ELSE NULL END) AS month5  
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 6 MONTH = order_month THEN customer_id ELSE NULL END) AS month6  
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 7 MONTH = order_month THEN customer_id ELSE NULL END) AS month7  
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 8 MONTH = order_month THEN customer_id ELSE NULL END) AS month8 
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 9 MONTH = order_month THEN customer_id ELSE NULL END) AS month9  
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 10 MONTH = order_month THEN customer_id ELSE NULL END) AS month10  
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 11 MONTH = order_month THEN customer_id ELSE NULL END) AS month11 
     , COUNT(DISTINCT CASE WHEN first_order_month + INTERVAL 12 MONTH = order_month THEN customer_id ELSE NULL END) AS month12  
FROM retail_cohort 
GROUP BY first_order_month
ORDER BY first_order_month;




# Step3. 추가 분석
-- 바캉스, 선물포장, 수리용품, 스포츠용품, 아마존, 악세사리, 어린이용, 여행용품, 욕실용품, 인테리어, 일반소품, 조리도구, 커피, 케이크, 크리스마스, 학용품, 홈데코
-- 질문1-1: 리텐션 수치가 높은 ‘2011-02’  첫 구매자들의 ‘첫 달’ 인기 상품은 무엇었을까?
-- 2월: ‘2011-02’  코호트 vs 나머지 코호트




-- 2월 전체 구매자 & 품목 수 크기
select count(distinct customer_id)
	 , (select count(distinct description) from retail_formatted_20_monthly)
from retail_formatted_20_monthly
where order_month = '2011-02-01';



-- 인기 TOP10 품목 상세
-- 2월 인기 TOP10 (대상: 2월 첫구매자)
SELECT description
     , COUNT(DISTINCT order_id) AS cnt_order
     , COUNT(DISTINCT customer_id) AS cnt_customer
     , ROUND(COUNT(DISTINCT customer_id) * 100 / (SELECT COUNT(DISTINCT customer_id) FROM retail_formatted_20 WHERE first_order_month = '2011-02-01'), 2) AS pct_customer
FROM retail_formatted_20_monthly
WHERE first_order_month = '2011-02-01'
GROUP BY description
ORDER BY cnt_order DESC
LIMIT 10;



-- 2월 인기 TOP10 (대상: 2월 첫구매자를 제외한 나머지)
SELECT description
     , COUNT(DISTINCT order_id) AS cnt_order
     , COUNT(DISTINCT customer_id) AS cnt_customer
     , ROUND(COUNT(DISTINCT customer_id) * 100 / (SELECT COUNT(DISTINCT customer_id) FROM retail_formatted_20 WHERE order_month = '2011-02-01'), 2) AS pct_customer
FROM retail_formatted_20_monthly
WHERE first_order_month <> '2011-02-01'
AND order_month = '2011-02-01'
GROUP BY description
ORDER BY cnt_order DESC
LIMIT 10;





## 질문1-2:  ‘2011-02’  첫 구매자들의 5월 6월 9월 재구매 인기상품은 무엇일까?
### ‘2011-02’ 코호트: 5,6,9월 재구매 vs 나머지 월 재구매

-- 리텐션 높은 달의 인기 카테고리





-- 인기상품과 주문횟수
-- 리텐션 높은 달의 인기상품과 주문횟수
SELECT description
     , COUNT(DISTINCT order_id) AS cnt_order
FROM retail_formatted_20_monthly
WHERE first_order_month = '2011-02-01'  -- 2월 첫 결제자 코호트
-- AND order_month = '2011-05-01'          -- 5월 재구매 횟수
-- AND order_month = '2011-06-01'       -- 6월 재구매 횟수
-- AND order_month = '2011-09-01'       -- 9월 재구매 횟수
AND order_month NOT IN ('2011-05-01', '2011-06-01', '2011-09-01')    -- 5,6,9월 제외 나머지 월 재구매 횟수
GROUP BY description
ORDER BY cnt_order DESC
LIMIT 10;









-- 데이터 전처리
-- customer_id, order_id, order_date, first_order_date, order_month, first_order_month, Country, StockCode, Description, UnitPrice, Quantity, sum_sales
-- Step1. 테이블 생성
CREATE TABLE retail_formatted AS
SELECT CustomerID AS customer_id
	 , InvoiceNo AS order_id
     , DATE_FORMAT(InvoiceDate, '%Y-%m-%d') AS order_date
     , Country, StockCode, Description, UnitPrice, Quantity
     , (UnitPrice * Quantity) sum_sales
FROM online_retail
ORDER BY customer_id, order_date;



-- Step2. 코호트 조건
-- 구매횟수 20번 이하 코호트
CREATE TABLE retail_formatted_20 AS
SELECT *
FROM retail_formatted
WHERE country LIKE 'United%'    -- 영국만
      AND customer_id <> 0      -- 회원만
      AND sum_sales > 0       -- 환불건 제외
      AND customer_id IN (
           SELECT customer_id
           FROM retail_formatted
           GROUP BY customer_id
           HAVING COUNT(customer_id) <= 20);  -- 구매 횟수가 20회 이하



-- 전체 코호트
CREATE TABLE retail_formatted_all AS
SELECT *
FROM retail_formatted
WHERE country LIKE 'United%'    영국만
      AND customer_id <> 0      -- 회원만
      AND sum_sales > 0;        -- 환불건 제외



-- Step3.컬럼 추가
-- `first_order_date`, `order_month`, `first_order_month` 컬럼 추가

CREATE TABLE retail_formatted_20_monthly AS
SELECT customer_id, order_id
	 , order_date
	, MIN(DATE_FORMAT(order_date, '%Y-%m-%d')) OVER (PARTITION BY customer_id) AS first_order_date
	, DATE_FORMAT(order_date, '%Y-%m-01') AS order_month
	, MIN(DATE_FORMAT(order_date, '%Y-%m-01')) OVER (PARTITION BY customer_id) AS first_order_month
	, Country, StockCode, Description, UnitPrice, Quantity, sum_sales
FROM retail_formatted_20;



-- Step4. 상품 카테고라이징
-- 상품 종류수
select count(distinct description)
from online_retail
where description is not null
AND description NOT LIKE '%?%';

-- 상품 리스트
SELECT DISTINCT description
FROM online_retail
WHERE description IS NOT NULL
AND description NOT LIKE '%?%'
ORDER BY description;



-- 3. 제안
SELECT count(distinct description)
FROM retail_formatted2;

SELECT distinct description
FROM retail_formatted2
LIMIT 30;


# EDA
-- InvoiceDate, InvoiceNo, CustomerID, Country, StockCode, Description, UnitPrice, Quantity

# 1.데이터 요약 통계 확인
-- 데이터셋 크기 확인
SELECT COUNT(*) AS TotalRecords 
FROM online_retail;



-- 데이터 유형 확인
DESCRIBE online_retail;



-- 결측값 확인
SELECT COUNT(*) AS MissingValues
FROM online_retail 
-- WHERE InvoiceDate IS NULL;
-- WHERE InvoiceNo IS NULL;
-- WHERE CustomerID IS NULL;
WHERE Country IS NULL;
-- WHERE StockCode IS NULL;
-- WHERE Description IS NULL;
-- WHERE UnitPrice IS NULL;
WHERE Quantity IS NULL;



-- 중복 레코드 확인
SELECT COUNT(*) AS DuplicateRecords
FROM online_retail
GROUP BY InvoiceNo, StockCode, CustomerID  -- 중복 여부를 확인할 열을 나열
HAVING COUNT(*) > 1;




# 데이터 분포 및 시각화
-- InvoiceDate, InvoiceNo, CustomerID, Country, StockCode, Description, UnitPrice, Quantity
select *
from online_retail
limit 5;




# 범주형 변수 분포 확인
## country 변수별 빈도 확인
SELECT Country
	 , COUNT(*) AS Frequency
FROM online_retail
GROUP BY Country
ORDER BY Frequency desc;



# 수치형 변수 분포 확인: UnitPrice
## UnitPrice 변수 갯수 확인
select count(distinct UnitPrice) dis_price
from online_retail;



## 평균, 최대, 최소값 확인
SELECT ROUND(AVG(UnitPrice),1) AS Mean
	 , MIN(UnitPrice) AS Minimum       # 극단값이 많기 때문에 시각적 확인이 필
     , MAX(UnitPrice) AS Maximum
FROM online_retail
WHERE UnitPrice > 0;



## 환불건
SELECT MIN(UnitPrice) AS Refund     # 음수는 환불금액
	 , COUNT(UnitPrice) AS Count    # 누락없음
FROM online_retail
WHERE UnitPrice < 0;



## StockCode x UnitPrice 별 가격 확인
SELECT StockCode
	 , UnitPrice
FROM online_retail
GROUP BY StockCode, UnitPrice
ORDER BY UnitPrice desc;
