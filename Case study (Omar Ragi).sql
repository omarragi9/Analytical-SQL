-- Note : In the whole project i used inline conversion to convert date column "which is varchar" into date data type to avoid changeing the data itself 

-- ********************************************** 
--                      Q1- Using OnlineRetail dataset                       **
-- ********************************************** 

-- Query 1 : Calculate the total sales
select sum(price * quantity) as total_sales
from tableRetail;


-- Query 2 : Calculate the sum of sales per customer ordered by customer_sales to get the highest customers sales
select distinct customer_id , sum(price * quantity) over(partition by customer_id) as customer_sales
from tableRetail
order by customer_sales desc;


-- Query 3 : Calculate the sum of profit per item ordered by stock_sales to get the highest profit items  
select distinct stockcode , sum(price * quantity) over(partition by stockcode) as stock_sales 
from tableRetail
order by stock_sales desc;


-- Query 4 : Calculate the sum of sales per year
with sub_query as
(
    select INVOICE, STOCKCODE, QUANTITY, INVOICEDATE, PRICE, CUSTOMER_ID, COUNTRY , 
    extract(year from to_date(INVOICEDATE , 'MM/DD/YYYY HH24:MI')) as year
    from tableRetail
)
select distinct year , sum(price * quantity) over(partition by year) as yearly_sales
from sub_query;


-- Query 5 : Calculate the sum of sales per month per year
with sub_query as
(
    select INVOICE, STOCKCODE, QUANTITY, INVOICEDATE, PRICE, CUSTOMER_ID, COUNTRY , 
    extract(month from to_date(INVOICEDATE , 'MM/DD/YYYY HH24:MI')) as month ,
    extract(year from to_date(INVOICEDATE , 'MM/DD/YYYY HH24:MI')) as year
    from tableRetail
)
select distinct month , year,  sum(price * quantity) over(partition by month , year) as monthly_sales
from sub_query
order by monthly_sales desc;

-- Query 6 : Average number of items per invoice
with items_per_invoice as
(
    select distinct invoice , count(distinct stockcode) over(partition by invoice) as num_items
    from tableRetail
)
select avg(num_items) as avg_items_per_invoice
from items_per_invoice;


-- Query 7 : Average profit and quantity each customer buy of each product
select customer_id , stockcode , round(avg(quantity * price) , 2) as profit , round(avg(quantity) , 2) as items
from tableRetail
group by customer_id , stockcode
order by customer_id , profit desc;


-- Query 8 : Calculate the montly difference of profit 
with sub_query as
(
    select INVOICE, STOCKCODE, QUANTITY, INVOICEDATE, PRICE, CUSTOMER_ID, COUNTRY ,
    to_date(extract(month from to_date(invoicedate , 'mm/dd/yyyy hh24:mi')) || '/' || extract(year from to_date(invoicedate , 'mm/dd/yyyy hh24:mi')) , 'mm/yyyy') as month_year
    from tableRetail
) ,
extract_date as
(
    -- This sub query to avoid using distinct keywoard with sum function as it gets me this error (ORA-01791: not a SELECTed expression)
    select month_year as month_year, sum(price * quantity) over(partition by month_year order by month_year) as monthly_sales 
    from sub_query
    order by month_year
) ,
distinct_query as
(
    -- This subquery to get the distinct values of month year to avoid having duplicate rows in the result
    select distinct month_year , monthly_sales 
    from extract_date
    order by month_year
)
select month_year , monthly_sales , lag(monthly_sales , 1 , 0) over (order by month_year) as previous_month_sales,
    round(((monthly_sales - lag(monthly_sales , 1 , 0) over (order by month_year)) / lag(monthly_sales , 1 , 1) over (order by month_year)) * 100 , 2) as "sales_percentage_difference%" 
from distinct_query;


-- ********************************************** 
--                      Q2- Monetary model                                      **
-- ********************************************** 
with sub_query as
(
    select distinct customer_id , last_value(invoicedate) 
    over(partition by customer_id order by to_date(invoicedate , 'mm/dd/yyyy hh24:mi') range between unbounded preceding and unbounded following) as last_purchase , 
    count(distinct invoice) over(partition by customer_id) as Frequency ,
    sum(quantity * price) over(partition by customer_id) as Monetary ,
    invoicedate
    from tableRetail
) ,
rfm_values as
(
    -- Sub query to calculate the Recency, Frequency and  Monetary
    select distinct customer_id , round(
    last_value(to_date(invoicedate , 'mm/dd/yyyy hh24:mi')) over(order by to_date(invoicedate , 'mm/dd/yyyy hh24:mi')
range between unbounded preceding and unbounded following) - to_date(last_purchase , 'mm/dd/yyyy hh24:mi'))  as Recency , Frequency ,  Monetary , 
    (Frequency + Monetary) / 2 as fm_average
    from sub_query
) ,
rfm_scores as
(
    -- Sub query to calculate the Recency_score , Frequency_score , Monetary_score
    select distinct customer_id ,  Recency , Frequency ,  Monetary , 
    ntile(5) over(order by Recency desc) as Recency_score , ntile(5) over(order by fm_average) as avg_fm_score
    from rfm_values
)
select distinct customer_id ,  Recency , Frequency ,  Monetary , Recency_score , avg_fm_score , 
    case 
        when Recency_score = 5 and AVG_FM_Score in (5 , 4) then 'Champions'
        when Recency_score  = 4 and AVG_FM_Score = 5 then 'Champions'
        when Recency_score in (5,4) and AVG_FM_Score = 2 then 'Potential Loyalists'
        when Recency_score in (3,4) and AVG_FM_Score = 3 then 'Potential Loyalists'
        when Recency_score = 5 and AVG_FM_Score = 3 then 'Loyal Customers'
        when Recency_score = 4 and AVG_FM_Score = 4 then 'Loyal Customers'
        when Recency_score = 3 and AVG_FM_Score in (4 , 5) then 'Loyal Customers'
        when Recency_score = 5 and AVG_FM_Score = 1 then 'Recent Customers'
        when Recency_score in (4 , 3) and AVG_FM_Score = 1 then 'Promising'
        when Recency_score = 3 and AVG_FM_Score = 2 then 'Customers Needing Attention'
        when Recency_score = 2 and AVG_FM_Score in (3 , 2) then 'Customers Needing Attention'
        when Recency_score = 2 and AVG_FM_Score in (4 , 5) then 'At Risk'
        when Recency_score = 1 and AVG_FM_Score = 3 then 'At Risk'
        when Recency_score = 1 and AVG_FM_Score IN (5 , 4) then 'Cant Lose Them'
        when Recency_score = 1 and AVG_FM_Score = 2 then 'Hibernating'
        when Recency_score = 2 and AVG_FM_Score  = 1 then 'Lost'            -- This value was not provided in the given table but was set in the logic, as not adding it will allow some rows to have empty values.
        when Recency_score = 1 and AVG_FM_Score  = 1 then 'Lost'
    end as cust_segment
from rfm_scores;


-- ********************************************** 
--           Q3- daily purchasing transactions for customers          **
-- ********************************************** 

-- Part 1
-- Create the new table and insert data into it
-- create table daily_transactions (cust_id number(10) , datee date , amount number(10,2));
with all_data as
(
    select cust_id , datee , amount , row_number () over(partition by cust_id order by datee) as row_num
    from daily_transactions
) ,date_diff as
(
    select cust_id , datee , amount , row_num , datee - row_num as diff
    from all_data
) ,cons_days as
(
    select cust_id , datee , amount , row_num , diff , count(*) over(partition by cust_id , diff) as consecutive_days
    from date_diff
)
select distinct cust_id , max(consecutive_days) over(partition by cust_id) as max_consecutive_days
from cons_days;



-- Part 2
with cumulative_transactions as
(
    select cust_id , datee , amount ,
    sum(amount) over(partition by cust_id order by datee rows between unbounded preceding and current row) as summ
    , count(amount) over(partition by cust_id order by datee rows between unbounded preceding and current row) as countt
    from daily_transactions
) , filtered_transactions  as
(
    select cust_id , datee , amount , summ , countt + 1 as countt
    from cumulative_transactions 
    where summ <= 250
) , max_days_per_customer  as
(
    select cust_id , max(countt) as max_days
    from filtered_transactions 
    group by cust_id
) , total_days_and_customers  as
(
    select sum(max_days) as summ , count(distinct cust_id)
    from max_days_per_customer 
)
-- Using subquery to get the disintct customer count as the total distinct customers are 20000 and the count that is returned from sub_query2 is 17991
select summ / (select count(distinct cust_id) from daily_transactions) as average_days
from total_days_and_customers ;

