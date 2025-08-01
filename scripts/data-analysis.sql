--Analyzing sales performance over time
select 
year(order_date) as order_year,
month(order_date) as order_month,
SUM(sales_amount) as total_sales,
COUNT(distinct customer_key) as customer_count,
SUM(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by year(order_date), month(order_date)
order by year(order_date), month(order_date)

--Calculating the total sales per month and the running total of sales over time
select order_year,order_month,total_sales,SUM(total_sales) over (order by order_year, order_month) as running_total,
AVG(avg_price) over (order by order_year, order_month) as moving_average
from(
select year(order_date) as order_year, MONTH(order_date) as order_month, SUM(sales_amount) as total_sales,
AVG(price) as avg_price
from gold.fact_sales
where order_date is not null
group by year(order_date),MONTH(order_date)) t

/* Analyzing the yearly performance of products by comparing their sales to both the average sales performance of the product
and the previous year's sales */

with yearly_product_sales as(
select YEAR(f.order_date) as order_year,p.product_name, sum(f.sales_amount) as current_sales
from gold.fact_sales f left join gold.dim_products p
on f.product_key = p.product_key
where YEAR(order_date) is not null
group by YEAR(order_date), p.product_name)

select order_year, product_name, current_sales,
AVG(current_sales) over (partition by product_name) as avg_sales,
current_sales - AVG(current_sales) over (partition by product_name) as diff_avg,
case when current_sales -  AVG(current_sales) over (partition by product_name) > 0 then 'Above Avg'
 when current_sales -  AVG(current_sales) over (partition by product_name) < 0 then 'Below Avg'
 else 'Avg'
 end avg_change,
 LAG(current_sales) over (partition by product_name order by order_year) as prev_year_sales,
 current_sales -  LAG(current_sales) over (partition by product_name order by order_year) as diff_py,
 case when current_sales -  LAG(current_sales) over (partition by product_name order by order_year) > 0 then 'Increase'
 when current_sales -  LAG(current_sales) over (partition by product_name order by order_year) < 0 then 'Decrease'
 else 'No Change'
 end py_change
 from yearly_product_sales
 order by product_name, order_year

--Finding out which categories contribute the most to overall sales
with category_sales as(
select p.category, sum(f.sales_amount) as total_sales
from gold.fact_sales f left join gold.dim_products p
on p.product_key = f.product_key
group by category)

select category, total_sales, SUM(total_sales) over() as overall_sales,
concat(round((cast(total_sales AS float)/SUM(total_sales) over())*100,2), '%') as percentage_sales
from category_sales
order by total_sales desc

/*Segmenting products into cost ranges and counting how many products fall into each segment */

with cte as (
select product_key,
product_name,
cost,
case when cost<100 then 'Below 100'
when cost between 100 and 500 then '100-500'
when cost between 500 and 1000 then '500-1000'
else 'Above 1000'
end cost_range
from gold.dim_products)

select cost_range,COUNT(distinct product_key) as product_count
from cte
group by cost_range
order by product_count desc

/* Grouping customers into 3 segments based on their spending behavior:- 
VIP: at least 12 months of history and spending more than $5000
Regular: at least 12 months of history but spending $5000 or less
New: lifespan less than 12 months 
And find total number of customers by each group*/
with customer_spending as 
(select c.customer_id, c.first_name, c.last_name, MIN(s.order_date) as first_order,MAX(s.order_date) as last_order, 
datediff(month,MIN(s.order_date),MAX(s.order_date)) as customer_lifespan,
sum(s.sales_amount) as total_sales
from gold.fact_sales s left join gold.dim_customers c
on s.customer_key = c.customer_key
group by c.customer_id, c.first_name, c.last_name)


select COUNT(distinct customer_id) as total_customers, customer_type from(
select customer_id, first_name, last_name, 
case when customer_lifespan >=12 and total_sales <= 5000 then 'Regular'
when customer_lifespan >=12 and total_sales > 5000 then 'VIP'
else 'New'
end as customer_type
from customer_spending)cs
group by customer_type


