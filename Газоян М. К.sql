create table transaction (
  transaction_id integer primary key
  ,product_id integer
  ,customer_id integer 
  ,transaction_date varchar(30)
  ,online_order varchar(30)
  ,order_status varchar(30)
  ,brand varchar(30)
  ,product_line varchar(30)
  ,product_class varchar(30)
  ,product_size varchar(30)
  ,list_price float(4) 
  ,standard_cost float(4)
);

create table  customer( 
  customer_id integer primary key
  ,first_name varchar(50) 
  ,last_name varchar(50)
  ,gender varchar(30)  
  ,DOB varchar(50)
  ,job_title varchar(50)
  ,job_industry_category varchar(50)
  ,wealth_segment varchar(50)
  ,deceased_indicator varchar(50)
  ,owns_car varchar(30)
  ,address varchar(50)
  ,postcode varchar(50)
  ,state varchar(30)
  ,country varchar(30)
  ,property_valuation integer
); 

alter table transaction
add column tran_date DATE;

update transaction
set tran_date = TO_DATE(transaction_date, 'DD/MM/YY');

alter table transaction
drop column transaction_date;

alter table transaction
rename column tran_date TO transaction_date;


select count(customer_id)
	,job_industry_category 
from customer 
group by job_industry_category
order by count(customer_id) desc;

select count(transaction_id)
	,date_trunc('month', transaction_date) as month
	,customer.job_industry_category  
from transaction
join customer on customer.customer_id = transaction.customer_id
group by customer.job_industry_category, month
order by customer.job_industry_category, month;

select brand
	,count(online_order) 
from transaction
join customer on customer.customer_id = transaction.customer_id
where customer.job_industry_category = 'IT' and transaction.order_status='Approved' and online_order = 'True'
group by transaction.brand;

select distinct customer_id
	,sum(list_price)
	,max(list_price)
	,min(list_price)
	,count(*) 
from transaction 
group by customer_id
order by sum(list_price) desc, count(*) desc;

select distinct customer_id
	,sum(list_price) over (partition by customer_id) as total_transaction_amount
	,max(list_price) over (partition by customer_id) as max_transaction_amount
	,min(list_price) over (partition by customer_id) as min_transaction_amount
	,count(*) over (partition by customer_id) as transaction_count
from transaction 
order by total_transaction_amount desc, transaction_count desc;

select first_name
	,last_name
	,sum(list_price) as total_amount
from customer c 
join transaction t on c.customer_id = t.customer_id
group by c.customer_id, c.first_name, c.last_name
having sum(t.list_price) = (
	select min(total_amount) 
	from (
			select customer_id
				,sum(list_price) as total_amount
			from transaction
			where list_price is not null
			group by customer_id
		) min_amount
);
	
select first_name
	,last_name
	,sum(list_price) as total_amount
from customer c 
join transaction t on c.customer_id = t.customer_id
group by c.customer_id, c.first_name, c.last_name
having sum(t.list_price) = (
	select max(total_amount) 
	from (
			select customer_id
				,sum(list_price) as total_amount
			from transaction
			where list_price is not null
			group by customer_id
		) max_amount
);

select distinct
    customer_id,
    FIRST_VALUE(transaction_id) over (partition by customer_id order by transaction_date) as first_transaction_id,
    FIRST_VALUE(transaction_date) over (partition by customer_id order by transaction_date) as first_transaction_date,
    FIRST_VALUE(list_price) over (partition by customer_id order by transaction_date) as first_list_price
from transaction;

select
     customer_id
    ,first_name
    ,last_name
    ,job_title
    ,days_between_transactions
from (
    select
        customer_id
	    ,first_name
	    ,last_name
	    ,job_title
	    ,days_between_transactions
        ,rank() over (partition by customer_id order by days_between_transactions desc) as rnk
    from (
        select
            c.customer_id,
            c.first_name,
            c.last_name,
            c.job_title,
            t.transaction_date,
            coalesce(t.transaction_date - lag(t.transaction_date) over (partition by c.customer_id order by t.transaction_date), 0) as days_between_transactions
        from transaction t
        join customer c on c.customer_id = t.customer_id
    ) as RankedTransactions
) as FinalResults
where rnk = 1
order by customer_id, days_between_transactions DESC;




