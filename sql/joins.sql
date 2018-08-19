
select first_name, last_name, order_date, amount
from customers c
inner join orders o
on c.id = o.customer_id

select first_name, last_name, order_date, amount
from customers c
inner join orders o
on c.id = o.customer_id
where amount > 100.00;


select first_name, last_name, order_date, amount
from customers c
left join orders o
on c.id = o.customer_id

select first_name, last_name, order_date, amount
from customers c
right join orders o
on c.id = o.customer_id

select first_name, last_name, order_date, amount
from customers c
full join orders o
on c.id = o.customer_id
