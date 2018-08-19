
select first_name, last_name, sum(amount) as sum_amounts
from customers c
inner join orders o
on c.id = o.customer_id
where o.customer_id = 1002
GROUP BY first_name, last_name;

select first_name, last_name, sum(amount) as sum_amounts
from customers c
inner join orders o
on c.id = o.customer_id
GROUP BY first_name, last_name;
