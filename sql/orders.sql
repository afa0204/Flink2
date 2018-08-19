drop table chris.orders;
create table chris.orders
(id	        	bigint not null primary key,
 order_date 	date not null,
 amount     	decimal(15,2),
 status         bigint,
 customer_id    bigint);

insert into chris.orders
(id, order_date, amount, status, customer_id)
values
(3000,DATE('2017-10-19'),192.74,100,1005);

insert into chris.orders
(id, order_date, amount, status, customer_id)
values
(3001,DATE('2017-11-10'),432.87,100,1000);

insert into chris.orders
(id, order_date, amount, status, customer_id)
values
(3002,DATE('2017-11-18'),22.19,100,1002);

insert into chris.orders
(id, order_date, amount, status, customer_id)
values
(3003,DATE('2017-05-06'),112.88,100,1002);

insert into chris.orders
(id, order_date, amount, status, customer_id)
values
(3004,DATE('2017-06-03'),28.15,100,1002);

insert into chris.orders
(id, order_date, amount, status, customer_id)
values
(3005,DATE('2017-03-08'),22.33,100,1000);

insert into chris.orders
(id, order_date, amount, status, customer_id)
values
(3006,DATE('2018-01-10'),11.88,100,1000);

insert into chris.orders
(id, order_date, amount, status, customer_id)
values
(3007,DATE('2018-02-05'),11.88,999,1001);

insert into chris.orders
(id, order_date, amount, status, customer_id)
values
(3008,DATE('2018-03-09'),432.11,100,1001);

insert into chris.orders
(id, order_date, amount, status, customer_id)
values
(3009,DATE('2018-04-20'),66.87,100,1007);

insert into chris.orders
(id, order_date, amount, status, customer_id)
values
(3010,DATE('2018-04-21'),74.31,200,1008);

insert into chris.orders
(id, order_date, amount, status, customer_id)
values
(3011,DATE('2018-07-19'),3.55,200,1008);
