
drop table chris.orders;
create table chris.orders
(id	        	bigint not null primary key,
 order_date 	date not null,
 amount     	decimal(15,2),
 customer_id    bigint);

drop table chris.customers; 
create table chris.customers
(id	        	 bigint not null primary key,
 first_name      varchar(40),
 last_name       varchar(40),
 country         varchar(40),
 street_address1 varchar(40),
 city            varchar(40),
 state           varchar(40),
 zip             varchar(40)
 );
 
 insert into chris.customers
 (id,first_name,last_name,country,street_address1,city,state,zip)
 values
 (1000,'Fred','Jenkins','USA','100 Main Street','Reston','VA','20171');
 
 insert into chris.customers
 (id,first_name,last_name,country,street_address1,city,state,zip)
 values
 (1001,'Susan','Smith','USA','200 Park St','Herndon','VA','20170');
 
 insert into chris.customers
 (id,first_name,last_name,country,street_address1,city,state,zip)
 values
 (1002,'John','Green','Canada','100 Quebec Ave','','','33221-55');
 

insert into chris.orders
(id, order_date, amount, customer_id)
values
(2000,CURRENT_DATE,432.87,1000);

insert into chris.orders
(id, order_date, amount, customer_id)
values
(2001,CURRENT_DATE,22.19,1002);

insert into chris.orders
(id, order_date, amount, customer_id)
values
(2002,CURRENT_DATE,112.88,1002);

insert into chris.orders
(id, order_date, amount, customer_id)
values
(2003,CURRENT_DATE,28.15,1002);

insert into chris.orders
(id, order_date, amount, customer_id)
values
(2004,CURRENT_DATE,22.33,1000);

insert into chris.orders
(id, order_date, amount, customer_id)
values
(2006,CURRENT_DATE,11.88,1001);


