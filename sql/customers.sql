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
 (1001,'Fred','Jenkins','USA','100 Main Street','Reston','VA','20194');
 
 insert into chris.customers
 (id,first_name,last_name,country,street_address1,city,state,zip)
 values
 (1002,'Susan','Smith','USA','200 Park Street','Herndon','VA','20170');
 
 insert into chris.customers
 (id,first_name,last_name,country,street_address1,city,state,zip)
 values
 (1003,'Larry','Jones','USA','150 Floris Ave','Herndon','VA','20171');
 
 insert into chris.customers
 (id,first_name,last_name,country,street_address1,city,state,zip)
 values
 (1004,'Harry','Brown','USA','12 Ralph Street','Hillsdale','NJ','07642');
 
 insert into chris.customers
 (id,first_name,last_name,country,street_address1,city,state,zip)
 values
 (1005,'Gary','Heiser','USA','29 River Street','Hillsdale','NJ','07642');
 
 insert into chris.customers
 (id,first_name,last_name,country,street_address1,city,state,zip)
 values
 (1006,'Mitch','Grey','USA','12 Knights Ave','Beaverton','PA','48271');
 
 insert into chris.customers
 (id,first_name,last_name,country,street_address1,city,state,zip)
 values
 (1007,'Melanie','Richards','USA','33 Palm Street','Tampa Bay','FL','38291');
 
 insert into chris.customers
 (id,first_name,last_name,country,street_address1,city,state,zip)
 values
 (1008,'Tony','Johnson','USA','44 Ranch Street','Houston','TX','62452');
 