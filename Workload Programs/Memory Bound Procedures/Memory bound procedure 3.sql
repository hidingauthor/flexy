#Memory-bound Procedure 3 
######################################
#The Memory-bound procedure contains eight declarative statements. Two statements utilize the I/O cost because they are independent and consume stored fragments. It covers most of the processing costs of the procedures. The remaining statements utilize CPU costs because they are dependent and consume computed fragments from their producer statements. It also contains two parallel parts with a loop region.
########################################
## x = 2000;
 
## y = 4977;
##########################

CREATE OR REPLACE PROCEDURE wp_io_3(IN INT x, IN INT y) 

AS BEGIN
 
 
 v2 = 
		select 
				d_year AS cs_sold_year, 
				cs_item_sk,
				cs_bill_customer_sk cs_customer_sk,
				sum(cs_quantity) cs_qty, 
				sum(cs_wholesale_cost) cs_wc,
				sum(cs_sales_price) cs_sp 
		from 
				catalog_sales 
					left join catalog_returns 
						on cr_order_number=cs_order_number and 
						cs_item_sk=cr_item_sk 
					join date_dim 
						on cs_sold_date_sk = d_date_sk 
		where 
				cr_order_number is null 
		group by 
				d_year, 
				cs_item_sk, 
				cs_bill_customer_sk;
 
 v4 = 
		select 
				TOP 1000 c.c_customer_sk, 
				d_year, 
				count(*) cnt 
		from 
				customer_address a, 
				customer c, 
				store_sales s, 
				date_dim d, 
				item i 
		where 
				a.ca_address_sk = c.c_current_addr_sk and 
				c.c_customer_sk = s.ss_customer_sk and 
				s.ss_sold_date_sk = d.d_date_sk and 
				s.ss_item_sk = i.i_item_sk and 
				i.i_current_price > 1.2 * (
											select 
													avg(j.i_current_price) 
											from 
													item j 
											where 
													j.i_category = i.i_category
											) 
		GROUP BY 
				c.c_customer_sk, 
				d_year;
 
 v5 = 
		select 
				* 
		from 
				:v2 
		WHERE 
				cs_sold_year between :a and :b;
 
 v6 = 
		select 
				* 
		from 
				:v4 
		WHERE 
				d_year between :a and :b;
 
 v7 = 
		select 
				:v6.c_customer_sk, 
				:v6.d_year 
		from 
				:v6, 
				:v5 
		WHERE 
				:v6.d_year = :v5.cs_sold_year;
 
 v8 = 
		SELECT 
				* 
		FROM 
				:v7 
		WHERE 
				c_customer_sk = :x;
 
 WHILE :x < :y DO
 
      v8 = 
			select 
					* 
			from 
					:v8 
			UNION ALL (
						SELECT 
								* 
						FROM 
								:v7 
						WHERE 
								c_customer_sk = :x
						);
						
      x =:x + 1;
	  
 END WHILE;
 
 select 
		count(*) 
 from 
		:v8;
 
END;