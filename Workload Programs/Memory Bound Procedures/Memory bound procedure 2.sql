#Memory-bound Procedure 2 
######################################
#The Memory-bound procedure contains seven declarative statements. Two statements utilize the I/O cost because they are independent and consume stored fragments. It covers most of the processing costs of the procedures. The remaining statements utilize CPU costs because they are dependent and consume computed fragments from their producer statements. It also contains two parallel parts with two loop regions.
########################################
## a = 1;
 
## b = 2000;
 
## x = 2000;
 
## y = 4977;
##########################

CREATE OR REPLACE PROCEDURE wp_io_2(IN INT a, IN INT b, IN INT x, IN INT y) 

AS BEGIN

 
 v2 = 
		select 
				d_year AS ws_sold_year, 
				ws_item_sk,
				ws_bill_customer_sk ws_customer_sk,
				sum(ws_quantity) ws_qty,
				sum(ws_wholesale_cost) ws_wc,
				sum(ws_sales_price) ws_sp 
		from 
				web_sales 
						left join web_returns 
							on wr_order_number=ws_order_number and 
							ws_item_sk=wr_item_sk 
						join date_dim 
							on ws_sold_date_sk = d_date_sk 
		where 
				wr_order_number is null 
		
		group by 
				d_year, 
				ws_item_sk,
				ws_bill_customer_sk 
		WITH HINT(NO_INLINE);
 
 v3 = 
		select 
				* 
		from 
				:v2 
		where 
				ws_sold_year between :a and :a + 5 
		WITH HINT(NO_INLINE);
 
 WHILE :a < :b DO
  
  v3 = 
		select 
				* 
		from 
				:v3 
		UNION ALL (
					select 
							* 
					from 
							:v2 
					where 
							ws_sold_year between :a and :a+5
					) 
					WITH HINT(NO_INLINE);
 
 a =:a + 5;
 
 END WHILE;
 
 v4 = 
		select 
				c.c_customer_sk, 
				d_year, count(*) cnt 
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
				d_year 
		WITH HINT(NO_INLINE);
 
 v5 = 
		SELECT 
				* 
		FROM 
				:v4 
		WHERE 
				c_customer_sk = :x 
		WITH HINT(NO_INLINE);
 
 WHILE :x < :y DO
   
   v5 = 
		select 
				* 
		from 
				:v5 
		UNION ALL (
					SELECT 
							* 
					FROM 
							:v4 
					WHERE 
							c_customer_sk = :x) 
					WITH HINT(NO_INLINE);
 
 x =:x + 1;
 
 END WHILE;
 
 select 
		count(*) 
 from 
		:v5, 
		:v3 
 WHERE 
		:v5.d_year=:v3.ws_sold_year 
 WITH HINT(NO_INLINE);

END;