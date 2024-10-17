#CPU-bound Procedure 3 
######################################
#The CPU-bound procedure contains 13 declarative statements. The majority of 9 statements utilize CPU costs because they are dependent and consume computed fragments from their producer statements. Only four statements utilize I/O costs because they are independent and consume stored fragments. It also contains two parallel parts with two loop regions.
########################################
## a = 1;
 
## b = 2000;
 
## x = 2000;
 
## y = 4977;
##########################

CREATE OR REPLACE PROCEDURE wp_cpu_3(IN INT a, IN INT b, IN INT x, IN INT y)

AS BEGIN


 date = 
		select 
				* 
		from 
				date_dim;
		
 sale = 
		select 
				* 
		from 
				catalog_sales;
		
 catalog_s = 
		select 
				* 
		from 
				catalog_returns;
		
 catalog_sale = 
		select 
				* 
		from 
				:sale 
					left join :catalog_s 
						on cr_order_number=cs_order_number;
 
 date_sale = 
		select 
				* 
		from 
				:sale 
					join :date 
						on cs_sold_date_sk = d_date_sk;

agg = 
		select 
				d_year AS cs_sold_year, 
				cs.cs_item_sk,
				cs.cs_bill_customer_sk cs_customer_sk,
				sum(cs.cs_quantity) cs_qty, 
				sum(cs.cs_wholesale_cost) cs_wc,
				sum(cs.cs_sales_price) cs_sp 
		from 
				:catalog_sale cs, 
				:date_sale 
		where 
				cr_order_number is null 
		group by 
				d_year, cs.cs_item_sk, 
				cs.cs_bill_customer_sk 
		WITH HINT(NO_INLINE);
 
 v2 = 
		select 
				* 
		from 
				:agg 
		WITH HINT(NO_INLINE);
 
 v3 = 
		select 
				* 
		from 
				:v2 
		where 
				cs_sold_year between :a and :a + 1 
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
					from :
							v2 
					where 
							cs_sold_year between :a and :a+1
					) 
		WITH HINT(NO_INLINE);
 
 a =:a + 1;
 
 END WHILE;
 
 v4 = 
		select 
				c.c_customer_sk, 
				d_year, 
				count(*) cnt 
		from 
				customer_address a, 
				customer c, 
				store_sales s, 
				date_dim d, 
				item i 
		where 
				a.ca_address_sk = c.c_current_addr_sk 
				and c.c_customer_sk = s.ss_customer_sk 
				and s.ss_sold_date_sk = d.d_date_sk 
				and s.ss_item_sk = i.i_item_sk 
				and i.i_current_price > 1.2 * (
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
								c_customer_sk = :x
						) 
			WITH HINT(NO_INLINE);
 
 x =:x + 1;
 
 END WHILE;
 
 select 
		count(*) 
 from 
		:v5, 
		:v3 
 WHERE 
		:v5.d_year=:v3.cs_sold_year 
 WITH HINT(NO_INLINE);
 
END;