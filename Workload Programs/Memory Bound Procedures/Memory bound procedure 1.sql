#Memory-bound Procedure 1 
######################################
#The Memory-bound procedure contains seven declarative statements. Two statements utilize the I/O cost because they are independent and consume stored fragments. It covers most of the processing costs of the procedures. The remaining statements utilize CPU costs because they are dependent and consume computed fragments from their producer statements. It also contains two parallel parts with two loop regions.
########################################
## a = 1;
 
## b = 2000;
 
## x = 2000;
 
## y = 4977;
##########################

CREATE OR REPLACE PROCEDURE wp_io_1(IN INT a, IN INT b, IN INT x, IN INT y) 

AS BEGIN

 
 Declare v2 table(
					d_year integer,
					brand_id integer, 
					brand varchar(50), 
					sum_agg decimal(18,2)
					);
 Declare v4 table(
					c_customer_sk integer, 
					d_year integer, 
					cnt BIGINT
					);
 v2 = 
		select 
				dt.d_year,
				i.i_brand_id brand_id, 
				i.i_brand brand,
				sum(ss_ext_sales_price) sum_agg 
		from 
				date_dim dt, 
				store_sales vs, 
				item i 
		where 
				dt.d_date_sk = vs.ss_sold_date_sk 
				and vs.ss_item_sk = i.i_item_sk 
		group by 
				dt.d_year,
				i.i_brand,
				i.i_brand_id 
		order by 
				dt.d_year,
				sum_agg desc,
				brand_id 
		WITH HINT(NO_INLINE);
 
 v3 = 
		select * 
		from :v2 
		where d_year between :a and :a + 1 
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
							d_year between :a and :a+1
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
												select avg(j.i_current_price) 
												from item j 
												where j.i_category = i.i_category
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
		:v5.d_year=:v3.d_year 
 WITH HINT(NO_INLINE);

END;