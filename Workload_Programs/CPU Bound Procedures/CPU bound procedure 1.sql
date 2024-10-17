#CPU-bound Procedure 1
######################################
#The CPU-bound procedure contains 35 declarative statements. The majority of 23 statements utilize CPU costs because they are dependent and consume computed fragments from their producer statements. Only 12 statements utilize I/O costs because they are independent and consume stored fragments. It also contains five parallel parts with five loop regions.
########################################
 #a  = 1;
 
# b = 2000;
 
# c = 1;
 
# d = 2000; 
 
# m = 1;
 
# n = 2000;
 
# o = 1;
 
# p = 2000;
 
# x = 4000;
 
# y = 4977;
##########################

CREATE OR REPLACE PROCEDURE wp_cpu_1(IN INT a, IN INT b, IN INT c, IN INT d, IN INT m, IN INT n, IN INT o, IN INT p, IN INT x, IN INT y) 

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
 
 date = 
		select 
				* 
		from 
				date_dim;
 
 sale = 
		select 
				* 
		from 
				store_sales;
 
 item_s = 
		select 
				* 
		from 
				item ;
 
 item_sale = 
		select 
				* 
		from 
				:item_s i, 
				:sale vs 
		where 
				vs.ss_item_sk = i.i_item_sk;
 
 date_sale = 
		select 
				* 
		from 
				:date dt, 
				:sale vs 
		where 
				dt.d_date_sk = vs.ss_sold_date_sk;
 
 agg = 
		select 
				dt.d_year,
				i.i_brand_id brand_id, 
				i.i_brand brand,
				sum(i.ss_ext_sales_price) sum_agg 
		from 
				:item_sale i, 
				:date_sale dt 
		group by 
				dt.d_year,
				i.i_brand,
				i.i_brand_id 
		order by 
				dt.d_year,
				sum_agg desc,
				brand_id;
 
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
				d_year between :a and :a + 1 
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
 
 date1 = 
		select 
				* 
		from 
				date_dim;
 
 sale1 = 
		select 
				* 
		from 
			store_sales;
 
 item_s1 = 
		select 
				* 
		from 
				item;
 
 item_sale1 = 
		select 
				* 
		from 
				:item_s1 i, :sale1 vs 
		where 
				vs.ss_item_sk = i.i_item_sk;
 
 date_sale1 = 
		select 
				* 
		from 
				:date1 dt, 
				:sale1 vs 
		where 
				dt.d_date_sk = vs.ss_sold_date_sk;
 
 v7 = 
		select 
				dt.d_year,
				i.i_brand_id brand_id, 
				i.i_brand brand,
				sum(i.ss_ext_sales_price) sum_agg 
		from 
				:item_sale1 i, 
				:date_sale1 dt 
		group by 
				dt.d_year,
				i.i_brand,
				i.i_brand_id 
		order by 
				dt.d_year,
				sum_agg desc,
				brand_id 
		WITH HINT(NO_INLINE);
 
 v8 = 
		select 
				* 
		from 
				:v7 
		where 
				d_year between :c and :c + 1 
		WITH HINT(NO_INLINE);
 
 WHILE :c < :d DO
  
  v8 = 
		select 
				* 
		from 
				:v8 
		UNION ALL (
					select 
							* 
					from 
							:v7 
					where 
							d_year between :c and :c+1
					) 
		WITH HINT(NO_INLINE);
 
 c =:c + 1;
 
 END WHILE;
 
 date2 = 
		select 
				* 
		from 
				date_dim;
 
 sale2 = 
		select 
				* 
		from 
				store_sales;
 
 item_s2 = 
		select 
				* 
		from 
				item;
 
 item_sale2 = 
		select 
				* 
		from 
				:item_s2 i, 
				:sale2 vs 
		where 
				vs.ss_item_sk = i.i_item_sk;
 
 date_sale2 = 
		select 
				* 
		from 
				:date2 dt, 
				:sale2 vs 
		where 
				dt.d_date_sk = vs.ss_sold_date_sk;
 
 v9 = 
		select 
				dt.d_year,
				i.i_brand_id brand_id, 
				i.i_brand brand,
				sum(i.ss_ext_sales_price) sum_agg 
		from 
				:item_sale2 i, 
				:date_sale2 dt 
		group by 
				dt.d_year,
				i.i_brand,
				i.i_brand_id 
		order by 
				dt.d_year,sum_agg desc,
				brand_id 
		WITH HINT(NO_INLINE);
 
 v10 = 
		select 
				* 
		from 
				:v9 
		where 
				d_year between :m and :m + 1 
		WITH HINT(NO_INLINE);
 
 WHILE :m < :n DO
  
  v10 = 
		select 
				* 
		from 
				:v10 
		UNION ALL (
					select 
							* 
					from 
							:v9 
					where 
							d_year between :m and :m+1) 
		WITH HINT(NO_INLINE);
 
 m =:m + 1;
 
 END WHILE;
 
 date3 = 
		select 
				* 
		from 
				date_dim;
 
 sale3 = 
		select 
				* 
		from 
				store_sales;
 
 item_s3 = 
		select 
				* 
		from 
				item;
 
 item_sale3 = 
		select 
				* 
		from 
				:item_s3 i, 
				:sale3 vs 
		where 
				vs.ss_item_sk = i.i_item_sk;
 
 date_sale3 = 
		select 
				* 
		from 
				:date3 dt, 
				:sale3 vs 
		where 
				dt.d_date_sk = vs.ss_sold_date_sk;
 
 v11 = 
		select 
				dt.d_year,
				i.i_brand_id brand_id, 
				i.i_brand brand,
				sum(i.ss_ext_sales_price) sum_agg 
		from 
				:item_sale2 i, 
				:date_sale2 dt 
		group by 
				dt.d_year,
				i.i_brand,
				i.i_brand_id 
		order by 
				dt.d_year,
				sum_agg desc,
				brand_id 
		WITH HINT(NO_INLINE);
 
 v12 = 
		select 
				* 
		from 
				:v11 
		where 
				d_year between :o and :o + 1 
		WITH HINT(NO_INLINE);
 
 WHILE :o < :p DO
  
  v12 = 
		select 
				* 
		from 
				:v12 
		UNION ALL (
					select 
							* 
					from 
							:v11 
					where 
							d_year between :o and :o+1
					) 
		WITH HINT(NO_INLINE);
 
 o =:o + 1;
 
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
		:v3, 
		:v8, 
		:v10, 
		:v12 
 WHERE 
		:v5.d_year=:v3.d_year 
		and :v5.d_year=:v8.d_year 
		and :v5.d_year=:v10.d_year 
		and :v5.d_year=:v12.d_year 
 WITH HINT(NO_INLINE);

END;