#CPU-bound Procedure 6 
######################################
#The CPU-bound procedure contains 20 declarative statements. The majority of 16 statements utilize CPU costs because they are dependent and consume computed fragments from their producer statements. Only four statements utilize I/O costs because they are independent and consume stored fragments. It also contains two parallel parts with two loop regions.
########################################
## a = 1;
 
## b = 2000;
 
## x = 2000;
 
## y = 4977;
##########################

CREATE OR REPLACE PROCEDURE wp_cpu_6(IN INT a, IN INT b, IN INT x, IN INT y) 

AS BEGIN

 
 store_sale = 
					select 
							* 
					from 
							store_sales;
 
 date = 			
					select 
							* 
					from 
							date_dim;
 
 customer = 
					select 
							* 
					from 
							customer_address;
 
 sale_date = 
					select 
							* 
					from 
							:store_sale, 
							:date 
					where 
							ss_sold_date_sk = d_date_sk;
 
 customer_sale = 
					select 
							* 
					from 
							:store_sale, :customer 
					where 
							ss_addr_sk=ca_address_sk;
 
 agg1 = 
					select 
							c.ca_county,
							s.d_qoy, 
							s.d_year,
							sum(s.ss_ext_sales_price) as store_sales 
					from 
							:sale_date s,
							:customer_sale c 
					group by 
							c.ca_county,
							s.d_qoy, 
							s.d_year;
 
 ss = 
					select 
							* 
					from 
							:agg1 
					WITH HINT(NO_INLINE);
					
 web_sale = 
					select 
							* 
					from 
							web_sales;
 
 w_date = 
					select 
							* 
					from 
							date_dim;
 
 w_customer = 
					select 
							* 
					from 
							customer_address;
 
 websale_date = 
					select 
							* 
					from 
							:web_sale, 
							:w_date 
					where 
							ws_sold_date_sk = d_date_sk;
 
 customer_websale = 
					select 
							* 
					from 
							:web_sale, :w_customer 
					where 
							ws_bill_addr_sk=ca_address_sk;
 
 ws = 
					select 
							w.ca_county,
							d.d_qoy, 
							d.d_year,
							sum(d.ws_ext_sales_price) as web_sales 
					from 
							:customer_websale w, 
							:websale_date d 
					group by 
							w.ca_county,
							d.d_qoy, 
							d.d_year 
					WITH HINT(NO_INLINE);
 v2 = 
					select 
							ss1.ca_county,
							ss1.d_year,
							ws2.web_sales/ws1.web_sales web_q1_q2_increase,
							ss2.store_sales/ss1.store_sales store_q1_q2_increase,
							ws3.web_sales/ws2.web_sales web_q2_q3_increase,
							ss3.store_sales/ss2.store_sales store_q2_q3_increase 
					from 
							:ss ss1,
							:ss ss2,
							:ss ss3,
							:ws ws1,
							:ws ws2,
							:ws ws3 
					where 
							ss1.d_qoy = 1 
							and ss1.d_year = 2002 
							and ss1.ca_county = ss2.ca_county 
							and ss2.d_qoy = 2 
							and ss2.d_year = 2002 
							and ss2.ca_county = ss3.ca_county 
							and ss3.d_qoy = 3 
							and ss3.d_year = 2002 
							and ss1.ca_county = ws1.ca_county 
							and ws1.d_qoy = 1 
							and ws1.d_year = 2002 
							and ws1.ca_county = ws2.ca_county 
							and ws2.d_qoy = 2 
							and ws2.d_year = 2002 
							and ws1.ca_county = ws3.ca_county 
							and ws3.d_qoy = 3 
							and ws3.d_year =2002 
							and 
								case 
									when 
										ws1.web_sales > 0 
									then 
										ws2.web_sales/ws1.web_sales 
									else 
										null 
									end 
								> case 
									when 
										ss1.store_sales > 0 
									then 
										ss2.store_sales/ss1.store_sales 
									else 
										null 
									end 
							and 
								case 
									when 
										ws2.web_sales > 0 
									then 
										ws3.web_sales/ws2.web_sales 
									else 
										null 
									end 
								> case 
									when 
										ss2.store_sales > 0 
									then 
										ss3.store_sales/ss2.store_sales 
									else 
										null 
									end 
					order by 
							ss1.ca_county,ss1.d_year, 
							web_q1_q2_increase, 
							store_q1_q2_increase, 
							web_q2_q3_increase, 
							store_q2_q3_increase 
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
		:v5.d_year=:v3.d_year 
 WITH HINT(NO_INLINE);
 
END;