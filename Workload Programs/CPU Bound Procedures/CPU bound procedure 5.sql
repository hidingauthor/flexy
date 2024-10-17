#CPU-bound Procedure 5 
######################################
#The CPU-bound procedure contains 16 declarative statements. The majority of 11 statements utilize CPU costs because they are dependent and consume computed fragments from their producer statements. Only five statements utilize I/O costs because they are independent and consume stored fragments. It also contains two parallel parts with two loop regions.
########################################
## a = 1;
 
## b = 2000;
 
## x = 2000;
 
## y = 4977;
##########################

CREATE OR REPLACE PROCE DURE wp_cpu_5(IN INT a, IN INT b, IN INT x, IN INT y) 

AS BEGIN


 customer = 
				select 
						* 
				from 
						customer;
				
 sale = 
				select 
						* 
				from 
						store_sales;
				
 websale = 
				select 
						* 
				from 
						web_sales;
 
 date = 
				select 
						* 
				from 
						date_dim;
				
 customer_sale = 
				select 
						* 
				from 
						:customer, 
						:sale 
				where 
						c_customer_sk = ss_customer_sk;
				
 date_sale = 
				select 
						* 
				from 
						:sale, 
						:date 
				where 
						ss_sold_date_sk = d_date_sk;
 
 customer_websale = 
				select 
						* 
				from 
						:customer, 
						:websale 
				where 
						c_customer_sk = ws_bill_customer_sk;
				
 date_websale = 
				select 
						* 
				from 
						:websale, 
						:date 
				where 
						ws_sold_date_sk = d_date_sk;
 
 year_total = 
				select 
						c.c_customer_id customer_id,
						c.c_first_name customer_first_name,
						c.c_last_name customer_last_name,
						c.c_preferred_cust_flag customer_preferred_cust_flag,
						c.c_birth_country customer_birth_country,c.c_login customer_login,
						c.c_email_address customer_email_address,d.d_year dyear,
						sum(c.ss_ext_list_price-c.ss_ext_discount_amt) year_total,
						's' sale_type 
				from 
						:customer_sale c, 
						:date_sale d 
				group by 
						c.c_customer_id, 
						c.c_first_name,
						c.c_last_name,
						c.c_preferred_cust_flag ,
						c.c_birth_country,
						c.c_login,
						c.c_email_address,
						d.d_year 
				union all 
						select 
								c1.c_customer_id customer_id,
								c1.c_first_name customer_first_name,
								c1.c_last_name customer_last_name,
								c1.c_preferred_cust_flag customer_preferred_cust_flag,
								c1.c_birth_country customer_birth_country,
								c1.c_login customer_login,
								c1.c_email_address customer_email_address,
								d1.d_year dyear,
								sum(c1.ws_ext_list_price-c1.ws_ext_discount_amt) year_total,
								'w' sale_type 
						from 
								:customer_websale c1, 
								:date_websale d1 
						group by 
								c1.c_customer_id,
								c1.c_first_name,
								c1.c_last_name,
								c1.c_preferred_cust_flag ,
								c1.c_birth_country,
								c1.c_login,
								c1.c_email_address,
								d1.d_year 
				WITH HINT(NO_INLINE);
 
 v2 = 
			select 
					t_s_secyear.customer_id,
					t_s_secyear.customer_first_name,
					t_s_secyear.customer_last_name, 
					t_s_firstyear.dyear 
			from 
					:year_total t_s_firstyear,
					:year_total t_s_secyear,
					:year_total t_w_firstyear,
					:year_total t_w_secyear 
			where 
					t_s_secyear.customer_id = t_s_firstyear.customer_id 
					and t_s_firstyear.customer_id = t_w_secyear.customer_id 
					and t_s_firstyear.customer_id = t_w_firstyear.customer_id 
					and t_s_firstyear.sale_type = 's' 
					and t_w_firstyear.sale_type = 'w' 
					and t_s_secyear.sale_type = 's' 
					and t_w_secyear.sale_type = 'w' 
					and t_s_firstyear.dyear = 1998 
					and t_s_secyear.dyear = 1998+1 
					and t_w_firstyear.dyear = 1998 
					and t_w_secyear.dyear = 1998+1 
					and t_s_firstyear.year_total > 0 
					and t_w_firstyear.year_total > 0 
					and case 
							when 
								t_w_firstyear.year_total > 0 
							then 
								t_w_secyear.year_total / t_w_firstyear.year_total 
							else 
								0.0 
							end 
						> case 
							when 
								t_s_firstyear.year_total > 0 
							then 
								t_s_secyear.year_total / t_s_firstyear.year_total 
							else 
								0.0 
							end 
			order by 
					t_s_secyear.customer_id,
					t_s_secyear.customer_first_name,
					t_s_secyear.customer_last_name, 
					t_s_firstyear.dyear 
			WITH HINT(NO_INLINE);
			
 v3 = 
			select 
					* 
			from 
					:v2 
			where 
					dyear between :a and :a + 1 
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
								dyear between :a and :a+1
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
			:v5.d_year=:v3.dyear 
 WITH HINT(NO_INLINE);
			
END;