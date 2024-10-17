#Memory-bound Procedure 5 
######################################
#The Memory-bound procedure contains eight declarative statements. Two statements utilize the I/O cost because they are independent and consume stored fragments. It covers most of the processing costs of the procedures. The remaining statements utilize CPU costs because they are dependent and consume computed fragments from their producer statements. It also contains two parallel parts with a loop region.
########################################

## x = 2000;
 
## y = 4977;
##########################

CREATE OR REPLACE PROCEDURE wp_io_5(IN INT x, IN INT y)

AS BEGIN

 
 year_total = 
				select 
						c_customer_id customer_id,
						c_first_name customer_first_name,
						c_last_name customer_last_name,
						c_preferred_cust_flag customer_preferred_cust_flag,
						c_birth_country customer_birth_country,
						c_login customer_login,
						c_email_address customer_email_address,
						d_year dyear,
						sum(ss_ext_list_price-ss_ext_discount_amt) year_total,
						's' sale_type 
				from 
						customer,
						store_sales,
						date_dim 
				where 
						c_customer_sk = ss_customer_sk 
						and ss_sold_date_sk = d_date_sk 
				group by 
						c_customer_id, 
						c_first_name,
						c_last_name,
						c_preferred_cust_flag ,
						c_birth_country,
						c_login,
						c_email_address,
						d_year 
				union all 
							select 
									c_customer_id customer_id,
									c_first_name customer_first_name,
									c_last_name customer_last_name,
									c_preferred_cust_flag customer_preferred_cust_flag,
									c_birth_country customer_birth_country,
									c_login customer_login,
									c_email_address customer_email_address,
									d_year dyear,
									sum(ws_ext_list_price-ws_ext_discount_amt) year_total,
									'w' sale_type 
							from 
									customer,
									web_sales,
									date_dim 
							where 
									c_customer_sk = ws_bill_customer_sk 
									and ws_sold_date_sk = d_date_sk 
							group by 
									c_customer_id,
									c_first_name,
									c_last_name,
									c_preferred_cust_flag ,
									c_birth_country,
									c_login,
									c_email_address,
									d_year;
 
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
							when t_w_firstyear.year_total > 0 
							then t_w_secyear.year_total / t_w_firstyear.year_total 
							else 0.0 
							end 
						> case 
							when t_s_firstyear.year_total > 0 
							then t_s_secyear.year_total / t_s_firstyear.year_total 
							else 0.0 
							end 
				order by 
						t_s_secyear.customer_id,
						t_s_secyear.customer_first_name,
						t_s_secyear.customer_last_name, 
						t_s_firstyear.dyear;
 
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
						d_year;
 
 v5 = 
				select 
						* 
				from 
						:v2 
				WHERE 
						dyear between :a and :b;
 
 v6 = 
				select 
						* 
				from 
						:v4 
				WHERE 
						d_year between :a and :b;
 
 v7 = 
				select 
						:v6.c_customer_sk, :v6.d_year 
				from 
						:v6, 
						:v5 
				WHERE 
						:v6.d_year = :v5.dyear;
 
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