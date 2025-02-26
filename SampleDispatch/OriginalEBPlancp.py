################################
###### Traditional Dispatch #############
#####################################
################################
###### A sample demnostration of EB dispatch in base load test with 40 program invocations #############
#####################################
from FederationConnector import FederationConnector

import time
import asyncio

class OriginalEBPlancp:
    def __init__(self):
        pass

    async def manual_execution(self):
        fc = FederationConnector()
        fc.create_federation_connection()

        start_time = time.time() * 1000



        # Workload in base load
        # we broadcast them
        await asyncio.gather(self.wp11_exe(fc, 1), self.wp11_exe(fc, 2), self.wp11_exe(fc, 3), self.wp11_exe(fc, 3),self.wp11_exe(fc, 3),self.wp11_exe(fc, 3),self.wp11_exe(fc, 1),self.wp11_exe(fc, 1),self.wp11_exe(fc, 1),self.wp11_exe(fc,1),self.wp11_exe(fc, 3),
                             self.wp21_exe(fc, 1), self.wp21_exe(fc, 1), self.wp21_exe(fc, 1),self.wp21_exe(fc, 1),self.wp21_exe(fc, 1), self.wp21_exe(fc, 1), self.wp21_exe(fc, 1), self.wp21_exe(fc, 1),self.wp21_exe(fc, 1),self.wp21_exe(fc, 1),self.wp21_exe(fc, 1), self.wp21_exe(fc, 1), self.wp21_exe(fc, 1),self.wp21_exe(fc, 1),self.wp21_exe(fc, 1),
                             self.wp21a_exe(fc, 3), self.wp21a_exe(fc, 3), self.wp21a_exe(fc, 3), self.wp21a_exe(fc, 3), self.wp21a_exe(fc, 3),
                             self.wp21b_exe(fc, 2), self.wp21b_exe(fc, 2),
                             self.wpx1_exe(fc, 1), self.wpx1_exe(fc, 1), self.wpx1_exe(fc, 1),
                             self.wpy1_exe(fc, 1), self.wpy1_exe(fc, 1), self.wpy1_exe(fc, 1), self.wpy1_exe(fc, 1), self.wpy1_exe(fc, 1), self.wpy1_exe(fc, 1)
                             )



        end_time = time.time() * 1000
        compile_time = end_time - start_time
        # time.sleep(2)
        print("Execution time: {0}".format(round(compile_time, 3)))
        fc.disconnect_federation()


    #####################################
    ############ Program 1 ####################
    ###################################
    async def wp11_exe(self, fc, node):
        self.wp11(fc, node)
        self.wp11_call(fc, node)



    def wp11(self, fc, node):

        wp11 = "CREATE OR REPLACE PROCEDURE wp_11()" \
               " AS BEGIN\n" \
              " DECLARE a INT := 1900;\n" \
              " DECLARE b INT := 2000;\n" \
              " DECLARE x INT := 4970;\n" \
              " DECLARE y INT := 4977;\n" \
              " Declare v2 table(d_year integer,brand_id integer, brand varchar(50), sum_agg decimal(18,2));\n" \
              " Declare v4 table(c_customer_sk integer, d_year integer, cnt BIGINT);\n" \
              " v2 = select dt.d_year,i.i_brand_id brand_id, i.i_brand brand,sum(ss_ext_sales_price) sum_agg from date_dim dt, store_sales vs, item i where dt.d_date_sk = vs.ss_sold_date_sk and vs.ss_item_sk = i.i_item_sk group by dt.d_year,i.i_brand,i.i_brand_id order by dt.d_year,sum_agg desc,brand_id;\n" \
              " v4 = select c.c_customer_sk, d_year, count(*) cnt from customer_address a, customer c, store_sales s, date_dim d, item i where a.ca_address_sk = c.c_current_addr_sk and c.c_customer_sk = s.ss_customer_sk and s.ss_sold_date_sk = d.d_date_sk and s.ss_item_sk = i.i_item_sk and i.i_current_price > 1.2 * (select avg(j.i_current_price) from item j where j.i_category = i.i_category) GROUP BY c.c_customer_sk, d_year;\n" \
              " v5 = select :v4.c_customer_sk, :v4.d_year from :v2, :v4 WHERE :v2.d_year=:v4.d_year;\n" \
              " v6 = SELECT * FROM :v5 WHERE c_customer_sk = :x;\n" \
              " WHILE :x < :y DO\n" \
              "      v6 = select * from :v6 UNION ALL (SELECT * FROM :v5 WHERE c_customer_sk = :x);\n" \
              "      x =:x + 1;\n" \
              " END WHILE;\n" \
              " select count(*) from :v6;\n" \
              "END;"
        fc.execute_query(node, wp11, True)


    def wp11_call(self, fc, node):

        fc.execute_query(node, "CALL wp_11()", True)
        fc.execute_query(node, "DROP procedure wp_11", True)




    #####################################
    ############ Program2 ####################
    ###################################

    async def wp21_exe(self, fc, node):
        self.wp21(fc, node)
        self.wp21_call(fc, node)

    def wp21(self,fc, node):

        wp21 = "CREATE OR REPLACE PROCEDURE wp_21()" \
               " AS BEGIN\n" \
               " DECLARE a INT := 1700;\n" \
               " DECLARE b INT := 2000;\n" \
               " DECLARE x INT := 4970;\n" \
               " DECLARE y INT := 4977;\n" \
               " v2 = select top 1000000 d_year AS ws_sold_year, ws_item_sk,ws_bill_customer_sk ws_customer_sk,sum(ws_quantity) ws_qty,sum(ws_wholesale_cost) ws_wc,sum(ws_sales_price) ws_sp from web_sales left join web_returns on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk join date_dim on ws_sold_date_sk = d_date_sk where wr_order_number is null group by d_year, ws_item_sk,ws_bill_customer_sk order by d_year desc, ws_item_sk;\n" \
               " v4 = select top 10000 c.c_customer_sk, d_year, count(*) cnt from customer_address a, customer c, store_sales s, date_dim d, item i where a.ca_address_sk = c.c_current_addr_sk and c.c_customer_sk = s.ss_customer_sk and s.ss_sold_date_sk = d.d_date_sk and s.ss_item_sk = i.i_item_sk and i.i_current_price > 1.2 * (select avg(j.i_current_price) from item j where j.i_category = i.i_category) GROUP BY c.c_customer_sk, d_year;\n" \
               " v5 = select * from :v2 WHERE ws_sold_year between :a and :b;\n" \
               " v6 = select * from :v4 WHERE d_year between :a and :b;\n" \
               " v7 = select :v6.c_customer_sk, :v6.d_year from :v6, :v5 WHERE :v6.d_year = :v5.ws_sold_year;\n" \
               " v8 = SELECT * FROM :v7 WHERE c_customer_sk = :x;\n" \
               " WHILE :x < :y DO\n" \
               "      v8 = select * from :v8 UNION ALL (SELECT * FROM :v7 WHERE c_customer_sk = :x);\n" \
               "      x =:x + 1;\n" \
               " END WHILE;\n" \
               " select count(*) from :v8;\n" \
               "END;"

        fc.execute_query(node, wp21, True)
        # fc.execute_query(node, "CALL wp_21()", True)

    def wp21_call(self, fc, node):

        fc.execute_query(node, "CALL wp_21()", True)
        fc.execute_query(node, "DROP procedure wp_21", True)

    #####################################
    ############ Program 3 ####################
    ###################################
    async def wp21a_exe(self, fc, node):
        self.wp21a(fc, node)
        self.wp21a_call(fc, node)

    def wp21a(self, fc, node):
        wp21a = "CREATE OR REPLACE PROCEDURE wp_21a()" \
               " AS BEGIN\n" \
               " DECLARE a INT := 1900;\n" \
               " DECLARE b INT := 2000;\n" \
               " DECLARE x INT := 4900;\n" \
               " DECLARE y INT := 4977;\n" \
               " v2 = select d_year AS cs_sold_year, cs_item_sk,cs_bill_customer_sk cs_customer_sk,sum(cs_quantity) cs_qty, sum(cs_wholesale_cost) cs_wc,sum(cs_sales_price) cs_sp from catalog_sales left join catalog_returns on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk join date_dim on cs_sold_date_sk = d_date_sk where cr_order_number is null group by d_year, cs_item_sk, cs_bill_customer_sk;\n" \
               " v4 = select TOP 1000 c.c_customer_sk, d_year, count(*) cnt from customer_address a, customer c, store_sales s, date_dim d, item i where a.ca_address_sk = c.c_current_addr_sk and c.c_customer_sk = s.ss_customer_sk and s.ss_sold_date_sk = d.d_date_sk and s.ss_item_sk = i.i_item_sk and i.i_current_price > 1.2 * (select avg(j.i_current_price) from item j where j.i_category = i.i_category) GROUP BY c.c_customer_sk, d_year;\n" \
               " v5 = select * from :v2 WHERE cs_sold_year between :a and :b;\n" \
               " v6 = select * from :v4 WHERE d_year between :a and :b;\n" \
               " v7 = select :v6.c_customer_sk, :v6.d_year from :v6, :v5 WHERE :v6.d_year = :v5.cs_sold_year;\n" \
               " v8 = SELECT * FROM :v7 WHERE c_customer_sk = :x;\n" \
               " WHILE :x < :y DO\n" \
               "      v8 = select * from :v8 UNION ALL (SELECT * FROM :v7 WHERE c_customer_sk = :x);\n" \
               "      x =:x + 1;\n" \
               " END WHILE;\n" \
               " select count(*) from :v8;\n" \
               "END;"


        fc.execute_query(node, wp21a, True)

    def wp21a_call(self, fc, node):

        fc.execute_query(node, "CALL wp_21a()", True)
        fc.execute_query(node, "DROP procedure wp_21a", True)




    #####################################
    ############ Program 4 ####################
    ###################################

    async def wp21b_exe(self, fc, node):
        self.wp21b(fc, node)
        self.wp21b_call(fc, node)

    def wp21b(self, fc, node):
        wp21b = "CREATE OR REPLACE PROCEDURE wp_21b()" \
                " AS BEGIN\n" \
                " DECLARE a INT := 1900;\n" \
                " DECLARE b INT := 2000;\n" \
                " DECLARE x INT := 4900;\n" \
                " DECLARE y INT := 4977;\n" \
                " v2 = select d_year AS ss_sold_year, ss_item_sk,ss_customer_sk, sum(ss_quantity) ss_qty, sum(ss_wholesale_cost) ss_wc,sum(ss_sales_price) ss_sp from store_sales left join store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk join date_dim on ss_sold_date_sk = d_date_sk where sr_ticket_number is null group by d_year, ss_item_sk, ss_customer_sk;\n" \
                " v4 = select TOP 500 c.c_customer_sk, d_year, count(*) cnt from customer_address a, customer c, store_sales s, date_dim d, item i where a.ca_address_sk = c.c_current_addr_sk and c.c_customer_sk = s.ss_customer_sk and s.ss_sold_date_sk = d.d_date_sk and s.ss_item_sk = i.i_item_sk and i.i_current_price > 1.2 * (select avg(j.i_current_price) from item j where j.i_category = i.i_category) GROUP BY c.c_customer_sk, d_year;\n" \
                " v5 = select * from :v2 WHERE ss_sold_year between :a and :b;\n" \
                " v6 = select * from :v4 WHERE d_year between :a and :b;\n" \
                " v7 = select :v6.c_customer_sk, :v6.d_year from :v6, :v5 WHERE :v6.d_year  =:v5.ss_sold_year;\n" \
                " v8 = SELECT * FROM :v7 WHERE c_customer_sk = :x;\n" \
                " WHILE :x < :y DO\n" \
                "      v8 = select * from :v8 UNION ALL (SELECT * FROM :v7 WHERE c_customer_sk = :x);\n" \
                "      x =:x + 1;\n" \
                " END WHILE;\n" \
                " select count(*) from :v8;\n" \
                "END;"


        fc.execute_query(node, wp21b, True)


    def wp21b_call(self, fc, node):

        fc.execute_query(node, "CALL wp_21b()", True)
        fc.execute_query(node, "DROP procedure wp_21b", True)


    ##############################
    ########### Program 5 #########
    ##############################
    async def wpx1_exe(self, fc, node):
         self.wpx1(fc, node)
         self.wpx1_call(fc, node)

    def wpx1(self, fc, node):
        wpx1 = "CREATE OR REPLACE PROCEDURE wp_x1()" \
                " AS BEGIN\n" \
                " DECLARE a INT := 1900;\n" \
                " DECLARE b INT := 2000;\n" \
                " DECLARE x INT := 4500;\n" \
                " DECLARE y INT := 4977;\n" \
                " year_total = select c_customer_id customer_id,c_first_name customer_first_name,c_last_name customer_last_name,c_preferred_cust_flag customer_preferred_cust_flag,c_birth_country customer_birth_country,c_login customer_login,c_email_address customer_email_address,d_year dyear,sum(ss_ext_list_price-ss_ext_discount_amt) year_total,'s' sale_type from customer,store_sales,date_dim where c_customer_sk = ss_customer_sk and ss_sold_date_sk = d_date_sk group by c_customer_id, c_first_name,c_last_name,c_preferred_cust_flag ,c_birth_country,c_login,c_email_address,d_year union all select c_customer_id customer_id,c_first_name customer_first_name,c_last_name customer_last_name,c_preferred_cust_flag customer_preferred_cust_flag,c_birth_country customer_birth_country,c_login customer_login,c_email_address customer_email_address,d_year dyear,sum(ws_ext_list_price-ws_ext_discount_amt) year_total,'w' sale_type from customer,web_sales,date_dim where c_customer_sk = ws_bill_customer_sk and ws_sold_date_sk = d_date_sk group by c_customer_id,c_first_name,c_last_name,c_preferred_cust_flag ,c_birth_country,c_login,c_email_address,d_year;\n" \
                " v2 = select t_s_secyear.customer_id,t_s_secyear.customer_first_name,t_s_secyear.customer_last_name, t_s_firstyear.dyear from :year_total t_s_firstyear,:year_total t_s_secyear,:year_total t_w_firstyear,:year_total t_w_secyear where t_s_secyear.customer_id = t_s_firstyear.customer_id and t_s_firstyear.customer_id = t_w_secyear.customer_id and t_s_firstyear.customer_id = t_w_firstyear.customer_id and t_s_firstyear.sale_type = 's' and t_w_firstyear.sale_type = 'w' and t_s_secyear.sale_type = 's' and t_w_secyear.sale_type = 'w' and t_s_firstyear.dyear = 1998 and t_s_secyear.dyear = 1998+1 and t_w_firstyear.dyear = 1998 and t_w_secyear.dyear = 1998+1 and t_s_firstyear.year_total > 0 and t_w_firstyear.year_total > 0 and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else 0.0 end > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else 0.0 end order by t_s_secyear.customer_id,t_s_secyear.customer_first_name,t_s_secyear.customer_last_name, t_s_firstyear.dyear;\n" \
                " v4 = select c.c_customer_sk, d_year, count(*) cnt from customer_address a, customer c, store_sales s, date_dim d, item i where a.ca_address_sk = c.c_current_addr_sk and c.c_customer_sk = s.ss_customer_sk and s.ss_sold_date_sk = d.d_date_sk and s.ss_item_sk = i.i_item_sk and i.i_current_price > 1.2 * (select avg(j.i_current_price) from item j where j.i_category = i.i_category) GROUP BY c.c_customer_sk, d_year;\n" \
                " v5 = select * from :v2 WHERE dyear between :a and :b;\n" \
                " v6 = select * from :v4 WHERE d_year between :a and :b;\n" \
                " v7 = select :v6.c_customer_sk, :v6.d_year from :v6, :v5 WHERE :v6.d_year = :v5.dyear;\n" \
                " v8 = SELECT * FROM :v7 WHERE c_customer_sk = :x;\n" \
                " WHILE :x < :y DO\n" \
                "      v8 = select * from :v8 UNION ALL (SELECT * FROM :v7 WHERE c_customer_sk = :x);\n" \
                "      x =:x + 1;\n" \
                " END WHILE;\n" \
                " select count(*) from :v8;\n" \
                "END;"


        fc.execute_query(node, wpx1, True)

    def wpx1_call(self, fc, node):

        fc.execute_query(node, "CALL wp_x1()", True)
        fc.execute_query(node, "DROP procedure wp_x1", True)


    #################################
    ######## Program 6 #################
    #################################

    async def wpy1_exe(self, fc, node):
        self.wpy1(fc, node)
        self.wpy1_call(fc, node)

    def wpy1(self, fc, node):
        wpy1 = "CREATE OR REPLACE PROCEDURE wp_y1()" \
               " AS BEGIN\n" \
               " DECLARE a INT := 1;\n" \
               " DECLARE b INT := 2000;\n" \
               " DECLARE x INT := 3000;\n" \
               " DECLARE y INT := 4977;\n" \
               " ss = select ca_county,d_qoy, d_year,sum(ss_ext_sales_price) as store_sales from store_sales,date_dim,customer_address where ss_sold_date_sk = d_date_sk and ss_addr_sk=ca_address_sk group by ca_county,d_qoy, d_year;\n" \
               " ws = select ca_county,d_qoy, d_year,sum(ws_ext_sales_price) as web_sales from web_sales,date_dim,customer_address where ws_sold_date_sk = d_date_sk and ws_bill_addr_sk=ca_address_sk group by ca_county,d_qoy, d_year;\n" \
               " v2 = select ss1.ca_county,ss1.d_year,ws2.web_sales/ws1.web_sales web_q1_q2_increase,ss2.store_sales/ss1.store_sales store_q1_q2_increase,ws3.web_sales/ws2.web_sales web_q2_q3_increase,ss3.store_sales/ss2.store_sales store_q2_q3_increase from :ss ss1,:ss ss2,:ss ss3,:ws ws1,:ws ws2,:ws ws3 where ss1.d_qoy = 1 and ss1.d_year = 2002 and ss1.ca_county = ss2.ca_county and ss2.d_qoy = 2 and ss2.d_year = 2002 and ss2.ca_county = ss3.ca_county and ss3.d_qoy = 3 and ss3.d_year = 2002 and ss1.ca_county = ws1.ca_county and ws1.d_qoy = 1 and ws1.d_year = 2002 and ws1.ca_county = ws2.ca_county and ws2.d_qoy = 2 and ws2.d_year = 2002 and ws1.ca_county = ws3.ca_county and ws3.d_qoy = 3 and ws3.d_year =2002 and case when ws1.web_sales > 0 then ws2.web_sales/ws1.web_sales else null end > case when ss1.store_sales > 0 then ss2.store_sales/ss1.store_sales else null end and case when ws2.web_sales > 0 then ws3.web_sales/ws2.web_sales else null end > case when ss2.store_sales > 0 then ss3.store_sales/ss2.store_sales else null end order by ss1.ca_county,ss1.d_year, web_q1_q2_increase, store_q1_q2_increase, web_q2_q3_increase, store_q2_q3_increase;\n" \
               " v4 = select c.c_customer_sk, d_year, count(*) cnt from customer_address a, customer c, store_sales s, date_dim d, item i where a.ca_address_sk = c.c_current_addr_sk and c.c_customer_sk = s.ss_customer_sk and s.ss_sold_date_sk = d.d_date_sk and s.ss_item_sk = i.i_item_sk and i.i_current_price > 1.2 * (select avg(j.i_current_price) from item j where j.i_category = i.i_category) GROUP BY c.c_customer_sk, d_year;\n" \
               " v5 = select * from :v2 WHERE d_year between :a and :b;\n" \
               " v6 = select * from :v4 WHERE d_year between :a and :b;\n" \
               " v7 = select :v6.c_customer_sk, :v6.d_year from :v6, :v5 WHERE :v6.d_year =:v5.d_year;\n" \
               " v8 = SELECT * FROM :v7 WHERE c_customer_sk = :x;\n" \
               " WHILE :x < :y DO\n" \
               "      v8 = select * from :v8 UNION ALL (SELECT * FROM :v7 WHERE c_customer_sk = :x);\n" \
               "      x =:x + 1;\n" \
               " END WHILE;\n" \
               " select count(*) from :v8;\n" \
               "END;"


        fc.execute_query(node, wpy1, True)


    def wpy1_call(self, fc, node):

        fc.execute_query(node, "CALL wp_y1()", True)
        fc.execute_query(node, "DROP procedure wp_y1", True)

