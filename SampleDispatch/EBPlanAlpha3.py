################################
###### Flexy Dispatch #############
#####################################
################################
###### A sample demnostration of EB dispatch in base load test with 40 program invocations #############
#####################################
################################
###### Alpha=3.5, Elastic Node=4 #############
#####################################

from FederationConnector import FederationConnector

import time
import threading
import asyncio




class EBPlanAlpha3:
    def __init__(self):
        pass

    async def manual_execution(self):
        fc = FederationConnector()
        fc.create_federation_connection()

        start_time = time.time() * 1000

        ##EB register
        await self.wp11_eb_registration(fc,[1],[1],[1,3])
        await self.wp21_eb_registration(fc, [2], [1], [3])
        await self.wp21a_eb_registration(fc, [3], [3], [1])
        await self.wp21b_eb_registration(fc, [2], [2], [2])
        await self.wpx1_eb_registration(fc, [2], [2], [1, 3])
        await self.wpy1_eb_registration(fc, [2], [1], [2])

        # Program broadcast
        await asyncio.gather(self.wp11_call(fc, 1, [1],[1],[1,3]), self.wp11_call(fc, 2, [1],[1],[1,3]), self.wp11_call(fc, 3, [1],[1],[1,3]), self.wp11_call(fc, 4, [1],[1],[1,3]), self.wp11_call(fc, 5, [1],[1],[1,3]), self.wp11_call(fc, 6, [1],[1],[1,3]),self.wp11_call(fc, 7, [1],[1],[1,3]), self.wp11_call(fc, 8, [1],[1],[1,3]), self.wp11_call(fc, 9, [1],[1],[1,3]), self.wp11_call(fc, 10, [1],[1],[1,3]), self.wp11_call(fc, 11, [1],[1],[1,3]),
                             self.wp21_call(fc, 1, [2], [1], [3]), self.wp21_call(fc, 2, [2], [1], [3]), self.wp21_call(fc, 3, [2], [1], [3]), self.wp21_call(fc, 4, [2], [1], [3]),self.wp21_call(fc, 5, [2], [1], [3]),self.wp21_call(fc, 6, [2], [1], [3]), self.wp21_call(fc, 7, [2], [1], [3]), self.wp21_call(fc, 8, [2], [1], [3]), self.wp21_call(fc, 9, [2], [1], [3]),self.wp21_call(fc, 10, [2], [1], [3]),self.wp21_call(fc, 11, [2], [1], [3]), self.wp21_call(fc, 12, [2], [1], [3]), self.wp21_call(fc, 13, [2], [1], [3]), self.wp21_call(fc, 14, [2], [1], [3]),self.wp21_call(fc, 15, [2], [1], [3]),
                             self.wp21a_call(fc, 1, [3], [3], [1]), self.wp21a_call(fc, 2, [3], [3], [1]), self.wp21a_call(fc, 3, [3], [3], [1]), self.wp21a_call(fc, 4, [3], [3], [1]),
                             self.wp21b_call(fc, 1, [2], [2], [2]), self.wp21b_call(fc, 2, [2], [2], [2]),
                             self.wpx1_call(fc, 1, [2], [2], [1, 3]),self.wpx1_call(fc, 2, [2], [2], [1, 3]),self.wpx1_call(fc, 3, [2], [2], [1, 3]),
                             self.wpy1_call(fc, 1, [2], [1], [2]), self.wpy1_call(fc, 2, [2], [1], [2]),self.wpy1_call(fc, 3, [2], [1], [2]),self.wpy1_call(fc, 4, [2], [1], [2]), self.wpy1_call(fc, 5, [2], [1], [2]),self.wpy1_call(fc, 6, [2], [1], [2]))



        end_time = time.time() * 1000
        compile_time = end_time - start_time
        print("Execution time: {0}".format(round(compile_time, 3)))
        fc.disconnect_federation()




    #####################################
    ############ Program 1 ####################
    ###################################


    async def wp11_eb1(self, fc, node):


        wp11_eb1 = "CREATE OR REPLACE PROCEDURE wp_11_eb1(OUT v2 TABLE(...))" \
                   " AS BEGIN\n" \
                   " v2 = select dt.d_year,i.i_brand_id brand_id, i.i_brand brand,sum(ss_ext_sales_price) sum_agg from date_dim dt, store_sales vs, item i where dt.d_date_sk = vs.ss_sold_date_sk and vs.ss_item_sk = i.i_item_sk group by dt.d_year,i.i_brand,i.i_brand_id order by dt.d_year,sum_agg desc,brand_id;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"
        fc.execute_query(node, wp11_eb1, True)

    async def wp11_eb1_call(self, fc, node):
        wp11_eb1_call = "CREATE OR REPLACE PROCEDURE wp_11_eb1_call(IN inv INTEGER)" \
                   " AS BEGIN\n" \
                   " Declare v2 table(d_year integer,brand_id integer, brand varchar(50), sum_agg decimal(18,2));\n" \
                   " CALL wp_11_eb1(v2);\n" \
                   " IF :inv = 1 THEN\n" \
                   "    CREATE TABLE v2wp111 AS (SELECT * FROM :v2);\n" \
                   " ELSEIF :inv = 2 THEN\n" \
                   "    CREATE TABLE v2wp112 AS (SELECT * FROM :v2);\n" \
                   " ELSEIF :inv = 3 THEN\n" \
                   "    CREATE TABLE v2wp113 AS (SELECT * FROM :v2);\n" \
                   " ELSEIF :inv = 4 THEN\n" \
                   "    CREATE TABLE v2wp114 AS (SELECT * FROM :v2);\n" \
                   " ELSEIF :inv = 5 THEN\n" \
                   "    CREATE TABLE v2wp115 AS (SELECT * FROM :v2);\n" \
                   " ELSEIF :inv = 6 THEN\n" \
                   "    CREATE TABLE v2wp116 AS (SELECT * FROM :v2);\n" \
                   " ELSEIF :inv = 7 THEN\n" \
                   "    CREATE TABLE v2wp117 AS (SELECT * FROM :v2);\n" \
                   " ELSEIF :inv = 8 THEN\n" \
                   "    CREATE TABLE v2wp118 AS (SELECT * FROM :v2);\n" \
                   " ELSEIF :inv = 9 THEN\n" \
                   "    CREATE TABLE v2wp119 AS (SELECT * FROM :v2);\n" \
                   " ELSEIF :inv = 10 THEN\n" \
                   "    CREATE TABLE v2wp1110 AS (SELECT * FROM :v2);\n" \
                   " ELSEIF :inv = 11 THEN\n" \
                   "    CREATE TABLE v2wp1111 AS (SELECT * FROM :v2);\n" \
                   " ELSE\n" \
                   "    CREATE TABLE v2wp1112 AS (SELECT * FROM :v2);\n" \
                   " END IF;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"

        fc.execute_query(node, wp11_eb1_call, True)


    async def wp11_eb2(self, fc, node):


        wp11_eb2 = "CREATE OR REPLACE PROCEDURE wp_11_eb2(OUT v4 TABLE(...))" \
                   " AS BEGIN\n" \
                   " v4 = select c.c_customer_sk, d_year, count(*) cnt from customer_address a, customer c, store_sales s, date_dim d, item i where a.ca_address_sk = c.c_current_addr_sk and c.c_customer_sk = s.ss_customer_sk and s.ss_sold_date_sk = d.d_date_sk and s.ss_item_sk = i.i_item_sk and i.i_current_price > 1.2 * (select avg(j.i_current_price) from item j where j.i_category = i.i_category) GROUP BY c.c_customer_sk, d_year;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"

        fc.execute_query(node, wp11_eb2, True)

    async def wp11_eb2_call(self, fc, node):
        wp11_eb2_call = "CREATE OR REPLACE PROCEDURE wp_11_eb2_call(IN inv INTEGER)" \
                     " AS BEGIN\n" \
                     " Declare v4 table(c_customer_sk integer, d_year integer, cnt BIGINT);\n" \
                     " CALL wp_11_eb2(v4);\n" \
                     " IF :inv = 1 THEN\n" \
                     "    CREATE TABLE v4wp111 AS (SELECT * FROM :v4);\n" \
                     " ELSEIF :inv = 2 THEN\n" \
                     "    CREATE TABLE v4wp112 AS (SELECT * FROM :v4);\n" \
                     " ELSEIF :inv = 3 THEN\n" \
                     "    CREATE TABLE v4wp113 AS (SELECT * FROM :v4);\n" \
                     " ELSEIF :inv = 4 THEN\n" \
                     "    CREATE TABLE v4wp114 AS (SELECT * FROM :v4);\n" \
                     " ELSEIF :inv = 5 THEN\n" \
                     "    CREATE TABLE v4wp115 AS (SELECT * FROM :v4);\n" \
                     " ELSEIF :inv = 6 THEN\n" \
                     "    CREATE TABLE v4wp116 AS (SELECT * FROM :v4);\n" \
                     " ELSEIF :inv = 7 THEN\n" \
                     "    CREATE TABLE v4wp117 AS (SELECT * FROM :v4);\n" \
                     " ELSEIF :inv = 8 THEN\n" \
                     "    CREATE TABLE v4wp118 AS (SELECT * FROM :v4);\n" \
                     " ELSEIF :inv = 9 THEN\n" \
                     "    CREATE TABLE v4wp119 AS (SELECT * FROM :v4);\n" \
                     " ELSEIF :inv = 10 THEN\n" \
                     "    CREATE TABLE v4wp1110 AS (SELECT * FROM :v4);\n" \
                     " ELSEIF :inv = 11 THEN\n" \
                     "    CREATE TABLE v4wp1111 AS (SELECT * FROM :v4);\n" \
                     " ELSEIF :inv = 12 THEN\n" \
                     "    CREATE TABLE v4wp1112 AS (SELECT * FROM :v4);\n" \
                     " ELSEIF :inv = 13 THEN\n" \
                     "    CREATE TABLE v4wp1113 AS (SELECT * FROM :v4);\n" \
                     " ELSEIF :inv = 14 THEN\n" \
                     "    CREATE TABLE v4wp1114 AS (SELECT * FROM :v4);\n" \
                     " ELSEIF :inv = 15 THEN\n" \
                     "    CREATE TABLE v4wp1115 AS (SELECT * FROM :v4);\n" \
                     " ELSE\n" \
                     "    CREATE TABLE v4wp1116 AS (SELECT * FROM :v4);\n" \
                     " END IF;\n" \
                     " SELECT 1 from dummy;\n" \
                     "END;"


        fc.execute_query(node, wp11_eb2_call, True)


    async def wp11_eb3(self, fc, node):


        wp11_eb3 = "CREATE OR REPLACE PROCEDURE wp_11_eb3(IN v2 TABLE(...), IN v4 TABLE(...))" \
                    " AS BEGIN\n" \
                    " DECLARE a INT := 1700;\n" \
                    " DECLARE b INT := 2000;\n" \
                    " DECLARE x INT := 4900;\n" \
                    " DECLARE y INT := 4977;\n" \
                    " v3 = select * from :v2 where d_year between :a and :a + 1;\n" \
                    " WHILE :a < :b DO\n" \
                   "  v3 = select * from :v3 UNION ALL (select * from :v2 where d_year between :a and :a+1);\n" \
                   " a =:a + 1;\n" \
                   " END WHILE;\n" \
                   " v5 = SELECT * FROM :v4 WHERE c_customer_sk = :x;\n" \
                   " WHILE :x < :y DO\n" \
                   "  v5 = select * from :v5 UNION ALL (SELECT * FROM :v4 WHERE c_customer_sk = :x) ;\n" \
                   " x =:x + 1;\n" \
                   " END WHILE;\n" \
                   " select count(*) from :v5, :v3 WHERE :v5.d_year=:v3.d_year ;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"

        fc.execute_query(node, wp11_eb3, True)


    async def wp11_eb3_call(self, fc, node):
        wp11_eb3_call = "CREATE OR REPLACE PROCEDURE wp_11_eb3_call(IN v2 TABLE(...), IN v4 TABLE(...))" \
                        "AS BEGIN\n" \
                        "  CALL wp_11_eb3(:v2, :v4);\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"
        fc.execute_query(node, wp11_eb3_call, True)


    async def wp11_eb_registration(self, fc, eb1node, eb2node, eb3node):
        ###112
        eb1node1 = eb1node[0]
        eb2node1 = eb2node[0]
        eb3node1 = eb3node[0]
        eb3node2 = eb3node[1]

        # EB registration broadcast
        await asyncio.gather(self.wp11_eb1(fc, eb1node1), self.wp11_eb2(fc, eb2node1), self.wp11_eb3(fc, eb3node1), self.wp11_eb3(fc, eb3node2))

        # Parallel EBs broadcast based on dependency tree
        await asyncio.gather(self.wp11_eb1_call(fc, eb1node1), self.wp11_eb2_call(fc, eb2node1), self.wp11_eb3_call(fc, eb3node1),self.wp11_eb3_call(fc, eb3node2))


    async def wp11_eb1_exe(self, fc,inv, eb1node, eb2node, eb3node):
        eb1node1 = eb1node[0]
        tab = "wp11" + str(inv)
        fc.execute_query(eb1node1, "DROP TABLE v2" + tab + ";", True)
        fc.execute_query(eb1node1, "CALL wp_11_eb1_call(" + str(inv) + ");", True)

    async def wp11_eb2_exe(self, fc,inv, eb1node, eb2node, eb3node):
        eb2node1 = eb2node[0]
        tab = "wp11" + str(inv)
        fc.execute_query(eb2node1, "DROP TABLE v4" + tab + ";", True)
        fc.execute_query(eb2node1, "CALL wp_11_eb2_call(" + str(inv) + ");", True)

    async def wp11_eb3_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb1node1 = eb1node[0]
        eb2node1 = eb2node[0]
        eb3node1 = eb3node[0]
        eb3node2 = eb3node[1]
        tab = "wp11" + str(inv)

        if inv == 1 or inv == 2 or inv == 3 or inv == 4 or inv == 5 or inv == 6 or inv == 7 or inv == 8 or inv == 9 or inv == 10 or inv == 11:

            if eb3node1 != eb1node1:
                fc.execute_query(eb3node1, "CREATE VIRTUAL TABLE v2vt" + tab + " AT DKE" + str(eb1node1) + "_HANA.SystemDB.DKE" + str(eb1node1) + ".v2" + tab + ";", True)

                fc.execute_query(eb3node1, "CREATE TABLE v2" + tab + " AS (SELECT * FROM v2vt" + tab + ");", True)

            if eb3node1 != eb2node1:
                fc.execute_query(eb3node1, "CREATE VIRTUAL TABLE v4vt" + tab + " AT DKE" + str(eb2node1) + "_HANA.SystemDB.DKE" + str(eb2node1) + ".v4" + tab + ";", True)

                fc.execute_query(eb3node1, "CREATE TABLE v4" + tab + " AS (SELECT * FROM v4vt" + tab + ");", True)

            fc.execute_query(eb3node1, "CALL wp_11_eb3_call(v2" + tab + ",v4" + tab + ")", True)

            if eb3node1 != eb1node1:
                fc.execute_query(eb3node1, "DROP TABLE v2" + tab + ";", True)
                # fc.execute_query(eb3node1, "DROP TABLE v2vt" + tab + ";", True)

            if eb3node1 != eb2node1:
                fc.execute_query(eb3node1, "DROP TABLE v4" + tab + ";", True)
                # fc.execute_query(eb3node1, "DROP TABLE v4vt" + tab + ";", True)


        else:
            print("not possible")


    async def wp11_call(self, fc, inv, eb1node, eb2node, eb3node):
        # Parallel EBs broadcast based on dependency tree
        await asyncio.gather(self.wp11_eb1_exe(fc,inv, eb1node, eb2node, eb3node), self.wp11_eb2_exe(fc,inv, eb1node, eb2node, eb3node))
        await self.wp11_eb3_exe(fc,inv, eb1node, eb2node, eb3node)





    ######################################
    ######## Program 2 #######################
    #######################################

    async def wp21_eb1(self, fc, node):

        wp21_eb1 = "CREATE OR REPLACE PROCEDURE wp_21_eb1(OUT v2 TABLE(...))" \
                   " AS BEGIN\n" \
                   " v2 = select d_year AS ws_sold_year, ws_item_sk,ws_bill_customer_sk ws_customer_sk,sum(ws_quantity) ws_qty,sum(ws_wholesale_cost) ws_wc,sum(ws_sales_price) ws_sp from web_sales left join web_returns on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk join date_dim on ws_sold_date_sk = d_date_sk where wr_order_number is null group by d_year, ws_item_sk,ws_bill_customer_sk;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"
        fc.execute_query(node, wp21_eb1, True)
        # fc.execute_query(1, "CALL wp_21_eb1(?)", True)

    async def wp21_eb1_call(self, fc, node):
        wp21_eb1_call_ = "CREATE OR REPLACE PROCEDURE wp_21_eb1_call(IN inv INTEGER)" \
                        " AS BEGIN\n" \
                        " Declare v2 table(ws_sold_year integer, ws_item_sk integer, ws_customer_sk integer, ws_qty BIGINT, ws_wc BIGINT, ws_sp BIGINT);\n" \
                        " CALL wp_21_eb1(v2);\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"

        wp21_eb1_call = "CREATE OR REPLACE PROCEDURE wp_21_eb1_call(IN inv INTEGER)" \
                        " AS BEGIN\n" \
                        " Declare v2 table(ws_sold_year integer, ws_item_sk integer, ws_customer_sk integer, ws_qty BIGINT, ws_wc BIGINT, ws_sp BIGINT);\n" \
                        " CALL wp_21_eb1(v2);\n" \
                        " IF :inv = 1 THEN\n" \
                        "    CREATE TABLE v2wp211 AS (SELECT * FROM :v2);\n" \
                        " ELSEIF :inv = 2 THEN\n" \
                        "    CREATE TABLE v2wp212 AS (SELECT * FROM :v2);\n" \
                        " ELSEIF :inv = 3 THEN\n" \
                        "    CREATE TABLE v2wp213 AS (SELECT * FROM :v2);\n" \
                        " ELSEIF :inv = 4 THEN\n" \
                        "    CREATE TABLE v2wp214 AS (SELECT * FROM :v2);\n" \
                        " ELSEIF :inv = 5 THEN\n" \
                        "    CREATE TABLE v2wp215 AS (SELECT * FROM :v2);\n" \
                        " ELSEIF :inv = 6 THEN\n" \
                        "    CREATE TABLE v2wp216 AS (SELECT * FROM :v2);\n" \
                        " ELSEIF :inv = 7 THEN\n" \
                        "    CREATE TABLE v2wp217 AS (SELECT * FROM :v2);\n" \
                        " ELSEIF :inv = 8 THEN\n" \
                        "    CREATE TABLE v2wp218 AS (SELECT * FROM :v2);\n" \
                        " ELSEIF :inv = 9 THEN\n" \
                        "    CREATE TABLE v2wp219 AS (SELECT * FROM :v2);\n" \
                        " ELSEIF :inv = 10 THEN\n" \
                        "    CREATE TABLE v2wp2110 AS (SELECT * FROM :v2);\n" \
                        " ELSEIF :inv = 11 THEN\n" \
                        "    CREATE TABLE v2wp2111 AS (SELECT * FROM :v2);\n" \
                        " ELSEIF :inv = 12 THEN\n" \
                        "    CREATE TABLE v2wp2112 AS (SELECT * FROM :v2);\n" \
                        " ELSEIF :inv = 13 THEN\n" \
                        "    CREATE TABLE v2wp2113 AS (SELECT * FROM :v2);\n" \
                        " ELSEIF :inv = 14 THEN\n" \
                        "    CREATE TABLE v2wp2114 AS (SELECT * FROM :v2);\n" \
                        " ELSEIF :inv = 15 THEN\n" \
                        "    CREATE TABLE v2wp2115 AS (SELECT * FROM :v2);\n" \
                        " ELSE\n" \
                        "    CREATE TABLE v2wp2116 AS (SELECT * FROM :v2);\n" \
                        " END IF;\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"

        fc.execute_query(node, wp21_eb1_call, True)

    async def wp21_eb2(self, fc, node):

        wp21_eb2 = "CREATE OR REPLACE PROCEDURE wp_21_eb2(OUT v4 TABLE(...))" \
                   " AS BEGIN\n" \
                   " v4 = select c.c_customer_sk, d_year, count(*) cnt from customer_address a, customer c, store_sales s, date_dim d, item i where a.ca_address_sk = c.c_current_addr_sk and c.c_customer_sk = s.ss_customer_sk and s.ss_sold_date_sk = d.d_date_sk and s.ss_item_sk = i.i_item_sk and i.i_current_price > 1.2 * (select avg(j.i_current_price) from item j where j.i_category = i.i_category) GROUP BY c.c_customer_sk, d_year;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"

        fc.execute_query(node, wp21_eb2, True)

    async def wp21_eb2_call(self, fc, node):
        wp21_eb2_call_ = "CREATE OR REPLACE PROCEDURE wp_21_eb2_call(IN inv INTEGER)" \
                        " AS BEGIN\n" \
                        " Declare v4 table(c_customer_sk integer, d_year integer, cnt BIGINT);\n" \
                        " CALL wp_21_eb2(v4);\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"

        wp21_eb2_call = "CREATE OR REPLACE PROCEDURE wp_21_eb2_call(IN inv INTEGER)" \
                        " AS BEGIN\n" \
                        " Declare v4 table(c_customer_sk integer, d_year integer, cnt BIGINT);\n" \
                        " CALL wp_21_eb2(v4);\n" \
                        " IF :inv = 1 THEN\n" \
                        "    CREATE TABLE v4wp211 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 2 THEN\n" \
                        "    CREATE TABLE v4wp212 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 3 THEN\n" \
                        "    CREATE TABLE v4wp213 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 4 THEN\n" \
                        "    CREATE TABLE v4wp214 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 5 THEN\n" \
                        "    CREATE TABLE v4wp215 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 6 THEN\n" \
                        "    CREATE TABLE v4wp216 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 7 THEN\n" \
                        "    CREATE TABLE v4wp217 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 8 THEN\n" \
                        "    CREATE TABLE v4wp218 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 9 THEN\n" \
                        "    CREATE TABLE v4wp219 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 10 THEN\n" \
                        "    CREATE TABLE v4wp2110 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 11 THEN\n" \
                        "    CREATE TABLE v4wp2111 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 12 THEN\n" \
                        "    CREATE TABLE v4wp2112 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 13 THEN\n" \
                        "    CREATE TABLE v4wp2113 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 14 THEN\n" \
                        "    CREATE TABLE v4wp2114 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 15 THEN\n" \
                        "    CREATE TABLE v4wp2115 AS (SELECT * FROM :v4);\n" \
                        " ELSE\n" \
                        "    CREATE TABLE v4wp2116 AS (SELECT * FROM :v4);\n" \
                        " END IF;\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"

        fc.execute_query(node, wp21_eb2_call, True)

    async def wp21_eb3(self, fc, node):

        wp21_eb3 = "CREATE OR REPLACE PROCEDURE wp_21_eb3(IN v2 TABLE(...), IN v4 TABLE(...))" \
                   " AS BEGIN\n" \
                   " DECLARE a INT := 1700;\n" \
                   " DECLARE b INT := 2000;\n" \
                   " DECLARE x INT := 4900;\n" \
                   " DECLARE y INT := 4977;\n" \
                   " v3 = select * from :v2 where ws_sold_year between :a and :a + 5;\n" \
                   " WHILE :a < :b DO\n" \
                   "  v3 = select * from :v3 UNION ALL (select * from :v2 where ws_sold_year between :a and :a+5);\n" \
                   " a =:a + 5;\n" \
                   " END WHILE;\n" \
                   " v5 = SELECT * FROM :v4 WHERE c_customer_sk = :x;\n" \
                   " WHILE :x < :y DO\n" \
                   "   v5 = select * from :v5 UNION ALL (SELECT * FROM :v4 WHERE c_customer_sk = :x);\n" \
                   " x =:x + 1;\n" \
                   " END WHILE;\n" \
                   " select count(*) from :v5, :v3 WHERE :v5.d_year=:v3.ws_sold_year;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"

        fc.execute_query(node, wp21_eb3, True)

    async def wp21_eb3_call(self, fc, node):
        wp21_eb3_call = "CREATE OR REPLACE PROCEDURE wp_21_eb3_call(IN v2 TABLE(...), IN v4 TABLE(...))" \
                        "AS BEGIN\n" \
                        "  CALL wp_21_eb3(:v2, :v4);\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"
        fc.execute_query(node, wp21_eb3_call, True)

    async def wp21_eb_registration(self, fc, eb1node, eb2node, eb3node):
        ###212
        eb1node1 = eb1node[0]
        eb2node1 = eb2node[0]
        eb3node1 = eb3node[0]

        # EB register broadcast
        await asyncio.gather(self.wp21_eb1(fc, eb1node1), self.wp21_eb2(fc, eb2node1),self.wp21_eb3(fc, eb3node1))

        await asyncio.gather(self.wp21_eb1_call(fc, eb1node1),self.wp21_eb2_call(fc, eb2node1),self.wp21_eb3_call(fc, eb3node1))


    async def wp21_eb1_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb1node1 = eb1node[0]
        tab = "wp21" + str(inv)
        fc.execute_query(eb1node1, "DROP TABLE v2" + tab + ";", True)
        fc.execute_query(eb1node1, "CALL wp_21_eb1_call(" + str(inv) + ");", True)

    async def wp21_eb2_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb2node1 = eb2node[0]
        tab = "wp21" + str(inv)
        fc.execute_query(eb2node1, "DROP TABLE v4" + tab + ";", True)
        fc.execute_query(eb2node1, "CALL wp_21_eb2_call(" + str(inv) + ");", True)

    async def wp21_eb3_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb1node1 = eb1node[0]
        eb2node1 = eb2node[0]
        eb3node1 = eb3node[0]
        tab = "wp21" + str(inv)


        if inv == 1 or inv == 2 or inv >= 3:

            if eb3node1 != eb1node1:
                fc.execute_query(eb3node1, "CREATE VIRTUAL TABLE v2vt" + tab + " AT DKE" + str(eb1node1) + "_HANA.SystemDB.DKE" + str(eb1node1) + ".v2" + tab + ";", True)

                fc.execute_query(eb3node1, "CREATE TABLE v2" + tab + " AS (SELECT * FROM v2vt" + tab + ");", True)

            if eb3node1 != eb2node1:
                fc.execute_query(eb3node1, "CREATE VIRTUAL TABLE v4vt" + tab + " AT DKE" + str( eb2node1) + "_HANA.SystemDB.DKE" + str(eb2node1) + ".v4" + tab + ";", True)

                fc.execute_query(eb3node1, "CREATE TABLE v4" + tab + " AS (SELECT * FROM v4vt" + tab + ");", True)

            fc.execute_query(eb3node1, "CALL wp_21_eb3_call(v2" + tab + ",v4" + tab + ")", True)

            if eb3node1 != eb1node1:
                fc.execute_query(eb3node1, "DROP TABLE v2" + tab + ";", True)
                # fc.execute_query(eb3node1, "DROP TABLE v2vt" + tab + ";", True)

            if eb3node1 != eb2node1:
                fc.execute_query(eb3node1, "DROP TABLE v4" + tab + ";", True)



        else:
            print("not possible")

    async def wp21_call(self, fc, inv, eb1node, eb2node, eb3node):

        # Parallel EBs broadcast based on dependency tree
        await asyncio.gather(self.wp21_eb1_exe(fc, inv, eb1node, eb2node, eb3node), self.wp21_eb2_exe(fc, inv, eb1node, eb2node, eb3node))
        await self.wp21_eb3_exe(fc, inv, eb1node, eb2node, eb3node)



    #####################
    ######### Program 3 ########
    ########################

    async def wp21a_eb1(self, fc, node):

        wp21a_eb1 = "CREATE OR REPLACE PROCEDURE wp_21a_eb1(OUT v2 TABLE(...))" \
                   " AS BEGIN\n" \
                   " v2 = select d_year AS cs_sold_year, cs_item_sk,cs_bill_customer_sk cs_customer_sk,sum(cs_quantity) cs_qty, sum(cs_wholesale_cost) cs_wc,sum(cs_sales_price) cs_sp from catalog_sales left join catalog_returns on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk join date_dim on cs_sold_date_sk = d_date_sk where cr_order_number is null group by d_year, cs_item_sk, cs_bill_customer_sk;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"
        fc.execute_query(node, wp21a_eb1, True)


    async def wp21a_eb1_call(self, fc, node):
        wp21a_eb1_call_ = "CREATE OR REPLACE PROCEDURE wp_21a_eb1_call(IN inv INTEGER)" \
                        " AS BEGIN\n" \
                        " Declare v2 table(cs_sold_year integer, cs_item_sk integer, cs_customer_sk integer, cs_qty BIGINT, cs_wc BIGINT, cs_sp BIGINT);\n" \
                        " CALL wp_21a_eb1(v2);\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"

        wp21a_eb1_call = "CREATE OR REPLACE PROCEDURE wp_21a_eb1_call(IN inv INTEGER)" \
                         " AS BEGIN\n" \
                         " Declare v2 table(cs_sold_year integer, cs_item_sk integer, cs_customer_sk integer, cs_qty BIGINT, cs_wc BIGINT, cs_sp BIGINT);\n" \
                         " CALL wp_21a_eb1(v2);\n" \
                         " IF :inv = 1 THEN\n" \
                         "    CREATE TABLE v2wp21a1 AS (SELECT * FROM :v2);\n" \
                         " ELSEIF :inv = 2 THEN\n" \
                         "    CREATE TABLE v2wp21a2 AS (SELECT * FROM :v2);\n" \
                         " ELSEIF :inv = 3 THEN\n" \
                         "    CREATE TABLE v2wp21a3 AS (SELECT * FROM :v2);\n" \
                         " ELSEIF :inv = 4 THEN\n" \
                         "    CREATE TABLE v2wp21a4 AS (SELECT * FROM :v2);\n" \
                         " ELSE\n" \
                         "    CREATE TABLE v2wp21a5 AS (SELECT * FROM :v2);\n" \
                         " END IF;\n" \
                         " SELECT 1 from dummy;\n" \
                         "END;"

        fc.execute_query(node, wp21a_eb1_call, True)

    async def wp21a_eb2(self, fc, node):

        wp21a_eb2 = "CREATE OR REPLACE PROCEDURE wp_21a_eb2(OUT v4 TABLE(...))" \
                   " AS BEGIN\n" \
                   " v4 = select c.c_customer_sk, d_year, count(*) cnt from customer_address a, customer c, store_sales s, date_dim d, item i where a.ca_address_sk = c.c_current_addr_sk and c.c_customer_sk = s.ss_customer_sk and s.ss_sold_date_sk = d.d_date_sk and s.ss_item_sk = i.i_item_sk and i.i_current_price > 1.2 * (select avg(j.i_current_price) from item j where j.i_category = i.i_category) GROUP BY c.c_customer_sk, d_year;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"

        fc.execute_query(node, wp21a_eb2, True)

    async def wp21a_eb2_call(self, fc, node):
        wp21a_eb2_call_ = "CREATE OR REPLACE PROCEDURE wp_21a_eb2_call(IN inv INTEGER)" \
                         " AS BEGIN\n" \
                         " Declare v4 table(c_customer_sk integer, d_year integer, cnt BIGINT);\n" \
                         " CALL wp_21a_eb2(v4);\n" \
                         " SELECT 1 from dummy;\n" \
                         "END;"

        wp21a_eb2_call = "CREATE OR REPLACE PROCEDURE wp_21a_eb2_call(IN inv INTEGER)" \
                        " AS BEGIN\n" \
                        " Declare v4 table(c_customer_sk integer, d_year integer, cnt BIGINT);\n" \
                        " CALL wp_21a_eb2(v4);\n" \
                        " IF :inv = 1 THEN\n" \
                        "    CREATE TABLE v4wp21a1 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 2 THEN\n" \
                        "    CREATE TABLE v4wp21a2 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 3 THEN\n" \
                        "    CREATE TABLE v4wp21a3 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 4 THEN\n" \
                        "    CREATE TABLE v4wp21a4 AS (SELECT * FROM :v4);\n" \
                        " ELSE\n" \
                        "    CREATE TABLE v4wp21a5 AS (SELECT * FROM :v4);\n" \
                        " END IF;\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"

        fc.execute_query(node, wp21a_eb2_call, True)

    async def wp21a_eb3(self, fc, node):

        wp21a_eb3 = "CREATE OR REPLACE PROCEDURE wp_21a_eb3(IN v2 TABLE(...), IN v4 TABLE(...))" \
                   " AS BEGIN\n" \
                   " DECLARE a INT := 10;\n" \
                   " DECLARE b INT := 2000;\n" \
                   " DECLARE x INT := 4950;\n" \
                   " DECLARE y INT := 4977;\n" \
                   " v3 = select * from :v2 where cs_sold_year between :a and :a + 1;\n" \
                   " WHILE :a < :b DO\n" \
                   "  v3 = select * from :v3 UNION ALL (select * from :v2 where cs_sold_year between :a and :a+1);\n" \
                   " a =:a + 1;\n" \
                   " END WHILE;\n" \
                   " v5 = SELECT * FROM :v4 WHERE c_customer_sk = :x;\n" \
                   " WHILE :x < :y DO\n" \
                   "    v5 = select * from :v5 UNION ALL (SELECT * FROM :v4 WHERE c_customer_sk = :x);\n" \
                   " x =:x + 1;\n" \
                   " END WHILE;\n" \
                   " select count(*) from :v5, :v3 WHERE :v5.d_year=:v3.cs_sold_year;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"

        fc.execute_query(node, wp21a_eb3, True)

    async def wp21a_eb3_call(self, fc, node):
        wp21a_eb3_call = "CREATE OR REPLACE PROCEDURE wp_21a_eb3_call(IN v2 TABLE(...), IN v4 TABLE(...))" \
                        "AS BEGIN\n" \
                        "  CALL wp_21a_eb3(:v2, :v4);\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"
        fc.execute_query(node, wp21a_eb3_call, True)

    async def wp21a_eb_registration(self, fc, eb1node, eb2node, eb3node):
        ###21a2
        eb1node1 = eb1node[0]
        eb2node1 = eb2node[0]
        eb3node1 = eb3node[0]

        #EB register broadcast
        await asyncio.gather(self.wp21a_eb1(fc, eb1node1), self.wp21a_eb2(fc, eb2node1), self.wp21a_eb3(fc, eb3node1))

        await asyncio.gather(self.wp21a_eb1_call(fc, eb1node1),self.wp21a_eb2_call(fc, eb2node1),self.wp21a_eb3_call(fc, eb3node1))

    async def wp21a_eb1_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb1node1 = eb1node[0]
        tab = "wp21a" + str(inv)
        fc.execute_query(eb1node1, "DROP TABLE v2" + tab + ";", True)
        fc.execute_query(eb1node1, "CALL wp_21a_eb1_call(" + str(inv) + ");", True)

    async def wp21a_eb2_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb2node1 = eb2node[0]
        tab = "wp21a" + str(inv)
        fc.execute_query(eb2node1, "DROP TABLE v4" + tab + ";", True)
        fc.execute_query(eb2node1, "CALL wp_21a_eb2_call(" + str(inv) + ");", True)

    async def wp21a_eb3_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb1node1 = eb1node[0]
        eb2node1 = eb2node[0]
        eb3node1 = eb3node[0]
        # eb3node2 = eb3node[1]
        tab = "wp21a" + str(inv)

        if inv == 1 or inv == 2 or inv >= 3:

            if eb3node1 != eb1node1:
                fc.execute_query(eb3node1, "CREATE VIRTUAL TABLE v2vt" + tab + " AT DKE" + str(
                    eb1node1) + "_HANA.SystemDB.DKE" + str(eb1node1) + ".v2" + tab + ";", True)

                fc.execute_query(eb3node1, "CREATE TABLE v2" + tab + " AS (SELECT * FROM v2vt" + tab + ");", True)

            if eb3node1 != eb2node1:
                fc.execute_query(eb3node1, "CREATE VIRTUAL TABLE v4vt" + tab + " AT DKE" + str(
                    eb2node1) + "_HANA.SystemDB.DKE" + str(eb2node1) + ".v4" + tab + ";", True)

                fc.execute_query(eb3node1, "CREATE TABLE v4" + tab + " AS (SELECT * FROM v4vt" + tab + ");", True)

            fc.execute_query(eb3node1, "CALL wp_21a_eb3_call(v2" + tab + ",v4" + tab + ")", True)

            if eb3node1 != eb1node1:
                fc.execute_query(eb3node1, "DROP TABLE v2" + tab + ";", True)
                # fc.execute_query(eb3node1, "DROP TABLE v2vt" + tab + ";", True)

            if eb3node1 != eb2node1:
                fc.execute_query(eb3node1, "DROP TABLE v4" + tab + ";", True)
                # fc.execute_query(eb3node1, "DROP TABLE v4vt" + tab + ";", True)


        else:
            print("not possible")

    async def wp21a_call(self, fc, inv, eb1node, eb2node, eb3node):

        # Parallel EBs broadcast based on dependency tree
        await asyncio.gather(self.wp21a_eb1_exe(fc, inv, eb1node, eb2node, eb3node), self.wp21a_eb2_exe(fc, inv, eb1node, eb2node, eb3node))
        await self.wp21a_eb3_exe(fc, inv, eb1node, eb2node, eb3node)



    #####################################
    ############ 4 ####################
    ###################################

    async def wp21b_eb1(self, fc, node):

        wp21b_eb1 = "CREATE OR REPLACE PROCEDURE wp_21b_eb1(OUT v2 TABLE(...))" \
                    " AS BEGIN\n" \
                    " v2 = select d_year AS ss_sold_year, ss_item_sk,ss_customer_sk, sum(ss_quantity) ss_qty, sum(ss_wholesale_cost) ss_wc,sum(ss_sales_price) ss_sp from store_sales left join store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk join date_dim on ss_sold_date_sk = d_date_sk where sr_ticket_number is null group by d_year, ss_item_sk, ss_customer_sk;\n" \
                    " SELECT 1 from dummy;\n" \
                    "END;"
        fc.execute_query(node, wp21b_eb1, True)

    async def wp21b_eb1_call(self, fc, node):
        wp21b_eb1_call_ = "CREATE OR REPLACE PROCEDURE wp_21b_eb1_call(IN inv INTEGER)" \
                         " AS BEGIN\n" \
                         " Declare v2 table(ss_sold_year integer, ss_item_sk integer, ss_customer_sk integer, ss_qty BIGINT, ss_wc BIGINT, ss_sp BIGINT);\n" \
                         " CALL wp_21b_eb1(v2);\n" \
                         " SELECT 1 from dummy;\n" \
                         "END;"

        wp21b_eb1_call = "CREATE OR REPLACE PROCEDURE wp_21b_eb1_call(IN inv INTEGER)" \
                         " AS BEGIN\n" \
                         " Declare v2 table(ss_sold_year integer, ss_item_sk integer, ss_customer_sk integer, ss_qty BIGINT, ss_wc BIGINT, ss_sp BIGINT);\n" \
                         " CALL wp_21b_eb1(v2);\n" \
                         " IF :inv = 1 THEN\n" \
                         "    CREATE TABLE v2wp21b1 AS (SELECT * FROM :v2);\n" \
                         " ELSEIF :inv = 2 THEN\n" \
                         "    CREATE TABLE v2wp21b2 AS (SELECT * FROM :v2);\n" \
                         " ELSE\n" \
                         "    CREATE TABLE v2wp21b3 AS (SELECT * FROM :v2);\n" \
                         " END IF;\n" \
                         " SELECT 1 from dummy;\n" \
                         "END;"

        fc.execute_query(node, wp21b_eb1_call, True)

    async def wp21b_eb2(self, fc, node):

        wp21b_eb2 = "CREATE OR REPLACE PROCEDURE wp_21b_eb2(OUT v4 TABLE(...))" \
                    " AS BEGIN\n" \
                    " v4 = select c.c_customer_sk, d_year, count(*) cnt from customer_address a, customer c, store_sales s, date_dim d, item i where a.ca_address_sk = c.c_current_addr_sk and c.c_customer_sk = s.ss_customer_sk and s.ss_sold_date_sk = d.d_date_sk and s.ss_item_sk = i.i_item_sk and i.i_current_price > 1.2 * (select avg(j.i_current_price) from item j where j.i_category = i.i_category) GROUP BY c.c_customer_sk, d_year;\n" \
                    " SELECT 1 from dummy;\n" \
                    "END;"

        fc.execute_query(node, wp21b_eb2, True)

    async def wp21b_eb2_call(self, fc, node):
        wp21b_eb2_call_ = "CREATE OR REPLACE PROCEDURE wp_21b_eb2_call(IN inv INTEGER)" \
                          " AS BEGIN\n" \
                          " Declare v4 table(c_customer_sk integer, d_year integer, cnt BIGINT);\n" \
                          " CALL wp_21b_eb2(v4);\n" \
                          " SELECT 1 from dummy;\n" \
                          "END;"

        wp21b_eb2_call = "CREATE OR REPLACE PROCEDURE wp_21b_eb2_call(IN inv INTEGER)" \
                         " AS BEGIN\n" \
                         " Declare v4 table(c_customer_sk integer, d_year integer, cnt BIGINT);\n" \
                         " CALL wp_21b_eb2(v4);\n" \
                         " IF :inv = 1 THEN\n" \
                         "    CREATE TABLE v4wp21b1 AS (SELECT * FROM :v4);\n" \
                         " ELSEIF :inv = 2 THEN\n" \
                         "    CREATE TABLE v4wp21b2 AS (SELECT * FROM :v4);\n" \
                         " ELSE\n" \
                         "    CREATE TABLE v4wp21b3 AS (SELECT * FROM :v4);\n" \
                         " END IF;\n" \
                         " SELECT 1 from dummy;\n" \
                         "END;"


        fc.execute_query(node, wp21b_eb2_call, True)

    async def wp21b_eb3(self, fc, node):

        wp21b_eb3 = "CREATE OR REPLACE PROCEDURE wp_21b_eb3(IN v2 TABLE(...), IN v4 TABLE(...))" \
                    " AS BEGIN\n" \
                    " DECLARE a INT := 1700;\n" \
                    " DECLARE b INT := 2000;\n" \
                    " DECLARE x INT := 4975;\n" \
                    " DECLARE y INT := 4977;\n" \
                    " v3 = select * from :v2 where ss_sold_year between :a and :a + 1;\n" \
                    " WHILE :a < :b DO\n" \
                    "  v3 = select * from :v3 UNION ALL (select * from :v2 where ss_sold_year between :a and :a+1);\n" \
                    " a =:a + 1;\n" \
                    " END WHILE;\n" \
                    " v5 = SELECT * FROM :v4 WHERE c_customer_sk = :x;\n" \
                    " WHILE :x < :y DO\n" \
                    "    v5 = select * from :v5 UNION ALL (SELECT * FROM :v4 WHERE c_customer_sk = :x);\n" \
                    " x =:x + 1;\n" \
                    " END WHILE;\n" \
                    " select count(*) from :v5, :v3 WHERE :v5.d_year=:v3.ss_sold_year;\n" \
                    " SELECT 1 from dummy;\n" \
                    "END;"

        fc.execute_query(node, wp21b_eb3, True)

    async def wp21b_eb3_call(self, fc, node):
        wp21b_eb3_call = "CREATE OR REPLACE PROCEDURE wp_21b_eb3_call(IN v2 TABLE(...), IN v4 TABLE(...))" \
                         "AS BEGIN\n" \
                         "  CALL wp_21b_eb3(:v2, :v4);\n" \
                         " SELECT 1 from dummy;\n" \
                         "END;"
        fc.execute_query(node, wp21b_eb3_call, True)

    async def wp21b_eb_registration(self, fc, eb1node, eb2node, eb3node):
        ###21b2
        eb1node1 = eb1node[0]
        eb2node1 = eb2node[0]
        eb3node1 = eb3node[0]
        await asyncio.gather(self.wp21b_eb1(fc, eb1node1),self.wp21b_eb2(fc, eb2node1),self.wp21b_eb3(fc, eb3node1))

        await asyncio.gather(self.wp21b_eb1_call(fc, eb1node1),self.wp21b_eb2_call(fc, eb2node1),self.wp21b_eb3_call(fc, eb3node1))
        #

    async def wp21b_eb1_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb1node1 = eb1node[0]
        tab = "wp21b" + str(inv)
        fc.execute_query(eb1node1, "DROP TABLE v2" + tab + ";", True)
        fc.execute_query(eb1node1, "CALL wp_21b_eb1_call(" + str(inv) + ");", True)

    async def wp21b_eb2_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb2node1 = eb2node[0]
        tab = "wp21b" + str(inv)
        fc.execute_query(eb2node1, "DROP TABLE v4" + tab + ";", True)
        fc.execute_query(eb2node1, "CALL wp_21b_eb2_call(" + str(inv) + ");", True)

    async def wp21b_eb3_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb1node1 = eb1node[0]
        eb2node1 = eb2node[0]
        eb3node1 = eb3node[0]

        tab = "wp21b" + str(inv)

        if inv == 1 or inv == 2 or inv >= 3:

            if eb3node1 != eb1node1:
                fc.execute_query(eb3node1, "CREATE VIRTUAL TABLE v2vt" + tab + " AT DKE" + str(eb1node1) + "_HANA.SystemDB.DKE" + str(eb1node1) + ".v2" + tab + ";", True)

                fc.execute_query(eb3node1, "CREATE TABLE v2" + tab + " AS (SELECT * FROM v2vt" + tab + ");", True)

            if eb3node1 != eb2node1:
                fc.execute_query(eb3node1, "CREATE VIRTUAL TABLE v4vt" + tab + " AT DKE" + str(eb2node1) + "_HANA.SystemDB.DKE" + str(eb2node1) + ".v4" + tab + ";", True)

                fc.execute_query(eb3node1, "CREATE TABLE v4" + tab + " AS (SELECT * FROM v4vt" + tab + ");", True)

            fc.execute_query(eb3node1, "CALL wp_21b_eb3_call(v2" + tab + ",v4" + tab + ")", True)

            if eb3node1 != eb1node1:
                fc.execute_query(eb3node1, "DROP TABLE v2" + tab + ";", True)
                # fc.execute_query(eb3node1, "DROP TABLE v2vt" + tab + ";", True)

            if eb3node1 != eb2node1:
                fc.execute_query(eb3node1, "DROP TABLE v4" + tab + ";", True)
                # fc.execute_query(eb3node1, "DROP TABLE v4vt" + tab + ";", True)


        else:
            print("not possible")

    async def wp21b_call(self, fc, inv, eb1node, eb2node, eb3node):
        # Parallel EBs broadcast based on dependency tree
        await asyncio.gather(self.wp21b_eb1_exe(fc, inv, eb1node, eb2node, eb3node), self.wp21b_eb2_exe(fc, inv, eb1node, eb2node, eb3node))
        await self.wp21b_eb3_exe(fc, inv, eb1node, eb2node, eb3node)



    ############################
    ######## Program 5  ###############
    ############################
    async def wpx1_eb1(self, fc, node):

        wpx1_eb1 = "CREATE OR REPLACE PROCEDURE wp_x1_eb1(OUT year_total TABLE(...))" \
                   " AS BEGIN\n" \
                   " year_total = select c_customer_id customer_id,c_first_name customer_first_name,c_last_name customer_last_name,c_preferred_cust_flag customer_preferred_cust_flag,c_birth_country customer_birth_country,c_login customer_login,c_email_address customer_email_address,d_year dyear,sum(ss_ext_list_price-ss_ext_discount_amt) year_total,'s' sale_type from customer,store_sales,date_dim where c_customer_sk = ss_customer_sk and ss_sold_date_sk = d_date_sk group by c_customer_id, c_first_name,c_last_name,c_preferred_cust_flag ,c_birth_country,c_login,c_email_address,d_year union all select c_customer_id customer_id,c_first_name customer_first_name,c_last_name customer_last_name,c_preferred_cust_flag customer_preferred_cust_flag,c_birth_country customer_birth_country,c_login customer_login,c_email_address customer_email_address,d_year dyear,sum(ws_ext_list_price-ws_ext_discount_amt) year_total,'w' sale_type from customer,web_sales,date_dim where c_customer_sk = ws_bill_customer_sk and ws_sold_date_sk = d_date_sk group by c_customer_id,c_first_name,c_last_name,c_preferred_cust_flag ,c_birth_country,c_login,c_email_address,d_year;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"
        fc.execute_query(node, wpx1_eb1, True)
        # fc.execute_query(1, "CALL wp_x1_eb1(?)", True)

    async def wpx1_eb1_call(self, fc, node):
        wpx1_eb1_call_ = "CREATE OR REPLACE PROCEDURE wp_x1_eb1_call(IN inv INTEGER)" \
                        " AS BEGIN\n" \
                        " Declare year_total table(customer_id varchar(16), customer_first_name varchar(20), customer_last_name varchar(30), customer_preferred_cust_flag varchar(1), customer_birth_country varchar(20), customer_login varchar(13), customer_email_address varchar(50), dyear integer, year_total BIGINT, sale_type varchar);\n" \
                        " CALL wp_x1_eb1(year_total);\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"

        wpx1_eb1_call = "CREATE OR REPLACE PROCEDURE wp_x1_eb1_call(IN inv INTEGER)" \
                        " AS BEGIN\n" \
                        " Declare year_total table(customer_id varchar(16), customer_first_name varchar(20), customer_last_name varchar(30), customer_preferred_cust_flag varchar(1), customer_birth_country varchar(20), customer_login varchar(13), customer_email_address varchar(50), dyear integer, year_total BIGINT, sale_type varchar);\n" \
                        " CALL wp_x1_eb1(year_total);\n" \
                        " IF :inv = 1 THEN\n" \
                        "    CREATE TABLE year_totalwpx11 AS (SELECT * FROM :year_total);\n" \
                        " ELSEIF :inv = 2 THEN\n" \
                        "    CREATE TABLE year_totalwpx12 AS (SELECT * FROM :year_total);\n" \
                        " ELSEIF :inv = 3 THEN\n" \
                        "    CREATE TABLE year_totalwpx13 AS (SELECT * FROM :year_total);\n" \
                        " ELSE\n" \
                        "    CREATE TABLE year_totalwpx14 AS (SELECT * FROM :year_total);\n" \
                        " END IF;\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"

        fc.execute_query(node, wpx1_eb1_call, True)

    async def wpx1_eb2(self, fc, node):

        wpx1_eb2 = "CREATE OR REPLACE PROCEDURE wp_x1_eb2(OUT v4 TABLE(...))" \
                   " AS BEGIN\n" \
                   " v4 = select c.c_customer_sk, d_year, count(*) cnt from customer_address a, customer c, store_sales s, date_dim d, item i where a.ca_address_sk = c.c_current_addr_sk and c.c_customer_sk = s.ss_customer_sk and s.ss_sold_date_sk = d.d_date_sk and s.ss_item_sk = i.i_item_sk and i.i_current_price > 1.2 * (select avg(j.i_current_price) from item j where j.i_category = i.i_category) GROUP BY c.c_customer_sk, d_year;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"

        fc.execute_query(node, wpx1_eb2, True)

    async def wpx1_eb2_call(self, fc, node):
        wpx1_eb2_call_ = "CREATE OR REPLACE PROCEDURE wp_x1_eb2_call(IN inv INTEGER)" \
                        " AS BEGIN\n" \
                        " Declare v4 table(c_customer_sk integer, d_year integer, cnt BIGINT);\n" \
                        " CALL wp_x1_eb2(v4);\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"

        wpx1_eb2_call = "CREATE OR REPLACE PROCEDURE wp_x1_eb2_call(IN inv INTEGER)" \
                        " AS BEGIN\n" \
                        " Declare v4 table(c_customer_sk integer, d_year integer, cnt BIGINT);\n" \
                        " CALL wp_x1_eb2(v4);\n" \
                        " IF :inv = 1 THEN\n" \
                        "    CREATE TABLE v4wpx11 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 2 THEN\n" \
                        "    CREATE TABLE v4wpx12 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 3 THEN\n" \
                        "    CREATE TABLE v4wpx13 AS (SELECT * FROM :v4);\n" \
                        " ELSE\n" \
                        "    CREATE TABLE v4wpx14 AS (SELECT * FROM :v4);\n" \
                        " END IF;\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"

        fc.execute_query(node, wpx1_eb2_call, True)

    async def wpx1_eb3(self, fc, node):

        wpx1_eb3 = "CREATE OR REPLACE PROCEDURE wp_x1_eb3(IN year_total TABLE(...), IN v4 TABLE(...))" \
                   " AS BEGIN\n" \
                    " DECLARE a INT := 100;\n" \
                    " DECLARE b INT := 2000;\n" \
                    " DECLARE x INT := 4500;\n" \
                    " DECLARE y INT := 4977;\n" \
                    " v2 = select t_s_secyear.customer_id,t_s_secyear.customer_first_name,t_s_secyear.customer_last_name, t_s_firstyear.dyear from :year_total t_s_firstyear,:year_total t_s_secyear,:year_total t_w_firstyear,:year_total t_w_secyear where t_s_secyear.customer_id = t_s_firstyear.customer_id and t_s_firstyear.customer_id = t_w_secyear.customer_id and t_s_firstyear.customer_id = t_w_firstyear.customer_id and t_s_firstyear.sale_type = 's' and t_w_firstyear.sale_type = 'w' and t_s_secyear.sale_type = 's' and t_w_secyear.sale_type = 'w' and t_s_firstyear.dyear = 1998 and t_s_secyear.dyear = 1998+1 and t_w_firstyear.dyear = 1998 and t_w_secyear.dyear = 1998+1 and t_s_firstyear.year_total > 0 and t_w_firstyear.year_total > 0 and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else 0.0 end > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else 0.0 end order by t_s_secyear.customer_id,t_s_secyear.customer_first_name,t_s_secyear.customer_last_name, t_s_firstyear.dyear;\n" \
                    " v3 = select * from :v2 where dyear between :a and :a + 1;\n" \
                    " WHILE :a < :b DO\n" \
                    "  v3 = select * from :v3 UNION ALL (select * from :v2 where dyear between :a and :a+1);\n" \
                    " a =:a + 1;\n" \
                    " END WHILE;\n" \
                    " v5 = SELECT * FROM :v4 WHERE c_customer_sk = :x;\n" \
                    " WHILE :x < :y DO\n" \
                    "    v5 = select * from :v5 UNION ALL (SELECT * FROM :v4 WHERE c_customer_sk = :x);\n" \
                    " x =:x + 1;\n" \
                    " END WHILE;\n" \
                    " select count(*) from :v5, :v3 WHERE :v5.d_year=:v3.dyear;\n" \
                    " SELECT 1 from dummy;\n" \
                    "END;"

        fc.execute_query(node, wpx1_eb3, True)

    async def wpx1_eb3_call(self, fc, node):
        wpx1_eb3_call = "CREATE OR REPLACE PROCEDURE wp_x1_eb3_call(IN year_total TABLE(...), IN v4 TABLE(...))" \
                        "AS BEGIN\n" \
                        "  CALL wp_x1_eb3(:year_total, :v4);\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"
        fc.execute_query(node, wpx1_eb3_call, True)

    async def wpx1_eb_registration(self, fc, eb1node, eb2node, eb3node):
        ###x12
        eb1node1 = eb1node[0]
        eb2node1 = eb2node[0]
        eb3node1 = eb3node[0]
        eb3node2 = eb3node[1]

        await asyncio.gather(self.wpx1_eb1(fc, eb1node1),self.wpx1_eb2(fc, eb2node1),self.wpx1_eb3(fc, eb3node1), self.wpx1_eb3(fc, eb3node2))

        await asyncio.gather(self.wpx1_eb1_call(fc, eb1node1),self.wpx1_eb2_call(fc, eb2node1),self.wpx1_eb3_call(fc, eb3node1),self.wpx1_eb3_call(fc, eb3node2))


    async def wpx1_eb1_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb1node1 = eb1node[0]
        tab = "wpx1" + str(inv)
        fc.execute_query(eb1node1, "DROP TABLE year_total" + tab + ";", True)
        fc.execute_query(eb1node1, "CALL wp_x1_eb1_call(" + str(inv) + ");", True)

    async def wpx1_eb2_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb2node1 = eb2node[0]
        tab = "wpx1" + str(inv)
        fc.execute_query(eb2node1, "DROP TABLE v4" + tab + ";", True)
        fc.execute_query(eb2node1, "CALL wp_x1_eb2_call(" + str(inv) + ");", True)

    async def wpx1_eb3_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb1node1 = eb1node[0]
        eb2node1 = eb2node[0]
        eb3node1 = eb3node[0]
        eb3node2 = eb3node[1]
        tab = "wpx1" + str(inv)

        if inv == 1 or inv >= 2:

            if eb3node1 != eb1node1:
                fc.execute_query(eb3node1, "CREATE VIRTUAL TABLE year_totalvt" + tab + " AT DKE" + str(
                    eb1node1) + "_HANA.SystemDB.DKE" + str(eb1node1) + ".year_total" + tab + ";", True)

                fc.execute_query(eb3node1, "CREATE TABLE year_total" + tab + " AS (SELECT * FROM year_totalvt" + tab + ");", True)

            if eb3node1 != eb2node1:
                fc.execute_query(eb3node1, "CREATE VIRTUAL TABLE v4vt" + tab + " AT DKE" + str(
                    eb2node1) + "_HANA.SystemDB.DKE" + str(eb2node1) + ".v4" + tab + ";", True)

            #     fc.execute_query(eb3node1, "CREATE TABLE v4" + tab + " AS (SELECT * FROM v4vt" + tab + ");", True)

            fc.execute_query(eb3node1, "CALL wp_x1_eb3_call(year_total" + tab + ",v4" + tab + ")", True)

            if eb3node1 != eb1node1:
                fc.execute_query(eb3node1, "DROP TABLE year_total" + tab + ";", True)
                # fc.execute_query(eb3node1, "DROP TABLE year_totalvt" + tab + ";", True)

            if eb3node1 != eb2node1:
                fc.execute_query(eb3node1, "DROP TABLE v4" + tab + ";", True)
                # fc.execute_query(eb3node1, "DROP TABLE v4vt" + tab + ";", True)
        else:
            print("not possible")

    async def wpx1_call(self, fc, inv, eb1node, eb2node, eb3node):

        # Parallel EBs broadcast based on dependency tree
        await asyncio.gather(self.wpx1_eb1_exe(fc, inv, eb1node, eb2node, eb3node), self.wpx1_eb1_exe(fc, inv, eb1node, eb2node, eb3node))
        await self.wpx1_eb3_exe(fc, inv, eb1node, eb2node, eb3node)


    #################################
    ######## Program 6 #################
    #################################

    async def wpy1_eb1(self, fc, node):

        wpy1_eb1 = "CREATE OR REPLACE PROCEDURE wp_y1_eb1(OUT ss TABLE(...))" \
                   " AS BEGIN\n" \
                   " ss = select ca_county,d_qoy, d_year,sum(ss_ext_sales_price) as store_sales from store_sales,date_dim,customer_address where ss_sold_date_sk = d_date_sk and ss_addr_sk=ca_address_sk group by ca_county,d_qoy, d_year;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"
        fc.execute_query(node, wpy1_eb1, True)

    async def wpy1_eb1_call(self, fc, node):
        wpy1_eb1_call_ = "CREATE OR REPLACE PROCEDURE wp_y1_eb1_call(IN inv INTEGER)" \
                        " AS BEGIN\n" \
                        " Declare ss table(ca_county varchar(30),d_qoy integer, d_year integer,store_sales BIGINT);\n" \
                        " CALL wp_y1_eb1(ss);\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"

        wpy1_eb1_call = "CREATE OR REPLACE PROCEDURE wp_y1_eb1_call(IN inv INTEGER)" \
                        " AS BEGIN\n" \
                        " Declare ss table(ca_county varchar(30),d_qoy integer, d_year integer,store_sales BIGINT);\n" \
                        " CALL wp_y1_eb1(ss);\n" \
                        " IF :inv = 1 THEN\n" \
                        "    CREATE TABLE sswpy11 AS (SELECT * FROM :ss);\n" \
                        " ELSEIF :inv = 2 THEN\n" \
                        "    CREATE TABLE sswpy12 AS (SELECT * FROM :ss);\n" \
                        " ELSEIF :inv = 3 THEN\n" \
                        "    CREATE TABLE sswpy13 AS (SELECT * FROM :ss);\n" \
                        " ELSEIF :inv = 4 THEN\n" \
                        "    CREATE TABLE sswpy14 AS (SELECT * FROM :ss);\n" \
                        " ELSEIF :inv = 5 THEN\n" \
                        "    CREATE TABLE sswpy15 AS (SELECT * FROM :ss);\n" \
                        " ELSEIF :inv = 6 THEN\n" \
                        "    CREATE TABLE sswpy16 AS (SELECT * FROM :ss);\n" \
                        " ELSE\n" \
                        "    CREATE TABLE sswpy17 AS (SELECT * FROM :ss);\n" \
                        " END IF;\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"

        fc.execute_query(node, wpy1_eb1_call, True)

    async def wpy1_eb2(self, fc, node):

        wpy1_eb2 = "CREATE OR REPLACE PROCEDURE wp_y1_eb2(OUT v4 TABLE(...))" \
                   " AS BEGIN\n" \
                   " v4 = select c.c_customer_sk, d_year, count(*) cnt from customer_address a, customer c, store_sales s, date_dim d, item i where a.ca_address_sk = c.c_current_addr_sk and c.c_customer_sk = s.ss_customer_sk and s.ss_sold_date_sk = d.d_date_sk and s.ss_item_sk = i.i_item_sk and i.i_current_price > 1.2 * (select avg(j.i_current_price) from item j where j.i_category = i.i_category) GROUP BY c.c_customer_sk, d_year;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"

        fc.execute_query(node, wpy1_eb2, True)

    async def wpy1_eb2_call(self, fc, node):
        wpy1_eb2_call_ = "CREATE OR REPLACE PROCEDURE wp_y1_eb2_call(IN inv INTEGER)" \
                        " AS BEGIN\n" \
                        " Declare v4 table(c_customer_sk integer, d_year integer, cnt BIGINT);\n" \
                        " CALL wp_y1_eb2(v4);\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"

        wpy1_eb2_call = "CREATE OR REPLACE PROCEDURE wp_y1_eb2_call(IN inv INTEGER)" \
                        " AS BEGIN\n" \
                        " Declare v4 table(c_customer_sk integer, d_year integer, cnt BIGINT);\n" \
                        " CALL wp_y1_eb2(v4);\n" \
                        " IF :inv = 1 THEN\n" \
                        "    CREATE TABLE v4wpy11 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 2 THEN\n" \
                        "    CREATE TABLE v4wpy12 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 3 THEN\n" \
                        "    CREATE TABLE v4wpy13 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 4 THEN\n" \
                        "    CREATE TABLE v4wpy14 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 5 THEN\n" \
                        "    CREATE TABLE v4wpy15 AS (SELECT * FROM :v4);\n" \
                        " ELSEIF :inv = 6 THEN\n" \
                        "    CREATE TABLE v4wpy16 AS (SELECT * FROM :v4);\n" \
                        " ELSE\n" \
                        "    CREATE TABLE v4wpy17 AS (SELECT * FROM :v4);\n" \
                        " END IF;\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"

        fc.execute_query(node, wpy1_eb2_call, True)

    async def wpy1_eb3(self, fc, node):

        wpy1_eb3 = "CREATE OR REPLACE PROCEDURE wp_y1_eb3(IN ss TABLE(...), IN v4 TABLE(...))" \
                   " AS BEGIN\n" \
                   " DECLARE a INT := 100;\n" \
                   " DECLARE b INT := 2000;\n" \
                   " DECLARE x INT := 4500;\n" \
                   " DECLARE y INT := 4977;\n" \
                   " ws = select ca_county,d_qoy, d_year,sum(ws_ext_sales_price) as web_sales from web_sales,date_dim,customer_address where ws_sold_date_sk = d_date_sk and ws_bill_addr_sk=ca_address_sk group by ca_county,d_qoy, d_year;\n" \
                   " v2 = select ss1.ca_county,ss1.d_year,ws2.web_sales/ws1.web_sales web_q1_q2_increase,ss2.store_sales/ss1.store_sales store_q1_q2_increase,ws3.web_sales/ws2.web_sales web_q2_q3_increase,ss3.store_sales/ss2.store_sales store_q2_q3_increase from :ss ss1,:ss ss2,:ss ss3,:ws ws1,:ws ws2,:ws ws3 where ss1.d_qoy = 1 and ss1.d_year = 2002 and ss1.ca_county = ss2.ca_county and ss2.d_qoy = 2 and ss2.d_year = 2002 and ss2.ca_county = ss3.ca_county and ss3.d_qoy = 3 and ss3.d_year = 2002 and ss1.ca_county = ws1.ca_county and ws1.d_qoy = 1 and ws1.d_year = 2002 and ws1.ca_county = ws2.ca_county and ws2.d_qoy = 2 and ws2.d_year = 2002 and ws1.ca_county = ws3.ca_county and ws3.d_qoy = 3 and ws3.d_year =2002 and case when ws1.web_sales > 0 then ws2.web_sales/ws1.web_sales else null end > case when ss1.store_sales > 0 then ss2.store_sales/ss1.store_sales else null end and case when ws2.web_sales > 0 then ws3.web_sales/ws2.web_sales else null end > case when ss2.store_sales > 0 then ss3.store_sales/ss2.store_sales else null end order by ss1.ca_county,ss1.d_year, web_q1_q2_increase, store_q1_q2_increase, web_q2_q3_increase, store_q2_q3_increase;\n" \
                   " v3 = select * from :v2 where d_year between :a and :a + 1;\n" \
                   " WHILE :a < :b DO\n" \
                   "  v3 = select * from :v3 UNION ALL (select * from :v2 where d_year between :a and :a+1);\n" \
                   " a =:a + 1;\n" \
                   " END WHILE;\n" \
                   " v5 = SELECT * FROM :v4 WHERE c_customer_sk = :x;\n" \
                   " WHILE :x < :y DO\n" \
                   "   v5 = select * from :v5 UNION ALL (SELECT * FROM :v4 WHERE c_customer_sk = :x);\n" \
                   " x =:x + 1;\n" \
                   " END WHILE;\n" \
                   " select count(*) from :v5, :v3 WHERE :v5.d_year=:v3.d_year;\n" \
                   " SELECT 1 from dummy;\n" \
                   "END;"

        fc.execute_query(node, wpy1_eb3, True)

    async def wpy1_eb3_call(self, fc, node):
        wpy1_eb3_call = "CREATE OR REPLACE PROCEDURE wp_y1_eb3_call(IN ss TABLE(...), IN v4 TABLE(...))" \
                        "AS BEGIN\n" \
                        "  CALL wp_y1_eb3(:ss, :v4);\n" \
                        " SELECT 1 from dummy;\n" \
                        "END;"
        fc.execute_query(node, wpy1_eb3_call, True)

    async def wpy1_eb_registration(self, fc, eb1node, eb2node, eb3node):
        ###y12
        eb1node1 = eb1node[0]
        eb2node1 = eb2node[0]
        eb3node1 = eb3node[0]

        # #
        await asyncio.gather(self.wpy1_eb1_call(fc, eb1node1), self.wpy1_eb2_call(fc, eb2node1), self.wpy1_eb3_call(fc, eb3node1))

    async def wpy1_eb1_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb1node1 = eb1node[0]
        tab = "wpy1" + str(inv)
        fc.execute_query(eb1node1, "DROP TABLE ss" + tab + ";", True)
        fc.execute_query(eb1node1, "CALL wp_y1_eb1_call(" + str(inv) + ");", True)

    async def wpy1_eb2_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb2node1 = eb2node[0]
        tab = "wpy1" + str(inv)
        fc.execute_query(eb2node1, "DROP TABLE v4" + tab + ";", True)
        fc.execute_query(eb2node1, "CALL wp_y1_eb2_call(" + str(inv) + ");", True)

    async def wpy1_eb3_exe(self, fc, inv, eb1node, eb2node, eb3node):
        eb1node1 = eb1node[0]
        eb2node1 = eb2node[0]
        eb3node1 = eb3node[0]
        # eb3node2 = eb3node[1]
        tab = "wpy1" + str(inv)

        if inv == 1 or inv == 2 or inv >= 3:

            if eb3node1 != eb1node1:
                fc.execute_query(eb3node1, "CREATE VIRTUAL TABLE ssvt" + tab + " AT DKE" + str(
                    eb1node1) + "_HANA.SystemDB.DKE" + str(eb1node1) + ".ss" + tab + ";", True)

                fc.execute_query(eb3node1, "CREATE TABLE ss" + tab + " AS (SELECT * FROM ssvt" + tab + ");", True)

            if eb3node1 != eb2node1:
                fc.execute_query(eb3node1, "CREATE VIRTUAL TABLE v4vt" + tab + " AT DKE" + str(
                    eb2node1) + "_HANA.SystemDB.DKE" + str(eb2node1) + ".v4" + tab + ";", True)

                fc.execute_query(eb3node1, "CREATE TABLE v4" + tab + " AS (SELECT * FROM v4vt" + tab + ");", True)

            fc.execute_query(eb3node1, "CALL wp_y1_eb3_call(ss" + tab + ",v4" + tab + ")", True)

            if eb3node1 != eb1node1:
                fc.execute_query(eb3node1, "DROP TABLE ss" + tab + ";", True)
                # fc.execute_query(eb3node1, "DROP TABLE ssvt" + tab + ";", True)

            if eb3node1 != eb2node1:
                fc.execute_query(eb3node1, "DROP TABLE v4" + tab + ";", True)
                # fc.execute_query(eb3node1, "DROP TABLE v4vt" + tab + ";", True)

        else:
            print("not possible")

    async def wpy1_call(self, fc, inv, eb1node, eb2node, eb3node):
        # Parallel EBs broadcast based on dependency tree

        await asyncio.gather(self.wpy1_eb1_exe(fc, inv, eb1node, eb2node, eb3node), self.wpy1_eb2_exe(fc, inv, eb1node, eb2node, eb3node))
        await self.wpy1_eb3_exe(fc, inv, eb1node, eb2node, eb3node)
