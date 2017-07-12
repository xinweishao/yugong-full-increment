/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 manticorecao@gmail.com
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */


package com.taobao.yugong.data;

import com.google.common.base.Throwables;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.*;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Fill MySQL Data
 * @author caobin
 * @version 1.0 2017.07.10
 */
public class DataGenerator {

    static {

        try {
            Class<?> driverClass = Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            Throwables.propagate(e);
        }
    }

    private static Connection getConnection(){
        try {
            return DriverManager.getConnection("jdbc:mysql://192.168.177.70:3306/test_mysql", "user_acct", "!@#Qaz");
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }


    private static void fillShopOrderDetail(int records) throws Exception{
        while (records -- > 0){
            Connection conn = getConnection();
            PreparedStatement pstmt = null;
            Random random = new Random();

            try {
                pstmt = conn.prepareStatement("insert into shop_order_detail(order_id, product_id, quantity, unit_cost, is_refunded, product_cate, discount_amount, timestamp) values (?,?,?,?,?,?,?,?)");

                pstmt.setInt(1, Math.abs(random.nextInt(100000)));
                pstmt.setInt(2, Math.abs(random.nextInt(300000)));
                pstmt.setInt(3, Math.abs(random.nextInt(1000)));
                pstmt.setBigDecimal(4, new BigDecimal(random.nextFloat(), new MathContext(10, RoundingMode.CEILING)).abs());
                pstmt.setBoolean(5, records % 2 == 0);
                pstmt.setInt(6, 4);
                pstmt.setBigDecimal(7, new BigDecimal(random.nextFloat(), new MathContext(10, RoundingMode.CEILING)).abs());
                pstmt.setDate(8, new Date(System.currentTimeMillis()));

                pstmt.executeUpdate();
            }  finally {
                if(pstmt != null)pstmt.close();
                if(conn != null)conn.close();
            }
        }
    }




    private static void fillShopOrderDetailMulti(int records) throws Exception{
        while (records -- > 0){
            Connection conn = getConnection();
            PreparedStatement pstmt = null;
            Random random = new Random();

            try {
                pstmt = conn.prepareStatement("insert into shop_order_detail_multi(order_id, multi_product_id, product_id, unit_cost) values (?,?,?,?)");

                pstmt.setInt(1, Math.abs(random.nextInt(1000000)));
                pstmt.setInt(2, Math.abs(random.nextInt(2000000)));
                pstmt.setInt(3, Math.abs(random.nextInt(1000)));
                pstmt.setBigDecimal(4, new BigDecimal(random.nextFloat(), new MathContext(10, RoundingMode.CEILING)).abs());

                pstmt.executeUpdate();
            }  finally {
                if(pstmt != null)pstmt.close();
                if(conn != null)conn.close();
            }
        }
    }

    public static void fillData(Tables tables, int recordsPerThread, int threads) throws Exception{
        CyclicBarrier cyclicBarrier = new CyclicBarrier(threads);
        ExecutorService executorService = Executors.newFixedThreadPool(threads);

        for(int i = 0; i < threads; i++){
            executorService.execute(new DataFiller(tables, cyclicBarrier, recordsPerThread));
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
    }


    static class DataFiller implements Runnable {

        private Tables tables;

        private CyclicBarrier cyclicBarrier;

        private int records;

        public DataFiller(Tables tables, CyclicBarrier cyclicBarrier, int records){
            this.tables = tables;
            this.cyclicBarrier = cyclicBarrier;
            this.records = records;
        }

        @Override
        public void run() {
            try {
                switch(tables){
                    case ShopOrderDetail:
                        fillShopOrderDetail(records);
                        break;
                    case ShopOrderDetailMulti:
                        fillShopOrderDetailMulti(records);
                        break;
                    default:
                }
                cyclicBarrier.await();
            } catch (Exception e){
                Throwables.propagate(e);
            }

        }
    }

    enum Tables {
        ShopOrderDetailMulti,

        ShopOrderDetail
    }

    public static void main(String[] args) throws Exception {
       fillData(Tables.ShopOrderDetail, 2, 4);
       fillData(Tables.ShopOrderDetailMulti, 2, 4);
    }

}
