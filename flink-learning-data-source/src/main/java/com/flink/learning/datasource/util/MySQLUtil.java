package com.flink.learning.datasource.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;


/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-03-16
 */
public class MySQLUtil {

    private static final Logger logger = LoggerFactory.getLogger(MySQLUtil.class);

    public static Connection getConnection(String driver, String url, String user, String password) {
        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return connection;
    }
}
