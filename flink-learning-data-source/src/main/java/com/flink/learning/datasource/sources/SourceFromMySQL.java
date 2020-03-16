package com.flink.learning.datasource.sources;

import com.flink.learning.datasource.model.Student;
import com.flink.learning.datasource.util.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @description: 自定义source，从mysql中读取数据
 * @author: fanyeuxiang
 * @createDate: 2020-03-16
 */
public class SourceFromMySQL extends RichSourceFunction<Student> {

    PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MySQLUtil.getConnection("com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8",
                "root",
                "root123456");
        String sql = "select * from Student;";
        ps = this.connection.prepareStatement(sql);
    }

    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            //关闭连接和释放资源
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * DataStream 调用一次 run() 方法用来获取数据
     *
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()){
            Student student = new Student(
                    resultSet.getLong("id"),
                    resultSet.getString("name").trim(),
                    resultSet.getString("password").trim(),
                    resultSet.getInt("age"));
            sourceContext.collect(student);
        }
    }

    @Override
    public void cancel() {

    }
}
