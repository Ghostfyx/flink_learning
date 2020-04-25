package com.flink.learning.datasink.sinks;

import com.flink.learning.datasink.model.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @description: 使用sink数据传输到MySQL
 * @author: fanyeuxiang
 * @createDate: 2020-03-23
 */
public class MySqlSink extends RichSinkFunction<Student> {

    PreparedStatement ps;

    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param configuration
     */
    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        connection = getConnection();
        String sql = "insert into Student(id, name, password, age) values(?, ?, ?, ?);";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(Student value, Context context) throws Exception {
        if (ps == null) {
            return;
        }
        //组装数据，执行插入操作
        ps.setInt(1, value.getId());
        ps.setString(2, value.getName());
        ps.setString(3, value.getPassword());
        ps.setInt(4, value.getAge());
        ps.executeUpdate();
    }

    private Connection getConnection(){
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/big_data_learn?useUnicode=true&characterEncoding=UTF-8",
                    "root", "123456");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }
}
