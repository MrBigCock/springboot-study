package com.coco.flinkmysql;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.springframework.beans.factory.annotation.Value;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySQLSink extends RichSinkFunction<Tuple3<Integer, String, Integer>> {

    private static final long serialVersionUID = 1L;
    private Connection connection;
    private PreparedStatement preparedStatement;
    static String password = "MVPzhao@24";

    @Value("mysql.user")
    static String username = "root";
    @Value("mysql.driver")
    static String drivername = "com.mysql.jdbc.Driver";
    @Value("mysql.url")
    static String dburl = "jdbc:mysql://localhost:3306/flinkmysql";

    @Override
    public void invoke(Tuple3<Integer, String, Integer> value) throws Exception {
        Class.forName(drivername);
        connection = DriverManager.getConnection(dburl, username, password);
        String sql = "replace into orders(order_id,order_no,order_price) values(?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setInt(1, value.f0);
        preparedStatement.setString(2, value.f1);
        preparedStatement.setInt(3, value.f2);
        preparedStatement.executeUpdate();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

}
