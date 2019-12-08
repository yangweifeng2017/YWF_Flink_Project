package com.ywf.sources;

import com.ywf.entry.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @ClassName SourceFromMySQL
 * @Description 读取mysql数据源
 * @Author YangWeiFeng
 * @Date 2019/12/8 16:29
 * @Version 1.0
 **/
public class SourceFromMySQL extends RichSourceFunction<Student> {
    PreparedStatement ps;
    public static Connection connection;

    private static Connection getConnection() {
        if (null != connection){
            return connection;
        }
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "root123456");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return connection;
    }
    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String sql = "select * from Student;";
        ps = getConnection().prepareStatement(sql);
    }

    @Override
    public void close() {
        try{
            super.close();
            if (connection != null) { //关闭连接和释放资源
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        }catch (Exception e){
            e.printStackTrace();
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
        while (resultSet.next()) {
            Student student = new Student(
                    resultSet.getInt("id"),
                    resultSet.getString("name").trim(),
                    resultSet.getString("password").trim(),
                    resultSet.getInt("age"));
            sourceContext.collect(student);
        }
    }

    @Override
    public void cancel() {
        try {
            this.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

