package com.huliang.jdbc;

import java.sql.*;

/**
 * Jdbc连接Hive数据仓库
 * @author huliang
 * @date 2018/10/3 16:45
 */
public class JdbcApp {

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String url = "jdbc:hive2://192.168.44.201:10000/mydb";
        String sql = "select id ,name, age from users";
        Connection conn = DriverManager.getConnection(url);
        Statement st = conn.createStatement();
        ResultSet rs  = st.executeQuery(sql);

        while (rs.next()) {
            System.out.println(rs.getInt(1)+":"+rs.getString(2));
        }

        rs.close();
        st.close();
        conn.close();
    }
}
