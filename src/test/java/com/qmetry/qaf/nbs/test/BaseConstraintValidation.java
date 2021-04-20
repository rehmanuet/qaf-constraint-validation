package com.qmetry.qaf.nbs.test;

import java.sql.*;

public class BaseConstraintValidation {
    static Connection conn = null;


    public static Connection getConnection() {
        if (conn != null) return conn;
        // get db, user, pass from settings file
        return getConnection("ttest");
    }

    private static Connection getConnection(String db_name) {
        try {
            String url = "jdbc:postgresql://database-1-instance-1.cdmnlp9kk2o8.us-east-1.rds.amazonaws.com:5432/dev?user=postgres&password=HighRoads#123&ssl=false";
//            con=DriverManager.getConnection("jdbc:mysql://localhost/"+db_name+"?user="+user_name+"&password="+password);
            conn = DriverManager.getConnection(url);
            System.out.println(db_name);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return conn;
    }


    public static ResultSet runQuery(String query) throws SQLException {

        Statement statementObject = conn.createStatement();
        ResultSet results = statementObject.executeQuery(query);
        results.next();
        return results;
    }
}
