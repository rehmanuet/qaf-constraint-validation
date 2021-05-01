package com.qmetry.qaf.nbs.utils;

import com.qmetry.qaf.automation.core.ConfigurationManager;

import java.sql.*;

public class BaseConstraintValidation {
    public static Connection CONN = null;


    public static Connection getConnection() {
        if (CONN != null) return CONN;
        String URL = ConfigurationManager.getBundle().getString("db.url");
        String PORT = ConfigurationManager.getBundle().getString("db.port");
        String SCHEMA = ConfigurationManager.getBundle().getString("db.schema");
        String USER = ConfigurationManager.getBundle().getString("db.user");
        String PASS = ConfigurationManager.getBundle().getString("db.pwd");
        return getConnection(URL, PORT, SCHEMA, USER, PASS);
    }

    private static Connection getConnection(String pgurl, String port, String schema, String user, String pass) {
        try {
            String url = pgurl + ":" + port + "/" + schema + "?" + "user=" + user + "&password=" + pass + "&ssl=false";
            CONN = DriverManager.getConnection(url);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return CONN;
    }


    public static ResultSet runQuery(String query) throws SQLException {

        Statement statementObject = CONN.createStatement();
        ResultSet results = statementObject.executeQuery(query);
        results.next();
        return results;
    }
}
