package com.qmetry.qaf.nbs.test;

import com.qmetry.qaf.automation.step.QAFTestStep;
import com.qmetry.qaf.automation.testng.dataprovider.QAFDataProvider;
import com.qmetry.qaf.automation.util.Reporter;
import com.sun.xml.internal.bind.annotation.OverrideAnnotationOf;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class ConstraintValidationTest extends BaseConstraintValidation {

    @BeforeSuite
    public void startUp() {
        CONN = getConnection();

    }


    @Test
    public void checkPrimaryKeyNamingConvention() throws SQLException {
        String tbl_name = "base_plan";
        String schema = "qatest";
        ResultSet result = runQuery("SELECT table_name,column_name ,constraint_name FROM information_schema.key_column_usage where table_name = '" + tbl_name + "' and table_schema = '" + schema + "';");
        String col_name = result.getString("column_name");
        String actual_const_name = result.getString("constraint_name");
        String expected_const_name = tbl_name + "_pk_" + col_name;
        System.out.println(actual_const_name);
        Reporter.log("Primary Key Constraint: " + actual_const_name);
        Assert.assertEquals(actual_const_name, expected_const_name);

    }

    //    @Test
    // TODO Figure out the logic for tbl_name and Scheme Name
    public static void checkIfPrimaryKeyExist() throws SQLException {
        String tbl_name = "base_plan";
        String schema = "qatest";
        ResultSet result = runQuery("SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_type = 'PRIMARY KEY' AND table_name = '" + tbl_name + "' and table_schema = '" + schema + "';");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 1);

        // TODO Failure Case logging
    }

    //    @Test
    @QAFDataProvider(sqlQuery = "SELECT column_name FROM information_schema.columns WHERE is_nullable ='NO' and table_name ='base_plan' and table_schema ='qatest';")
    public static void checkNotNULL(Map<String, String> data) throws SQLException {
        String tbl_name = "base_plan";
        String schema = "qatest";
        System.out.println(data.get("column_name"));
        ResultSet result = runQuery("SELECT COUNT(*) FROM " + schema + "." + tbl_name + " WHERE " + data.get("column_name") + " is null;");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);
        // TODO Add logic to logged those id whose constraint is violated

    }

    //    @Test
    @QAFDataProvider(sqlQuery = "SELECT column_name FROM information_schema.columns WHERE data_type ='ARRAY' and table_name ='base_plan';")
    public static void checkEnum(Map<String, String> data) throws SQLException {
        String tbl_name = "base_plan";
        String schema = "qatest";
        System.out.println("ColumnName: " + data.get("column_name"));
        ResultSet result = runQuery("SELECT COUNT(*) FROM " + schema + "." + tbl_name + " WHERE cast(" + data.get("column_name") + " as text) !~ '^([0-9][0-9]*|\\{[0-9][0-9]*(,[0-9][0-9]*)*\\})$';");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);

        // TODO Add assertion and logic to logged those id whose constraint is violated
    }

    //     @Test
    @QAFDataProvider(sqlQuery = "SELECT column_name FROM information_schema.columns WHERE table_name ='base_plan' and data_type in ('character varying','text');")
    public static void checkEmptyQuotes(Map<String, String> data) throws SQLException {
        String tbl_name = "base_plan";
        String schema = "qatest";
        System.out.println("ColumnName: " + data.get("column_name"));
        ResultSet result = runQuery("SELECT COUNT(*) FROM " + schema + "." + tbl_name + " WHERE ltrim(trim(" + data.get("column_name") + ")) = '';");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);
        // TODO Add logic to logged those id whose constraint is violated
    }

    //     @Test
    @QAFDataProvider(sqlQuery = "SELECT column_name FROM information_schema.columns WHERE table_name ='base_plan' and data_type ='boolean';")
    public static void checkBoolean(Map<String, String> data) throws SQLException {
        String tbl_name = "base_plan";
        String schema = "qatest";
        System.out.println("ColumnName: " + data.get("column_name"));
        ResultSet result = runQuery("SELECT COUNT(*) FROM " + schema + "." + tbl_name + " WHERE " + data.get("column_name") + " NOT IN (true,false) AND " + data.get("column_name") + " is not null;");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);
        // TODO Add logic to logged those id whose constraint is violated
    }

    //     @Test
    public static void checkDuplicationOnPKAndUniqueKey() throws SQLException {

        String tbl_name = "base_plan";
        String schema = "qatest";
        ResultSet result_pk = runQuery("SELECT table_name,column_name ,constraint_name FROM information_schema.key_column_usage where table_name = '" + tbl_name + "' and table_schema = '" + schema + "';");
//        String col_name = result_pk.getString("column_name");
        String col_name = result_pk.getString("column_name");
        ResultSet result = runQuery("SELECT count(*) FROM ( SELECT COUNT(*) OVER (PARTITION BY " + col_name + ") AS cnt FROM " + schema + "." + tbl_name + ") AS t WHERE t.cnt > 1");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);
        // TODO Add assertion and logic to logged those id whose constraint is violated
    }

    //     @Test
    @QAFDataProvider(sqlQuery = "SELECT column_name FROM information_schema.columns WHERE table_name ='base_plan' and data_type ='date';")
    public static void checkDate(Map<String, String> data) throws SQLException {
        String tbl_name = "base_plan";
        String schema = "qatest";
        System.out.println("ColumnName: " + data.get("column_name"));

        ResultSet result = runQuery("SELECT COUNT(*) FROM " + schema + "." + tbl_name + " WHERE cast(" + data.get("column_name") + " as text) !~ '[0-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]$';");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);
        // TODO logic to logged those id whose constraint is violated

    }

    //     @Test
    @QAFDataProvider(sqlQuery = "SELECT column_name FROM information_schema.columns WHERE table_name ='base_plan' and data_type ='timestamp with time zone';")
    public static void checkTimestamp(Map<String, String> data) throws SQLException {
        String tbl_name = "base_plan";
        String schema = "qatest";
        System.out.println("ColumnName: " + data.get("column_name"));
        ResultSet result = runQuery("SELECT COUNT(*) FROM " + schema + "." + tbl_name + " WHERE cast(" + data.get("column_name") + " as text) !~ '(\\d{4}-\\d{2}-\\d{2}) +(\\d{2}:\\d{2}:\\d{2}\\+\\d{2})';");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);
        // TODO Add logic to logged those id whose constraint is violated

    }

    //        @Test
    @QAFDataProvider(sqlQuery = "SELECT column_name FROM information_schema.columns WHERE data_type IN ('character varying','text') and table_name ='base_plan' and table_schema ='qatest';")
    public static void checkNullAsStringValue(Map<String, String> data) throws SQLException {
        String tbl_name = "base_plan_old";
        String schema = "qatest";
        System.out.println(data.get("column_name"));
        ResultSet result = runQuery("SELECT COUNT(*) FROM " + schema + "." + tbl_name + " WHERE " + data.get("column_name") + " ='null';");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);
        // TODO Add assertion and logic to logged those id whose constraint is violated
    }


    @AfterSuite
    public void tearDown() throws SQLException {
        CONN.close();
        System.out.println("Closing Database connection");
    }
}
