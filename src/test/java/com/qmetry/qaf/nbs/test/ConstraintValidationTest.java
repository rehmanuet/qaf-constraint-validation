package com.qmetry.qaf.nbs.test;

import com.qmetry.qaf.automation.testng.dataprovider.QAFDataProvider;
import com.qmetry.qaf.automation.util.Reporter;
import com.qmetry.qaf.nbs.utils.BaseConstraintValidation;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static com.qmetry.qaf.nbs.utils.DataQuery.*;

public class ConstraintValidationTest extends BaseConstraintValidation {

    @BeforeSuite
    public void startUp() {
        CONN = getConnection();
    }

    @Test
    public void checkPrimaryKeyNamingConvention() throws SQLException {
        ResultSet result = runQuery("SELECT table_name,column_name ,constraint_name FROM information_schema.key_column_usage where table_name = '" + table_name + "' and table_schema = '" + schema_name + "';");
        String col_name = result.getString("column_name");
        String actual_const_name = result.getString("constraint_name");
        String expected_const_name = table_name + "_pk_" + col_name;
        System.out.println(actual_const_name);
        Reporter.log("Primary Key Constraint : " + actual_const_name);
        Assert.assertEquals(actual_const_name, expected_const_name);
    }

    @Test
    public static void checkIfPrimaryKeyExist() throws SQLException {
        ResultSet result = runQuery("SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_type = 'PRIMARY KEY' AND table_name = '" + table_name + "' and table_schema = '" + schema_name + "';");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 1);
    }

    @Test
    @QAFDataProvider(sqlQuery = NOT_NULL)
    public static void checkNotNULL(Map<String, String> data) throws SQLException {
        System.out.println("Not Null Column: " + data.get("column_name"));
        ResultSet result = runQuery("SELECT COUNT(*) FROM " + schema_name + "." + table_name + " WHERE " + data.get("column_name") + " is null;");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);
    }

    @Test
    @QAFDataProvider(sqlQuery = ENUM)
    public static void checkEnum(Map<String, String> data) throws SQLException {
        System.out.println("Enum Column: " + data.get("column_name"));
        ResultSet result = runQuery("SELECT COUNT(*) FROM " + schema_name + "." + table_name + " WHERE cast(" + data.get("column_name") + " as text) !~ '^([0-9][0-9]*|\\{[0-9][0-9]*(,[0-9][0-9]*)*\\})$';");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);

    }

    @Test
    @QAFDataProvider(sqlQuery = EMPTY_QUOTES)
    public static void checkEmptyQuotes(Map<String, String> data) throws SQLException {
        System.out.println("Column: " + data.get("column_name"));
        ResultSet result = runQuery("SELECT COUNT(*) FROM " + schema_name + "." + table_name + " WHERE ltrim(trim(" + data.get("column_name") + ")) = '';");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);
    }

    @Test
    @QAFDataProvider(sqlQuery = BOOLEAN)
    public static void checkBoolean(Map<String, String> data) throws SQLException {
        System.out.println("Boolean Column: " + data.get("column_name"));
        ResultSet result = runQuery("SELECT COUNT(*) FROM " + schema_name + "." + table_name + " WHERE " + data.get("column_name") + " NOT IN (true,false) AND " + data.get("column_name") + " is not null;");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);
    }

    @Test
    public static void checkDuplicationOnPKAndUniqueKey() throws SQLException {
        ResultSet result_pk = runQuery("SELECT table_name,column_name ,constraint_name FROM information_schema.key_column_usage where table_name = '" + table_name + "' and table_schema = '" + schema_name + "';");
//        String col_name = result_pk.getString("column_name");
        String col_name = result_pk.getString("column_name");
        ResultSet result = runQuery("SELECT count(*) FROM ( SELECT COUNT(*) OVER (PARTITION BY " + col_name + ") AS cnt FROM " + schema_name + "." + table_name + ") AS t WHERE t.cnt > 1");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);
    }

    @Test
    @QAFDataProvider(sqlQuery = DATE)
    public static void checkDate(Map<String, String> data) throws SQLException {
        System.out.println("ColumnName: " + data.get("column_name"));
        ResultSet result = runQuery("SELECT COUNT(*) FROM " + schema_name + "." + table_name + " WHERE cast(" + data.get("column_name") + " as text) !~ '[0-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]$';");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);
    }

    @Test
    @QAFDataProvider(sqlQuery = TIMESTAMP)
    public static void checkTimestamp(Map<String, String> data) throws SQLException {
        System.out.println("ColumnName: " + data.get("column_name"));
        ResultSet result = runQuery("SELECT COUNT(*) FROM " + schema_name + "." + table_name + " WHERE cast(" + data.get("column_name") + " as text) !~ '(\\d{4}-\\d{2}-\\d{2}) +(\\d{2}:\\d{2}:\\d{2}\\+\\d{2})';");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);
    }

    @Test
    @QAFDataProvider(sqlQuery = NULL_STRING)
    public static void checkNullAsStringValue(Map<String, String> data) throws SQLException {
        System.out.println(data.get("column_name"));
        ResultSet result = runQuery("SELECT COUNT(*) FROM " + schema_name + "." + table_name + " WHERE " + data.get("column_name") + " ='null';");
        int actual_count = Integer.parseInt(result.getString("count"));
        Assert.assertEquals(actual_count, 0);
    }

    @AfterSuite
    public void tearDown() throws SQLException {
        CONN.close();
        System.out.println("Closing Database connection");
    }
}
