package com.qmetry.qaf.nbs.steps;

public class DataQuery {

    public static final String table_name = "base_plan";
    public static final String schema_name = "public";
    public static final String NOT_NULL = "SELECT column_name FROM information_schema.columns WHERE is_nullable ='NO' and table_name ='" + table_name + "' and table_schema ='" + schema_name + "';";
    public static final String ENUM = "SELECT column_name FROM information_schema.columns WHERE data_type ='ARRAY' and table_name ='" + table_name + "' and table_schema ='" + schema_name + "';";
    public static final String EMPTY_QUOTES = "SELECT column_name FROM information_schema.columns WHERE table_name ='" + table_name + "' and data_type in ('character varying','text') and table_schema ='" + schema_name + "';";
    public static final String BOOLEAN = "SELECT column_name FROM information_schema.columns WHERE table_name ='" + table_name + "' and data_type ='boolean' and table_schema ='" + schema_name + "';";
    public static final String DATE = "SELECT column_name FROM information_schema.columns WHERE table_name ='" + table_name + "' and data_type ='date' and table_schema ='" + schema_name + "';";
    public static final String TIMESTAMP = "SELECT column_name FROM information_schema.columns WHERE table_name ='" + table_name + "' and data_type ='timestamp with time zone' and table_schema ='" + schema_name + "';";
    public static final String NULL_STRING = "SELECT column_name FROM information_schema.columns WHERE data_type IN ('character varying','text') and table_name ='" + table_name + "' and table_schema ='" + schema_name + "';";

}