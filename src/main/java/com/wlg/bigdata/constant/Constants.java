package com.wlg.bigdata.constant;

//常量
public abstract class Constants {
    public static final String empty = " ";
    public static final String CL = " ( ";
    public static final String CR = " ) ";
    public static final String seg = ";\n";
    public static final String dot = ",";
    public static final String dot1 = ".";
    public static final String eq = " = ";
    public static final String not_eq = " != ";
    public static final String dot2 = "'";

    public static final String select = "select ";
    public static final String from =" from ";
    public static final String where = " where ";
    public static final String and = " and ";
    public static final String or = " or ";
    public static final String drop_table = "drop table if exists ";
    public static final String create_table = "create table ";
    public static final String as = " as ";
    public static final String left_join = " left join ";
    public static final String on = " on ";
    public static final String insert_overwrite = "insert overwrite table ";


    public static final String key_prefix = "${key}";
    public static final String row_sort_prefix = "${sortkey}";
    public static final String partition_prefix = "${partition}";
    public static final String field_name_prefix = "${field_name}";
    public static final String default_val_prefix = "${default_val}";
    public static final String day_prefix = "${Day}";
    public static final String month_prefix = "${Month}";
    public static final String year_prefix = "${Year}";

    //row_num_eq_1 & row_num是一对
    public static final String row_num_eq_1 = "row_num = 1";
    public static final String row_num = "row_number() over(distribute by ${key} sort by ${sortkey} desc) row_num,";
    //主键不能为空
    public static final String key_not_null = "${key} is not null";
    //按天分区
    public static final String p_dt = "concat(year,month,day)='${partition}'";
    //按小时分区
    public static final String p_ht = "concat(year,month,day,hour)='${partition}'";
    //nvl
    public static final String nvl = "nvl(${field_name},'${default_val}')";
    //partiton_dt
    public static final String partition_dt = " partition(year='${Year}',month='${Month}',day='${Day}') ";
}
