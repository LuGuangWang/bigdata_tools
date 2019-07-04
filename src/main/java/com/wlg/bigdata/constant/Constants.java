package com.wlg.bigdata.constant;

//常量
public abstract class Constants {
    public static final String empty = " ";

    public static final String select = "select";
    public static final String from ="from";
    public static final String where = "where";
    public static final String and = " and ";


    public static final String key_prefix = "${key}";
    public static final String row_sort_prefix = "${sortkey}";
    public static final String partition_prefix = "${partition}";

    public static final String row_num = "row_number() over(distribute by ${key} sort by ${sortkey} desc) row_num,";
    //主键不能为空
    public static final String key_not_null = "${key} is not null";
    //按天分区
    public static final String p_dt = "concat(year,month,day)='${partition}'";
    //按小时分区
    public static final String p_ht = "concat(year,month,day,hour)='${partition}'";
}
