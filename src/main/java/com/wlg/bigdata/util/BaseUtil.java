package com.wlg.bigdata.util;

import com.wlg.bigdata.constant.Constants;

//基础拼接方法
public abstract class BaseUtil {
    /**
     * 拼接 表名字 & 字段
     * @param tableName 表名
     * @param tableFields 表中字段
     * @param sql
     */
    public static void appendFieldsAndTable(String tableFields, String tableName, StringBuilder sql) {
        //要拉取的字段
        sql.append(Constants.empty).append(tableFields.trim());
        //from关键字
        sql.append(Constants.empty).append(Constants.from);
        //表名
        sql.append(Constants.empty).append(tableName);
    }

    public static StringBuilder getSelect(){
        return new StringBuilder(Constants.select);
    }
}
