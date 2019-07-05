package com.wlg.bigdata.util;

import com.wlg.bigdata.constant.Constants;
import com.wlg.bigdata.constant.SysConstants;

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
        sql.append(Constants.from);
        //表名
        sql.append(Constants.empty).append(tableName);
    }

    /**
     * 添加自定义分区条件
     * @param partitionVal
     * @param hourPartition
     * @return
     */
    public static String getPartitionWhere(String partitionVal, boolean hourPartition) {
        //分区
        String pt_val;
        if(hourPartition){//按小时分区
            pt_val = Constants.p_ht.replace(Constants.partition_prefix,partitionVal);
        }else{//按天分区
            pt_val = Constants.p_dt.replace(Constants.partition_prefix,partitionVal);
        }
        return pt_val;
    }

    /**
     * 添加默认分区条件
     * @param hourPartition
     * @return
     */
    public static String getPartitionWhere(boolean hourPartition) {
        String partitionVal = SysConstants.BIZ_DATE;
        if(hourPartition){
            partitionVal = SysConstants.BIZ_HOUR;
        }
        return getPartitionWhere(partitionVal,hourPartition);
    }

    /**
     * selet sql开头
     * @return
     */
    public static StringBuilder getSelect(){
        return new StringBuilder(Constants.select);
    }
}
