package com.wlg.bigdata;

import com.wlg.bigdata.constant.Constants;
import com.wlg.bigdata.util.BaseUtil;

/**
 * 拼接普通sql
 * select * from table
 */
public class BuildNormalSql extends BuildSql{

    private static BuildNormalSql ins = new BuildNormalSql();

    private BuildNormalSql(){}

    public static BuildNormalSql getIns(){
        return ins;
    }

    /**
     *
     * @param tableFields  字段名
     * @param tableName 表名
     * @param partitionVal  分区值
     * @param hourPartition 分区类型（天/小时）
     * @return
     */
    public String buildNormalSql(String tableFields, String tableName,
                                  String partitionVal,boolean hourPartition) {
        StringBuilder sql = BaseUtil.getSelect();
        BaseUtil.appendFieldsAndTable(tableFields,tableName,sql);
        //where条件
        sql.append(Constants.where);
        //添加分区条件
        String pt_val = BaseUtil.getPartitionWhere(partitionVal,hourPartition);
        sql.append(pt_val);

        return sql.toString();
    }
}
