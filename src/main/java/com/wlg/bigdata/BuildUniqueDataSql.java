package com.wlg.bigdata;

import com.wlg.bigdata.constant.Constants;
import com.wlg.bigdata.constant.SysConstants;
import com.wlg.bigdata.util.BaseUtil;

/**
 * 拼接根据主键去重数据sql
 * select * from （select row_num over(xxxx) from t1） where row_num=1
 */
public class BuildUniqueDataSql extends BuildSql{

    private static BuildUniqueDataSql ins = new BuildUniqueDataSql();

    private BuildUniqueDataSql(){}

    public static BuildUniqueDataSql getIns(){
        return ins;
    }

    /**
     * 根据主键获取表中唯一值
     * @param key  row_num 主键字段
     * @param rowNumSort row_num 排序字段
     * @param tableName 表名
     * @param tableFields   表字段
     * @param hourPartition 分区类型，true 为小时分区
     * @param partitionVal 表分区的值
     * @return
     */
    public String buildUniqueDataSql(String key,
                                         String rowNumSort,
                                         String tableName,
                                         String tableFields,
                                         String partitionVal,
                                         boolean hourPartition) {
        //select关键字
        StringBuilder sql = BaseUtil.getSelect();
        //row_num
        String row_num_sql = Constants.row_num.replace(Constants.key_prefix, key)
                .replace(Constants.row_sort_prefix, rowNumSort);
        sql.append(Constants.empty).append(row_num_sql);
        //字段 & 表名
        BaseUtil.appendFieldsAndTable(tableFields,tableName, sql);
        //where 关键字
        sql.append(Constants.empty).append(Constants.where);
        //添加分区条件
        String pt_val = BaseUtil.getPartitionWhere(partitionVal, hourPartition);
        sql.append(Constants.empty).append(pt_val);
        //添加主键不为空条件
        String key_sql = Constants.key_not_null.replace(Constants.key_prefix,key);
        sql.append(Constants.empty).append(Constants.and).append(key_sql);

        return sql.toString();
    }



    /**
     * 根据主键获取表中唯一值
     * @param key  row_num 主键字段
     * @param rowNumSort row_num 排序字段
     * @param tableName 表名
     * @param tableFields   表字段
     * @param hourPartition 分区类型，true 为小时分区
     * @return
     */
    public String buildUniqueDataSql(String key,
                                         String rowNumSort,
                                         String tableName,
                                         String tableFields,
                                         boolean hourPartition){
        String partitionVal = SysConstants.BIZ_DATE;
        if(hourPartition){
            partitionVal = SysConstants.BIZ_HOUR;
        }
        return buildUniqueDataSql(key,rowNumSort,tableName,tableFields,partitionVal,hourPartition);
    }

}
