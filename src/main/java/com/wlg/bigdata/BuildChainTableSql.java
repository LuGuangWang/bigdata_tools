package com.wlg.bigdata;

import com.wlg.bigdata.constant.Constants;
import com.wlg.bigdata.constant.SysConstants;

/**
 * 构建拉链表sql
 */
public class BuildChainTableSql extends BuildSql{

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
    private String buildGetUniqueDataSql(String key,
                                        String rowNumSort,
                                        String tableName,
                                        String tableFields,
                                        String partitionVal,
                                        boolean hourPartition) {
        //select关键字
        StringBuilder sql = new StringBuilder(Constants.select);
        //row_num
        String row_num_sql = Constants.row_num.replace(Constants.key_prefix, key)
                .replace(Constants.row_sort_prefix, rowNumSort);
        sql.append(Constants.empty).append(row_num_sql);
        //要拉取的字段
        sql.append(Constants.empty).append(tableFields.trim());
        //from关键字
        sql.append(Constants.empty).append(Constants.from);
        //表名
        sql.append(Constants.empty).append(tableName);
        //where 关键字
        sql.append(Constants.empty).append(Constants.where);
        //分区
        String pt_val;
        if(hourPartition){//按小时分区
            pt_val = Constants.p_ht.replace(Constants.partition_prefix,partitionVal);
        }else{//按天分区
            pt_val = Constants.p_dt.replace(Constants.partition_prefix,partitionVal);
        }
        sql.append(Constants.empty).append(pt_val);
        //主键不为空
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
    public String buildGetUniqueDataSql(String key,
                                        String rowNumSort,
                                        String tableName,
                                        String tableFields,
                                        boolean hourPartition){
        String partitionVal = SysConstants.BIZ_DATE;
        if(hourPartition){
            partitionVal = SysConstants.BIZ_HOUR;
        }
        return buildGetUniqueDataSql(key,rowNumSort,tableName,tableFields,partitionVal,hourPartition);
    }



    //构建拉链表sql
    public String buildSql() {
        return null;
    }
}
