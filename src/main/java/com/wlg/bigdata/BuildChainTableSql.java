package com.wlg.bigdata;

import com.wlg.bigdata.constant.Constants;
import com.wlg.bigdata.constant.SysConstants;
import com.wlg.bigdata.util.BaseUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * 构建拉链表sql
 */
public class BuildChainTableSql extends BuildSql {

    private final String table_prefix = "${table_name}";
    //存放历史全量数据
    private final String old_table_rule = "tmp_${table_name}_old";
    //存放最新全量数据
    private final String new_table_rule = "tmp_${table_name}_new";

    private static BuildChainTableSql ins = new BuildChainTableSql();

    private BuildChainTableSql() {
    }

    public static BuildChainTableSql getIns() {
        return ins;
    }

    public String buildSql(String key,
                           String rowNumSort,
                           String tableName,
                           String tableFields,
                           boolean hourPartition,
                           String targetTableName) {
        StringBuilder sql = new StringBuilder();
        //存储最新的全量数据
        String newSaveSql = buildSaveNewTableSql(key, rowNumSort, tableName, tableFields, hourPartition);
        sql.append(newSaveSql);
        //存储昨日全量数据
        String oldSaveSql = buildSaveOldTableSql(tableFields,targetTableName);
        sql.append(oldSaveSql);
        //存储最新全量数据到目标表




        return sql.toString();
    }

    //整理最新全量数据sql
    private String buildNewDataSql(String key,
                                   String rowNumSort,
                                   String tableName,
                                   String tableFields,
                                   boolean hourPartition,
                                   String targetTableName){
        StringBuilder sql = new StringBuilder();
        //昨日全量数据临时表
        String oldDataTable = getTmpTableName(targetTableName, old_table_rule);
        //今日最新全量数据临时表
        String newDataTable = getTmpTableName(tableName, new_table_rule);

        Set<String> fields = new HashSet<>();


        return sql.toString();
    }

    //存储昨日全量数据
    private String buildSaveOldTableSql(String tableFields,
                                        String targetTableName){
        StringBuilder sql = new StringBuilder();
        String tmpTableName = getTmpTableName(targetTableName, old_table_rule);
        //先清理临时表数据
        sql.append(Constants.drop_table).append(tmpTableName).append(Constants.seg);
        //创建新表
        sql.append(Constants.create_table).append(tmpTableName).append(Constants.as);
        //获取昨日全量数据sql
        String old_sql = buildOldTableSql(tableFields,targetTableName);
        sql.append(old_sql).append(Constants.seg);

        return sql.toString();
    }

    private String getTmpTableName(String tableName, String tableNameRule) {
        //临时表名
        return tableNameRule.replace(table_prefix, tableName.replace(".", "_"));
    }

    //获取昨日全量数据
    private String buildOldTableSql(String tableFields,
                                    String targetTableName) {
        return BuildNormalSql.getIns().buildNormalSql(tableFields, targetTableName, SysConstants.END_DT_T, false);
    }

    //存储最新的全量数据
    private String buildSaveNewTableSql(String key,
                                        String rowNumSort,
                                        String tableName,
                                        String tableFields,
                                        boolean hourPartition) {
        StringBuilder sql = new StringBuilder();
        //临时表名
        String newTableName = getTmpTableName(tableName, new_table_rule);
        //先清理临时表数据
        sql.append(Constants.drop_table).append(newTableName).append(Constants.seg);
        //创建新表
        sql.append(Constants.create_table).append(newTableName).append(Constants.as);
        //新表sql
        String new_table_sql = buildNewTableSql(key, rowNumSort, tableName, tableFields, hourPartition);
        sql.append(new_table_sql).append(Constants.seg);
        return sql.toString();
    }


    //拼接最新全量数据sql
    private String buildNewTableSql(String key,
                                    String rowNumSort,
                                    String tableName,
                                    String tableFields,
                                    boolean hourPartition) {
        StringBuilder sql = BaseUtil.getSelect();
        //获取row_num
        String row_num_sql = BuildUniqueDataSql.getIns().buildUniqueDataSql(key, rowNumSort, tableName, tableFields, hourPartition);
        //临时表名
        String tmpName = Constants.CL + row_num_sql + Constants.CR + "tmp";
        BaseUtil.appendFieldsAndTable(tableFields, tmpName, sql);
        //where 关键字
        sql.append(Constants.empty).append(Constants.where);
        //row_num=1
        sql.append(Constants.empty).append(Constants.row_num_eq_1);
        return sql.toString();
    }


}
