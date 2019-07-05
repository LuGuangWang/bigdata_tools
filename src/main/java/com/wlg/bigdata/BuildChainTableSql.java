package com.wlg.bigdata;

import com.wlg.bigdata.constant.Constants;
import com.wlg.bigdata.constant.SysConstants;
import com.wlg.bigdata.util.BaseUtil;

/**
 * 构建拉链表sql
 */
public class BuildChainTableSql extends BuildSql{

    private final String table_prefix = "${table_name}";
    //存放历史全量数据
    private final String old_table = "tmp_${table_name}_old";
    //存放最新全量数据
    private final String new_table = "tmp_${table_name}_new";

    private static BuildChainTableSql ins = new BuildChainTableSql();

    private BuildChainTableSql(){}

    public static BuildChainTableSql getIns(){
        return ins;
    }

    //存储最新的全量数据
    public String buildSaveNewTableSql(String key,
                                       String rowNumSort,
                                       String tableName,
                                       String tableFields,
                                       boolean hourPartition){
        StringBuilder sql = new StringBuilder();
        //临时表名
        String newTableName = new_table.replace(table_prefix,tableName.replace(".","_"));
        //先清理临时表数据
        sql.append(Constants.drop_table).append(newTableName).append(Constants.seg);
        //创建新表
        sql.append(Constants.create_table).append(newTableName).append(Constants.as);
        //新表sql
        String new_table_sql = buildNewTableSql(key,rowNumSort,tableName,tableFields,hourPartition);
        sql.append(new_table_sql).append(Constants.seg);
        return sql.toString();
    }


    //拼接最新全量数据sql
    private String buildNewTableSql(String key,
                                    String rowNumSort,
                                    String tableName,
                                    String tableFields,
                                    boolean hourPartition){
        StringBuilder sql = BaseUtil.getSelect();
        //获取row_num
        String row_num_sql = BuildUniqueDataSql.getIns().buildUniqueDataSql(key,rowNumSort,tableName,tableFields,hourPartition);
        //临时表名
        String tmpName = Constants.CL + row_num_sql + Constants.CR + "tmp";
        BaseUtil.appendFieldsAndTable(tableFields,tmpName, sql);
        //where 关键字
        sql.append(Constants.empty).append(Constants.where);
        //row_num=1
        sql.append(Constants.empty).append(Constants.row_num_eq_1);
        return sql.toString();
    }


}
