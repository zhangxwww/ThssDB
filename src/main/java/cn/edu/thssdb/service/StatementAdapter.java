package cn.edu.thssdb.service;

import cn.edu.thssdb.query.WhereCondition;
import cn.edu.thssdb.schema.*;

import java.util.Map;

public class StatementAdapter {

    public void createTable(String tbName, Column[] cols){

        return ;
    }

    public void dropTable(String tbName){

        return ;
    }

    public int tableAttrsNum(String tbName){
        //获取该表的属性个数
        //TODO
        return 3;
    }

    public void insertTableRow(String tbName, String[] entries){
        //插入整行
        int attrsNum = tableAttrsNum(tbName);
        if (entries.length != attrsNum) {
            //TODO 异常处理 所给values个数不对
            System.out.println("Insert Failure! valueEntries.size()! = attrsNum");
            return;
        } else {
            //TODO 插入整行


        }
        return ;
    }

    public void insertTableRow(String tbName, String[] attrNames, String[] attrValues){
        //value的个数要与colomn个数一致
        if (attrNames.length != attrValues.length) {
            //TODO 异常处理 所给长度不一致
            return;
        } else {
            String primaryKeyName =getTablePrimaryAttr(tbName);
            boolean hasPrimaryKey = false;
            for (int j = 0; j < attrNames.length; j++) {
                if (attrNames[j].equals(primaryKeyName)){
                    hasPrimaryKey = true;
                }
            }
            if (hasPrimaryKey){

            }else{
                //TODO 异常处理 插入的属性中没有主键
                return;
            }
        }
        return ;
    }

    public void delFromTable(String tbName, WhereCondition wherecond){
        //whereCondition可能为空
        return;
    }

    public String getTablePrimaryAttr(String tbName){
        return "ID";
    }

    public void updateTable(String tbName, String colName, String attrValue, WhereCondition wherecond) {
        return;
    }



}
