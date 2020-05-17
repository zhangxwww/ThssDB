package cn.edu.thssdb.service;

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

    public void insertTableRow(String tbName, Row r){

        return ;
    }

    public void insertTableRow(String[] attrNames, Entry[] entries){

        return ;
    }

    public String getTablePrimaryAttr(String tbName){
        return "ID";
    }

}
