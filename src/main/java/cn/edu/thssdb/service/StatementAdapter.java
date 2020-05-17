package cn.edu.thssdb.service;

import cn.edu.thssdb.query.AttrCompare;
import cn.edu.thssdb.query.JoinCondition;
import cn.edu.thssdb.query.WhereCondition;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;

import java.util.List;
import cn.edu.thssdb.schema.*;
import javafx.util.Pair;

import java.util.List;
import java.util.Map;

public class StatementAdapter {

    private Manager m;
    private Database dbTEST;

    public StatementAdapter() {
        m = new Manager();
        m.createDatabaseIfNotExists("TEST");
        dbTEST = m.switchDatabase("TEST");
    }

    public void createTable(String tbName, Column[] cols) {
        dbTEST.createTable(tbName, cols);
    }

    public void dropTable(String tbName) {
        dbTEST.dropTable(tbName);
    }

    public int tableAttrsNum(String tbName) {
        //获取该表的属性个数
        Table t = dbTEST.getTable(tbName);
        return t.getColumns().size();
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

    public String getTablePrimaryAttr(String tbName) {
        return "ID";
    }

    public void updateTable(String tbName, String colName, String attrValue, WhereCondition wherecond) {
        return;
    }



    public Table select(List<Pair<String, String>> results, String table1, String table2, JoinCondition jc, WhereCondition wc) {
        // TODO
        if (table2 == null || table2.equals("") || jc == null) {
            return selectWithoutJoin(results, table1, wc);
        }
        return null;
    }

    private Table selectWithoutJoin(List<Pair<String, String>> results, String table, WhereCondition wc) {
        AttrCompare compare = new AttrCompare(wc.op);
        Table t = dbTEST.getTable(table);
        String attr = wc.attr;
        ColumnType cType = null;
        for (Column c : t.getColumns()) {
            if (c.getName().equals(attr)){
                cType = c.getType();
                break;
            }
            // TODO throw error: attr not in columns
        }
        Entry instant;
        String value = wc.val;
        switch (cType) {
            case INT:
                instant = new Entry(Integer.parseInt(value));
                break;
        }
    }
}
