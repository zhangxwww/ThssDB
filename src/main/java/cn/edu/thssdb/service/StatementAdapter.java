package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.WrongInsertArgumentNumException;
import cn.edu.thssdb.query.AttrCompare;
import cn.edu.thssdb.query.JoinCondition;
import cn.edu.thssdb.query.WhereCondition;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;

import java.util.ArrayList;
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

    public void insertTableRow(String tbName, String[] attrValues){
        //插入整行
        int attrsNum = tableAttrsNum(tbName);
        if (attrValues.length != attrsNum) {
            //TODO 异常处理 所给values个数不对
            System.out.println("Insert Failure! valueEntries.size()! = attrsNum");
            return;
        } else {
            //TODO 插入整行
            Table t = dbTEST.getTable(tbName);
            ArrayList<Column> attrs = t.getColumns();
            Entry[] entries = new Entry[attrsNum];
            for (int i = 0; i < attrs.size(); i++){
                Column tmpAttr = attrs.get(i);
                ColumnType attrType = tmpAttr.getType();
                Entry e = null;
                switch (attrType){
                    case INT:
                        e = new Entry(Integer.parseInt(attrValues[i]));
                        break;
                    case LONG:
                        e = new Entry(Long.parseLong(attrValues[i]));
                        break;
                    case FLOAT:
                        e = new Entry(Float.parseFloat(attrValues[i]));
                        break;
                    case DOUBLE:
                        e = new Entry(Double.parseDouble(attrValues[i]));
                        break;
                    case STRING:
                        e = new Entry(attrValues[i]);
                        break;
                }
                entries[i] = e;
            }
            t.insert(new Row(entries));
        }
        return ;
    }

    public void insertTableRow(String tbName, String[] attrNames, String[] attrValues){
        //value的个数要与colomn个数一致
        if (attrNames.length != attrValues.length) {
            //TODO 异常处理 所给长度不一致
            throw new WrongInsertArgumentNumException();
        } else {
            String primaryKeyName =getTablePrimaryAttr(tbName);
            boolean hasPrimaryKey = false;
            for (int j = 0; j < attrNames.length; j++) {
                if (attrNames[j].equals(primaryKeyName)){
                    hasPrimaryKey = true;
                }
            }
            if (!hasPrimaryKey){
                //TODO 异常处理 插入的属性中没有主键
                return;
            }

            //插入一条Row
            Table t = dbTEST.getTable(tbName);
            ArrayList<Column> attrs = t.getColumns();
            Entry[] entries = new Entry[attrs.size()];
            for (int i = 0; i < attrs.size(); i++){
                Column tmpAttr = attrs.get(i);
                Entry e = null;
                for(int j = 0; j < attrNames.length; j++){
                    if (attrNames[j].toUpperCase().equals(tmpAttr.getName().toUpperCase())){
                        ColumnType attrType = tmpAttr.getType();
                        switch (attrType){
                            case INT:
                                e = new Entry(Integer.parseInt(attrValues[j]));
                                break;
                            case LONG:
                                e = new Entry(Long.parseLong(attrValues[j]));
                                break;
                            case FLOAT:
                                e = new Entry(Float.parseFloat(attrValues[j]));
                                break;
                            case DOUBLE:
                                e = new Entry(Double.parseDouble(attrValues[j]));
                                break;
                            case STRING:
                                e = new Entry(attrValues[j]);
                                break;
                        }
                        break;
                    }//为给出的属性值就为null
                }
                entries[i] = e;
            }
            t.insert(new Row(entries));
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
        return null;
    }
}
