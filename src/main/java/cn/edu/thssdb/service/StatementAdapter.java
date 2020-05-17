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

    public void insertTableRow(String tbName, Row r) {
        Table t = dbTEST.getTable(tbName);
        t.insert(r);
    }

    public void insertTableRow(String tbName, String[] attrNames, Entry[] entries) {
        Table t = dbTEST.getTable(tbName);
        // TODO
        return;
    }

    public String getTablePrimaryAttr(String tbName) {
        return "ID";
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
