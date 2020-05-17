package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.WrongInsertArgumentNumException;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thssdb.schema.*;

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

    public void insertTableRow(String tbName, String[] attrValues) {
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
            for (int i = 0; i < attrs.size(); i++) {
                Column tmpAttr = attrs.get(i);
                ColumnType attrType = tmpAttr.getType();
                Entry e = null;
                switch (attrType) {
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
        return;
    }

    public void insertTableRow(String tbName, String[] attrNames, String[] attrValues) {
        //value的个数要与colomn个数一致
        if (attrNames.length != attrValues.length) {
            //TODO 异常处理 所给长度不一致
            throw new WrongInsertArgumentNumException();
        } else {
            String primaryKeyName = getTablePrimaryAttr(tbName);
            boolean hasPrimaryKey = false;
            for (int j = 0; j < attrNames.length; j++) {
                if (attrNames[j].equals(primaryKeyName)) {
                    hasPrimaryKey = true;
                }
            }
            if (!hasPrimaryKey) {
                //TODO 异常处理 插入的属性中没有主键
                return;
            }

            //插入一条Row
            Table t = dbTEST.getTable(tbName);
            ArrayList<Column> attrs = t.getColumns();
            Entry[] entries = new Entry[attrs.size()];
            for (int i = 0; i < attrs.size(); i++) {
                Column tmpAttr = attrs.get(i);
                Entry e = null;
                for (int j = 0; j < attrNames.length; j++) {
                    if (attrNames[j].toUpperCase().equals(tmpAttr.getName().toUpperCase())) {
                        ColumnType attrType = tmpAttr.getType();
                        switch (attrType) {
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
        return;
    }


    public void delFromTable(String tbName, WhereCondition wherecond) {
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
        QueryTable[] q;
        boolean needJoin;
        int jIndex1 = -1, jIndex2 = -1;
        if (table2 == null || table2.equals("") || jc == null) {
            // 不需要join
            q = new QueryTable[]{getQueryTable(table1, wc)};
            needJoin = false;
        } else {
            // 检查where中是否存在歧义
            if (!checkAmbiguousColumnForWhere(table1, table2, wc)) {
                // TODO throw error: ambiguous column in where condition
            }
            q = new QueryTable[]{getQueryTable(table1, wc), getQueryTable(table2, wc)};
            // 哪两列被join
            List<Integer> joinIndex = getJoinIndex(table1, table2, jc);
            jIndex1 = joinIndex.get(0);
            jIndex2 = joinIndex.get(1);
            needJoin = true;
        }
        // select哪些列
        List<Integer> index = getSelectIndex(results, table1, table2);
        List<Row> qResult = new QueryResult(q, index, needJoin, jIndex1, jIndex2).query();
        return generateTmpTable(qResult, table1, table2, index);
    }

    private QueryTable getQueryTable(String table, WhereCondition wc) {
        QueryTable q;
        Table t = dbTEST.getTable(table);
        if (wc != null && (wc.tableName.equals(table) || wc.tableName.equals(""))) {
            AttrCompare compare = new AttrCompare(wc.op);
            String attr = wc.attr;
            ColumnType cType = null;
            for (Column c : t.getColumns()) {
                if (c.getName().equals(attr)) {
                    cType = c.getType();
                    break;
                }
                // TODO throw error: attr not in columns
            }
            String value = wc.val;
            Entry instant = parseValue(cType, value);
            q = new QueryTable(compare, t, attr, instant);
        } else {
            q = new QueryTable(null, t, null, null);
        }
        return q;
    }


    private Entry parseValue(ColumnType type, String value) {
        Entry instant;
        switch (type) {
            case INT:
                instant = new Entry(Integer.parseInt(value));
                break;
            case LONG:
                instant = new Entry(Long.parseLong(value));
                break;
            case FLOAT:
                instant = new Entry(Float.parseFloat(value));
                break;
            case DOUBLE:
                instant = new Entry(Double.parseDouble(value));
                break;
            case STRING:
            default:
                instant = new Entry(value);
                break;
        }
        return instant;
    }

    private List<Integer> getSelectIndex(List<Pair<String, String>> res, String t1, String t2) {
        List<Integer> results = new ArrayList<>();
        List<String> attrs = new ArrayList<>();
        int midIndex = mergeAttrs(t1, t2, attrs);
        for (Pair<String, String> p : res) {
            String tbName = p.getKey();
            String attrName = p.getValue();
            int index = 0;
            if (tbName.equals("")) {
                int first = attrs.indexOf(attrName);
                int last = attrs.lastIndexOf(attrName);
                if (first != last) {
                    // TODO throw error: ambiguous column name
                } else if (first < 0) {
                    // TODO throw error: column not exists
                } else {
                    index = first;
                }
            } else {
                if (tbName.equals(t1)) {
                    int tmp = attrs.indexOf(attrName);
                    if (tmp >= midIndex) {
                        // TODO throw error: attrName not exists in table
                    } else {
                        index = tmp;
                    }
                } else if (tbName.equals(t2)) {
                    int tmp = attrs.lastIndexOf(attrName);
                    if (tmp < midIndex) {
                        // TODO throw error: attrName not exists in table
                    } else {
                        index = tmp;
                    }
                } else {
                    // TODO throw error: table not exists
                }
            }
            results.add(index);
        }
        return results;
    }

    private List<Integer> getJoinIndex(String t1, String t2, JoinCondition jc) {
        List<Integer> results = new ArrayList<>();
        List<String> attrs = new ArrayList<>();
        int midIndex = mergeAttrs(t1, t2, attrs);
        ArrayList<Column> c1 = dbTEST.getTable(t1).getColumns();
        ArrayList<Column> c2 = dbTEST.getTable(t2).getColumns();
        if (jc.table1.equals(t1) && jc.table2.equals(t2)) {
            for (int i = 0; i < c1.size(); ++i) {
                if (c1.get(i).getName().equals(jc.attr1)) {
                    results.set(0, i);
                    break;
                }
                // TODO throw error: attr not in table
            }
            for (int i = 0; i < c2.size(); ++i) {
                if (c2.get(i).getName().equals(jc.attr2)) {
                    results.set(1, i + midIndex);
                    break;
                }
                // TODO throw error: attr not in table
            }
        } else if (jc.table1.equals(t2) && jc.table2.equals(t1)) {
            for (int i = 0; i < c1.size(); ++i) {
                if (c1.get(i).getName().equals(jc.attr2)) {
                    results.set(0, i);
                    break;
                }
                // TODO throw error: attr not in table
            }
            for (int i = 0; i < c2.size(); ++i) {
                if (c2.get(i).getName().equals(jc.attr1)) {
                    results.set(1, i + midIndex);
                    break;
                }
                // TODO throw error: attr not in table
            }
        } else {
            // TODO throw error: table name can't match
        }
        return results;
    }

    private boolean checkAmbiguousColumnForWhere(String t1, String t2, WhereCondition wc) {
        if (wc == null) {
            return true;
        }
        if (wc.tableName.equals(t1) || wc.tableName.equals(t2)) {
            return true;
        }
        if (wc.tableName.equals("")) {
            Table table1 = dbTEST.getTable(t1);
            Table table2 = dbTEST.getTable(t2);
            boolean found = false;
            for (Column c : table1.getColumns()) {
                if (c.getName().equals(wc.attr)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return true;
            }
            for (Column c : table2.getColumns()) {
                if (c.getName().equals(wc.attr)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private Table generateTmpTable(List<Row> rows, String t1, String t2, List<Integer> index) {
        List<Column> newColumn = new ArrayList<>();
        List<String> attrs = new ArrayList<>();
        int midIndex = mergeAttrs(t1, t2, attrs);
        Table table1, table2;
        List<Column> c1 = new ArrayList<>();
        List<Column> c2 = new ArrayList<>();
        table1 = dbTEST.getTable(t1);
        c1 = table1.getColumns();
        if (t2 != null && !t2.equals("")) {
            table2 = dbTEST.getTable(t2);
            c2 = table2.getColumns();
        }
        for (int i : index) {
            if (i < midIndex) {
                newColumn.add(new Column(c1.get(i)));
            } else {
                newColumn.add(new Column(c2.get(i - midIndex)));
            }
        }
        Table tmpTable = new Table("", "__TEMP", (Column[]) newColumn.toArray(), table1.primaryIndex);
        for (Row r : rows) {
            tmpTable.insert(r);
        }
        return tmpTable;
    }

    private int mergeAttrs(String t1, String t2, List<String> mergedAttrs) {
        Table table1, table2;
        int midIndex = 0;
        if (t1 != null && !t1.equals("")) {
            table1 = dbTEST.getTable(t1);
            midIndex = tableAttrsNum(t1);
            for (Column c : table1.getColumns()) {
                mergedAttrs.add(c.getName());
            }
        }
        if (t2 != null && !t2.equals("")) {
            table2 = dbTEST.getTable(t2);
            for (Column c : table2.getColumns()) {
                mergedAttrs.add(c.getName());
            }
        }
        return midIndex;
    }
}
