package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import com.sun.scenario.animation.shared.InfiniteClipEnvelope;
import javafx.scene.control.Tab;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import cn.edu.thssdb.schema.*;
import org.w3c.dom.Attr;

import javax.naming.directory.AttributeInUseException;
import javax.swing.text.html.MinimalHTMLWriter;

public class StatementAdapter {
    private Database database;
    private boolean isInTransaction = false;
    private List<Table> exclusiveLockedTables = new ArrayList<Table>();

    private final long transactionID;
    private LogHandler logHandler = null;

    private List<Column> resultHeader = null;
    private List<Row> resultTable = null;
    private ResultType resultType = null;
    private List<String> resultSchemaHeader = null;
    private List<List<String>> resultSchemaTable = null;

    private enum ResultType {
        TABLE,
        SCHEMA
    }

    public StatementAdapter(Database database, long sessionid) {
        this.database = database;
        transactionID = sessionid;
        this.logHandler = new LogHandler(this.database);
    }

    public void initializeTransaction() {
        this.isInTransaction = true;
    }

    public void createTable(String tbName, Column[] cols) {
        database.createTable(tbName, cols);
        if (isInTransaction) {
            logHandler.addCreateTableLog(transactionID, tbName);
        }
    }

    public void dropTable(String tbName) {
        //如果在事务处理中，就不能在这一步真的删除表
        if (isInTransaction) {
            logHandler.addDropTableLog(transactionID, tbName);
        } else {
            database.dropTable(tbName);
        }

    }

    public void showTable(String tbName) {
        Table t = database.getTable(tbName);
        resultSchemaHeader = new ArrayList<String>() {{
            add("NAME");
            add("TYPE");
            add("PRIMARY");
            add("NOT NULL");
            add("MAX LENGTH");
            add("UNIQUE");
        }};
        resultSchemaTable = new ArrayList<>();
        for (Column c : t.getColumns()) {
            List<String> info = new ArrayList<>();
            info.add(c.getName());
            info.add(c.getType().toString());
            info.add(String.valueOf(c.isPrimary()));
            info.add(String.valueOf(c.isNotNull()));
            String maxLength = c.getType() == ColumnType.STRING ?
                    String.valueOf(c.getMaxLength())
                    : "-";
            info.add(maxLength);
            info.add(String.valueOf(c.isUnique()));
            resultSchemaTable.add(info);
        }

        resultType = ResultType.SCHEMA;
    }

    public int tableAttrsNum(String tbName) {
        //获取该表的属性个数
        Table t = database.getTable(tbName);
        return t.getColumns().size();
    }

    public void insertTableRow(String tbName, String[] attrValues) {
        //插入整行
        int attrsNum = tableAttrsNum(tbName);
        if (attrValues.length != attrsNum) {
            throw new WrongInsertArgumentNumException();
        } else {
            Table t = database.getTable(tbName);
            lockData(t, true);

            try {
                if (this.isInTransaction) {
                    logHandler.addInsertRowLog(transactionID, tbName, t.getPrimaryKeyName(), attrValues[t.getPrimaryKeyIndex()]);
                }

                ArrayList<Column> attrs = t.getColumns();
                Entry[] entries = new Entry[attrsNum];
                for (int i = 0; i < attrs.size(); i++) {
                    Column tmpAttr = attrs.get(i);
                    ColumnType attrType = tmpAttr.getType();
                    if (attrType == ColumnType.STRING && attrValues[i].length() > tmpAttr.getMaxLength() + 2) {
                        // 检查value长度
                        // +2: 'xxx' length=3
                        throw new StringValueExceedLengthException(tmpAttr.getName(), tmpAttr.getMaxLength());
                    }
                    Entry e = parseValue(attrType, attrValues[i]);
                    if (tmpAttr.isUnique()) {
                        if (t.contains(e, tmpAttr.isPrimary(), i)) {
                            throw new DuplicateKeyException();
                        }
                    }
                    entries[i] = e;
                }
                t.insert(new Row(entries));
            }finally {
                unlockData(t, true);
            }
        }
    }

    public void insertTableRow(String tbName, String[] attrNames, String[] attrValues) {
        //value的个数要与colomn个数一致
        if (attrNames.length != attrValues.length) {
            throw new WrongInsertArgumentNumException();
        } else {
            String primaryKeyName = getTablePrimaryAttr(tbName);
            int primaryKeyIndex = -1;
            boolean hasPrimaryKey = false;
            for (int i = 0; i < attrNames.length; i++) {
                String attrName = attrNames[i];
                if (attrName.toUpperCase().equals(primaryKeyName.toUpperCase())) {
                    hasPrimaryKey = true;
                    primaryKeyIndex = i;
                }
            }
            if (!hasPrimaryKey) {
                throw new PrimaryKeyRequiredException();
            }

            //插入一条Row
            Table t = database.getTable(tbName);
            lockData(t, true);
            try {
                if (this.isInTransaction) {
                    logHandler.addInsertRowLog(transactionID, tbName, primaryKeyName, attrValues[primaryKeyIndex]);
                }

                ArrayList<Column> attrs = t.getColumns();
                Entry[] entries = new Entry[attrs.size()];
                for (int i = 0; i < attrs.size(); i++) {
                    Column tmpAttr = attrs.get(i);
                    Entry e = null;
                    for (int j = 0; j < attrNames.length; j++) {
                        if (attrNames[j].toUpperCase().equals(tmpAttr.getName().toUpperCase())) {
                            ColumnType attrType = tmpAttr.getType();
                            if (attrType == ColumnType.STRING && attrValues[j].length() > tmpAttr.getMaxLength() + 2) {
                                // 检查value长度
                                // +2: 'xxx' length=3
                                throw new StringValueExceedLengthException(tmpAttr.getName(), tmpAttr.getMaxLength());
                            }
                            e = parseValue(attrType, attrValues[j]);
                            break;
                        }
                    }
                    if (tmpAttr.isNotNull() && e == null) {
                        //TODO 异常处理 不满足 NOTNULL 属性
                        throw new NotNullAttributeAssignedNullException(tmpAttr.getName());
                    }
                    if (tmpAttr.isUnique()) {
                        if (t.contains(e, tmpAttr.isPrimary(), i)) {
                            throw new DuplicateKeyException();
                        }
                    }
                    entries[i] = e;
                }
                t.insert(new Row(entries));
            }finally {
                unlockData(t, true);
            }
        }
    }


    public void delFromTable(String tbName, WhereCondition wherecond) {
        //whereCondition可能为空
        Table t = database.getTable(tbName);
        lockData(t, true);
        try {
            QueryTable q = getQueryTable(tbName, wherecond, null);
            while (q.hasNext()) {
                Row r = q.next();
                if (isInTransaction) {
                    int attrNum = tableAttrsNum(tbName);
                    String[] attrValues = new String[attrNum];
                    for (int i = 0; i < attrNum; i++) {
                        attrValues[i] = r.getEntry(i);
                    }
                    logHandler.addDeleteRowLog(transactionID, tbName, attrNum, attrValues);
                }
                t.delete(r);
            }
        } finally {
            unlockData(t, true);
        }
    }

    public String getTablePrimaryAttr(String tbName) {
        Table t = database.getTable(tbName);
        return t.getPrimaryKeyName();
    }

    public LogHandler getLogHandler() {
        return this.logHandler;
    }

    public void updateTable(String tbName, String colName, String attrValue, WhereCondition wherecond) {
        Table t = database.getTable(tbName);
        lockData(t, true);
        ColumnType cType = null;
        int attrIndex = -1;
        for (int i = 0; i < t.getColumns().size(); i++) {
            Column c = t.getColumns().get(i);
            if (c.getName().toUpperCase().equals(colName.toUpperCase())) {
                cType = c.getType();
                attrIndex = i;
                break;
            }
        }
        if (attrIndex == -1) {
            unlockData(t,true);
            throw new AttrNotExistsException();
        }
        assert cType != null;

        try {
            Entry e = parseValue(cType, attrValue);
            QueryTable q = getQueryTable(tbName, wherecond, null);
            while (q.hasNext()) {
                Row r = q.next();
                if (isInTransaction) {
                    Row oldRow = r;
                    int attrNum = tableAttrsNum(tbName);
                    String[] attrValues = new String[attrNum];
                    for (int i = 0; i < attrNum; i++) {
                        attrValues[i] = r.getEntry(i);
                    }
                    logHandler.addDeleteRowLog(transactionID, tbName, attrNum, attrValues);
                    String primaryKeyName = t.getPrimaryKeyName();
                    if (primaryKeyName.toUpperCase().equals(colName)) {
                        //如果要update的正好是primaryKey
                        logHandler.addInsertRowLog(transactionID, tbName, primaryKeyName, attrValue);
                    } else {
                        logHandler.addInsertRowLog(transactionID, tbName, primaryKeyName, r.getEntry(t.getPrimaryKeyIndex()));
                    }
                }
                t.update(attrIndex, e, r);
            }
        }finally {
            unlockData(t, true);
        }

    }


    public void select(List<Pair<String, String>> results,
                       String table1, String table2,
                       JoinCondition jc,
                       WhereCondition wc,
                       boolean distinct,
                       OrderBy ob) {


        lockData(database.getTable(table1), false);
        if (table2 != null && table2.length() > 0) {
            lockData(database.getTable(table2), false);
        }

        try {
            QueryTable[] q;
            boolean needJoin;
            JoinCondition.JoinType joinType = JoinCondition.JoinType.INNER;
            int jIndex1 = -1, jIndex2 = -1;
            List<Boolean> bothFound = new ArrayList<>();
            if (table2 == null || table2.equals("") || jc == null) {
                // 不需要join
                q = new QueryTable[]{getQueryTable(table1, wc, bothFound)};
                needJoin = false;
                if (bothFound.size() > 0) {
                    throw new AttrNotExistsException();
                }

            } else {
                // 检查where中是否存在歧义
                if (!checkAmbiguousColumnForWhere(table1, table2, wc)) {
                    throw new AmbiguousColumnException();
                }
                q = new QueryTable[]{getQueryTable(table1, wc, bothFound), getQueryTable(table2, wc, bothFound)};

                if (bothFound.size() > 1) {
                    throw new AttrNotExistsException();
                }
                // 哪两列被join
                List<Integer> joinIndex = getJoinIndex(table1, table2, jc);

                jIndex1 = joinIndex.get(0);
                jIndex2 = joinIndex.get(1);
                joinType = jc.type;
                needJoin = true;
            }

            // select哪些列

            List<Integer> index = getSelectIndex(results, table1, table2);
            // Order by
            int orderByIndex;
            boolean desc;
            if (ob != null) {
                orderByIndex = getOrderByIndex(table1, table2, ob, index);
                desc = ob.desc;
            } else {
                orderByIndex = -1;
                desc = false;
            }
            List<Row> qResult = new QueryResult(q, index, needJoin, jIndex1, jIndex2, joinType, orderByIndex, desc, distinct).query();
            generateTmpTable(qResult, table1, table2, index);
            resultType = ResultType.TABLE;
        }finally {
            unlockData(database.getTable(table1), false);
            if (table2 != null && table2.length() > 0) {
                unlockData(database.getTable(table2), false);
            }
        }
    }

    private QueryTable getQueryTable(String table, WhereCondition wc, List<Boolean> bothFound) {
        QueryTable q;
        Table t = database.getTable(table);
        if (wc != null && (wc.tableName.equals(table) || wc.tableName.equals(""))) {
            AttrCompare compare = new AttrCompare(wc.op);
            String attr = wc.attr;
            ColumnType cType = null;
            boolean found = false;
            for (Column c : t.getColumns()) {
                if (c.getName().toUpperCase().equals(attr.toUpperCase())) {
                    cType = c.getType();
                    found = true;
                    break;
                }
            }
            if (!found) {
                if (wc.tableName.equals(table)) {
                    // TODO throw error: attr not in columns
                    throw new AttrNotExistsException();
                } else if (bothFound != null) {
                    bothFound.add(false);
                }
            }
            if (found) {
                String value = wc.val;
                Entry instant = parseValue(cType, value);
                q = new QueryTable(compare, t, attr, instant);
            } else {
                q = new QueryTable(null, t, null, null);
            }
        } else {
            q = new QueryTable(null, t, null, null);
        }
        return q;
    }

    private Entry parseValue(ColumnType type, String value) {
        Entry instant;
        try {
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
                    instant = new Entry(value.substring(1, value.length() - 1));
                    break;
                default:
                    instant = new Entry(value);
                    break;
            }
        } catch (NumberFormatException e) {
            throw new ColumnTypeWrongException(value, type);
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
                    throw new AmbiguousColumnException();
                } else if (first < 0) {
                    // TODO throw error: column not exists
                    throw new AttrNotExistsException();
                } else {
                    index = first;
                }
            } else {
                if (tbName.toUpperCase().equals(t1.toUpperCase())) {
                    int tmp = attrs.indexOf(attrName);
                    if (tmp >= midIndex) {
                        // TODO throw error: attrName not exists in table
                        throw new AttrNotExistsException();
                    } else {
                        index = tmp;
                    }
                } else if (tbName.toUpperCase().equals(t2.toUpperCase())) {
                    int tmp = attrs.lastIndexOf(attrName);
                    if (tmp < midIndex) {
                        // TODO throw error: attrName not exists in table
                        throw new AttrNotExistsException();
                    } else {
                        index = tmp;
                    }
                } else {
                    // TODO throw error: table not exists
                    throw new TableNotExistsException();
                }
            }
            results.add(index);
        }
        return results;
    }

    private List<Integer> getJoinIndex(String t1, String t2, JoinCondition jc) {
        List<Integer> results = new ArrayList<>();
        ArrayList<Column> c1 = database.getTable(t1).getColumns();
        ArrayList<Column> c2 = database.getTable(t2).getColumns();
        int index;
        if (jc.table1.equals(t1) && jc.table2.equals(t2)) {
            index = findAttrIndexInColumns(c1, jc.attr1);
            results.add(index);
            index = findAttrIndexInColumns(c2, jc.attr2);
            results.add(index);
        } else if (jc.table1.toUpperCase().equals(t2.toUpperCase()) && jc.table2.toUpperCase().equals(t1.toUpperCase())) {
            index = findAttrIndexInColumns(c1, jc.attr2);
            results.add(index);
            index = findAttrIndexInColumns(c2, jc.attr1);
            results.add(index);
        } else {
            throw new TableNotExistsException();
        }
        return results;
    }

    private int getOrderByIndex(String t1, String t2, OrderBy ob, List<Integer> selectIndex) {
        List<String> attrs = new ArrayList<>();
        int midIndex = mergeAttrs(t1, t2, attrs);
        if (ob.tableName.equals("")) {
            for (int i = 0; i < selectIndex.size(); ++i) {
                if (attrs.get(selectIndex.get(i)).equals(ob.attr)) {
                    return i;
                }
            }
            throw new AttrNotExistsException();
        } else if (ob.tableName.equals(t1)) {
            for (int i = 0; i < selectIndex.size(); ++i) {
                int index = selectIndex.get(i);
                if (index >= midIndex) {
                    break;
                }
                if (attrs.get(index).equals(ob.attr)) {
                    return i;
                }
            }
            throw new AttrNotExistsException();
        } else if (ob.tableName.equals(t2)) {
            for (int i = 0; i < selectIndex.size(); ++i) {
                int index = selectIndex.get(i);
                if (index < midIndex) {
                    continue;
                }
                if (attrs.get(index).equals(ob.attr)) {
                    return i;
                }
            }
            throw new AttrNotExistsException();
        } else {
            throw new TableNotExistsException();
        }
    }

    private boolean checkAmbiguousColumnForWhere(String t1, String t2, WhereCondition wc) {
        if (wc == null) {
            return true;
        }
        if (wc.tableName.toUpperCase().equals(t1.toUpperCase()) || wc.tableName.toUpperCase().equals(t2.toUpperCase())) {
            return true;
        }
        if (wc.tableName.equals("")) {
            Table table1 = database.getTable(t1);
            Table table2 = database.getTable(t2);
            boolean found = false;
            for (Column c : table1.getColumns()) {
                if (c.getName().toUpperCase().equals(wc.attr.toUpperCase())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return true;
            }
            for (Column c : table2.getColumns()) {
                if (c.getName().toUpperCase().equals(wc.attr.toUpperCase())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private void generateTmpTable(List<Row> rows, String t1, String t2, List<Integer> index) {
        List<String> attrs = new ArrayList<>();
        int midIndex = mergeAttrs(t1, t2, attrs);
        Table table1, table2 = null;
        List<Column> c1 = new ArrayList<>();
        List<Column> c2 = new ArrayList<>();
        table1 = database.getTable(t1);
        c1 = table1.getColumns();
        if (t2 != null && !t2.equals("")) {
            table2 = database.getTable(t2);
            c2 = table2.getColumns();
        }

        resultHeader = new ArrayList<>();
        for (int i : index) {
            if (i < midIndex) {
                resultHeader.add(new Column(c1.get(i)));
            } else {
                resultHeader.add(new Column(c2.get(i - midIndex)));
            }
        }

        resultTable = rows;
    }

    private int mergeAttrs(String t1, String t2, List<String> mergedAttrs) {
        Table table1, table2;
        int midIndex = 0;
        if (t1 != null && !t1.equals("")) {
            table1 = database.getTable(t1);
            midIndex = tableAttrsNum(t1);
            for (Column c : table1.getColumns()) {
                mergedAttrs.add(c.getName());
            }
        }
        if (t2 != null && !t2.equals("")) {
            table2 = database.getTable(t2);
            for (Column c : table2.getColumns()) {
                mergedAttrs.add(c.getName());
            }
        }
        return midIndex;
    }

    void setInTransaction(boolean value) {
        this.isInTransaction = value;
    }

    boolean getInTransaction() {
        return this.isInTransaction;
    }

    void terminateTransaction() {
        for (Table t : this.exclusiveLockedTables) {
            t.getLock().writeLock().unlock();
        }
        this.exclusiveLockedTables.clear();
        this.isInTransaction = false;
    }


    public boolean getResult(List<String> columnList, List<List<String>> rowList) {
        columnList.clear();
        rowList.clear();
        if (resultTable == null && resultSchemaTable == null) {
            return false;
        }
        if (resultType == ResultType.TABLE) {
            for (Column c : resultHeader) {
                columnList.add(c.getName());
            }
            for (Row row : resultTable) {
                List<String> rr = new ArrayList<>();
                for (Entry e : row.getEntries()) {
                    if (e == null) {
                        rr.add("null");
                    } else {
                        rr.add(e.toString());
                    }
                }
                rowList.add(rr);
            }
            resultTable = null;
            resultHeader = null;
        } else {
            columnList.addAll(resultSchemaHeader);
            rowList.addAll(resultSchemaTable);
            resultSchemaTable = null;
            resultSchemaHeader = null;
        }
        return true;
    }


    private int findAttrIndexInColumns(List<Column> columns, String attr) {
        for (int i = 0; i < columns.size(); ++i) {
            if (columns.get(i).getName().toUpperCase().equals(attr.toUpperCase())) {
                return i;
            }
        }
        throw new AttrNotExistsException();
    }

    private void lockData(Table t, boolean isWrite) {
        if (isWrite) {
            t.getLock().writeLock().lock();
        } else {
            t.getLock().readLock().lock();
        }
        if (isInTransaction && isWrite) {
            this.exclusiveLockedTables.add(t);
        }
    }

    private void unlockData(Table t, boolean isWrite) {
        if (!isInTransaction) {
            if (isWrite) {
                t.getLock().writeLock().unlock();
            } else {
                t.getLock().readLock().unlock();
            }
        } else {
            if (!isWrite) {
                t.getLock().readLock().unlock();
            }
        }
    }


}
