package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.persist.PageFilePersist;
import cn.edu.thssdb.query.QueryTable;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
    ReentrantReadWriteLock lock;
    private String databaseName;
    public String tableName;
    private ArrayList<Column> columns;
    public BPlusTree<Entry, Row> index;
    public int primaryIndex;

    public Table(String databaseName, String tableName, Column[] columns) {
        // TODO
        this.lock = new ReentrantReadWriteLock();
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<>(Arrays.asList(columns));
        this.index = new BPlusTree<>();
        for (int i = 0; i < columns.length; ++i) {
            if (columns[i].isPrimary()) {
                this.primaryIndex = i;
                break;
            }
        }

    }

    public Table(String databaseName, String tableName, Column[] columns, String primaryName) {
        // TODO
        this.lock = new ReentrantReadWriteLock();
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<>(Arrays.asList(columns));
        this.index = new BPlusTree<>();
        for (int i = 0; i < columns.length; ++i) {
            if (columns[i].getName().equals(primaryName)) {
                this.primaryIndex = i;
                break;
            }
        }
    }

    public Table(String databaseName, String tableName, Column[] columns, int primaryIndex) {
        // TODO
        this.lock = new ReentrantReadWriteLock();
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<>(Arrays.asList(columns));
        this.index = new BPlusTree<>();
        this.primaryIndex = primaryIndex;
    }

    public void recover(byte[] bData) {
        // TODO
        //从磁盘中反序列化得到纪录
        ArrayList<Row> rows = deserialize(bData);
        for (Row row : rows) {
            Entry e = row.getEntries().get(primaryIndex);
            index.put(e, row);
        }
    }

    public void insert(Row row) {
        // TODO
        Entry e = row.getEntries().get(primaryIndex);
        index.put(e, row);
    }

    public void delete(Row row) {
        // TODO
        Entry e = row.getEntries().get(primaryIndex);
        index.remove(e);
    }

    public void delete(Iterator<Row> iterator) {
        while (iterator.hasNext()) {
            Row r = iterator.next();
            delete(r);
        }
    }

    public void update(int attrIndex, Entry attrValue, Row row) {
        // TODO
        Entry e = row.getEntries().get(primaryIndex);
        row.getEntries().set(attrIndex, attrValue);
        index.update(e, row);
    }

    public void update(int attrIndex, Entry attrValue, Iterator<Row> rows) {
        // TODO
        for (; rows.hasNext(); ) {
            Row r = rows.next();
            update(attrIndex, attrValue, r);
        }
    }

    ////   Make the rows to bytes
    public byte[] serialize() {
        // TODO
        try {
            ByteArrayOutputStream bOutStream = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bOutStream);
            Iterator<Row> iter = iterator();
            while (iter.hasNext()) {
                Row r = iter.next();
                oos.writeObject(r);
            }
            byte[] bData = bOutStream.toByteArray();
            return bData;
//            persistManager.storeTable(tableName, bData);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private ArrayList<Row> deserialize(byte[] bData) {
        // TODO
        ArrayList<Row> rows = new ArrayList<Row>();
        index = new BPlusTree<Entry, Row>();
        try {
            ByteArrayInputStream bInStream = new ByteArrayInputStream(bData);
            ObjectInputStream ois = new ObjectInputStream(bInStream);
            while (true) {
                try {
                    Row o = (Row) ois.readObject();
                    rows.add(o);
                } catch (StreamCorruptedException ignored) {
                    break;
                }
            }
        } catch (EOFException e) {
            System.out.println("table deserialize中类对象已完全读入");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rows;
    }

    public void printBPlusTree() {
        Iterator<Row> iter = iterator();
        while (iter.hasNext()) {
            Row r = iter.next();
            System.out.println(r.toString());
        }
    }

    public ArrayList<Column> getColumns() {
        return this.columns;
    }

    public boolean contains(Entry entry, boolean isPrimary, int columnIndex) {
        if (isPrimary) {
            return index.contains(entry);
        } else {
            for (Row row : this) {
                if (row.getEntries().get(columnIndex).equals(entry)) {
                    return true;
                }
            }
            return false;
        }
    }

    public String getPrimaryKeyName() {
        return this.columns.get(primaryIndex).getName();
    }

    public int getPrimaryKeyIndex() {
        return this.primaryIndex;
    }

    public int getAttributeIndex(String attrName) {
        int index = -1;
        for (int i = 0; i < this.columns.size(); i++) {
            if (this.columns.get(i).getName().toUpperCase().equals(attrName.toUpperCase())) {
                index = i;
            }
        }
        return index;
    }

    private class TableIterator implements Iterator<Row> {
        private Iterator<Pair<Entry, Row>> iterator;

        TableIterator(Table table) {
            this.iterator = table.index.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Row next() {
            return iterator.next().getValue();
        }
    }

    @Override
    public Iterator<Row> iterator() {
        return new TableIterator(this);
    }

    public ReentrantReadWriteLock getLock() {
        return this.lock;
    }
}
