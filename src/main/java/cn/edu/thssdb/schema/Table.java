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
    private boolean isInTransaction = false;

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

    public void recover(PageFilePersist persistManager) {
        // TODO
        //从磁盘中反序列化得到纪录
        ArrayList<Row> rows = deserialize(persistManager);
        for (Row row : rows) {
            Entry e = row.getEntries().get(primaryIndex);
            index.put(e, row);
        }
    }

    public void recover() {
        // TODO
    }

    public void insert(Row row) {
        // TODO
        if (this.isInTransaction) {
            this.lock.writeLock().lock();
        }
        Entry e = row.getEntries().get(primaryIndex);
        index.put(e, row);
    }

    public void delete(Row row) {
        // TODO
        if (this.isInTransaction) {
            this.lock.writeLock().lock();
        }
        Entry e = row.getEntries().get(primaryIndex);
        index.remove(e);
    }

    public void delete(Iterator<Row> iterator) {
        if (this.isInTransaction) {
            this.lock.writeLock().lock();
        }
        while (iterator.hasNext()) {
            Row r = iterator.next();
            delete(r);
        }
    }

    public void update(int attrIndex, Entry attrValue, Row row) {
        // TODO
        if (this.isInTransaction) {
            this.lock.writeLock().lock();
        }
        Entry e = row.getEntries().get(primaryIndex);
        row.getEntries().set(attrIndex, attrValue);
        index.update(e, row);
    }

    public void update(int attrIndex, Entry attrValue, Iterator<Row> rows) {
        // TODO
        if (this.isInTransaction) {
            this.lock.writeLock().lock();
        }
        for (; rows.hasNext(); ) {
            Row r = rows.next();
            update(attrIndex, attrValue, r);
        }
    }


    //   When this function is called, those dirty pages that related to this table
//   will be flushed to corresponding position on disks.
    public void persist(PageFilePersist persistManager) {
        // TODO
        this.lock.writeLock().lock();
        try {
            persistManager.flushTable(tableName);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    ////   Take the rows to the buffer pool
    public void serialize(PageFilePersist persistManager) {
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
            persistManager.storeTable(tableName, bData);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private ArrayList<Row> deserialize(PageFilePersist persistManager) {
        // TODO
        ArrayList<Row> rows = new ArrayList<Row>();
        byte[] bData = persistManager.retrieveTable(tableName);
        index = new BPlusTree<Entry, Row>();
        try {
            ByteArrayInputStream bInStream = new ByteArrayInputStream(bData);
            ObjectInputStream ois = new ObjectInputStream(bInStream);
            while (true) {
                Row o = (Row) ois.readObject();
                rows.add(o);
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
        if (this.isInTransaction) {
            this.lock.readLock().lock();
        }
        return new TableIterator(this);
    }

    public boolean isInTransaction() {
        return isInTransaction;
    }

    public void setInTransaction(boolean value) {
        this.isInTransaction = value;
    }

    public ReentrantReadWriteLock getLock() {
        return this.lock;
    }
}
