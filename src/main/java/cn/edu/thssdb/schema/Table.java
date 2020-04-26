package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.query.QueryTable;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  private int primaryIndex;

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

  private void recover() {
    // TODO
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

  private void serialize() {
    // TODO
    Iterator<Row> iter = iterator();

  }

  private ArrayList<Row> deserialize() {
    // TODO
    return null;
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
}
