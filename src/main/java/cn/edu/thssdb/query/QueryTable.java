package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import java.util.Iterator;

public class QueryTable implements Iterator<Row> {
  AttrCompare attrCompare;
  QueryTable(AttrCompare attrCompare) {
    // TODO
    this.attrCompare = attrCompare;
  }

  @Override
  public boolean hasNext() {
    // TODO
    return true;
  }

  @Override
  public Row next() {
    // TODO
    return null;
  }
}