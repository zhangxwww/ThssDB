package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;
import com.sun.javafx.logging.PulseLogger;

import java.io.Serializable;
import java.security.PublicKey;

public class Column implements Comparable<Column>, Serializable {
  private String name;
  private ColumnType type;
  private int primary;
  private boolean notNull;
  private int maxLength;

  public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    this.notNull = notNull;
    this.maxLength = maxLength;
  }

  public boolean isPrimary() {
    return primary != 0;
  }

  public String getName() {
    return name;
  }

  public ColumnType getType() {
    return type;
  }

  @Override
  public int compareTo(Column e) {
    return name.compareTo(e.name);
  }

  public String toString() {
    return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
  }
}
