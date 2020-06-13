package cn.edu.thssdb.schema;

import java.io.*;
import java.util.ArrayList;

public class TableInfo implements Serializable {
    public String tableName;
    public ArrayList<Column> columns;
    public int primaryIndex;
    public ArrayList<Integer> framePagesId;

    public TableInfo(String tbName, ArrayList<Column> cols, int primaryId, ArrayList<Integer> framePagesID) {
        tableName = tbName;
        columns = cols;
        primaryIndex = primaryId;
        framePagesId = framePagesID;
    }
}
