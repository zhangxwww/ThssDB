package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javafx.scene.control.Cell;

public class QueryResult {

    private List<MetaInfo> metaInfoInfos;
    private List<Integer> index;
    private List<Cell> attrs;

    public QueryResult(QueryTable[] queryTables) {
        // TODO
        this.index = new ArrayList<>();
        this.attrs = new ArrayList<>();
    }

    public static Row combineRow(LinkedList<Row> rows) {
        // TODO
        ArrayList<Entry> entries = new ArrayList<>();
        for (Row r : rows) {
            entries.addAll(r.getEntries());
        }
        return new Row((Entry[]) entries.toArray());
    }

    public Row generateQueryRecord(Row row) {
        // TODO
        return null;
    }
}