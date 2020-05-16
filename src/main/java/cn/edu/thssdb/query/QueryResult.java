package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import cn.edu.thssdb.schema.Table;
import javafx.scene.control.Cell;

public class QueryResult {

    private List<MetaInfo> metaInfoInfos;
    // 查询哪些列，如果有多张表，提前计算join后的index
    private List<Integer> index;
    private List<Cell> attrs;
    private boolean needJoin = false;
    // join on 哪些列
    private int joinIndex1, joinIndex2;

    public QueryResult(QueryTable[] queryTables, List<Integer> index, boolean needJoin, int joinIndex1, int joinIndex2) {
        // TODO
        this.index = index;
        this.needJoin = needJoin;
        this.joinIndex1 = joinIndex1;
        this.joinIndex2 = joinIndex2;
        this.attrs = new ArrayList<>();
    }

    public List<Row> join(List<Row> r1, List<Row> r2) {
        int l1 = r1.size();
        int l2 = r2.size();
        List<Row> joinedRows = new ArrayList<>();
        for (int i = 0; i < l1; ++i) {
            for (int j = 0; j < l2; ++j) {
                Row row1 = r1.get(i);
                Row row2 = r2.get(j);
                if (row1.getEntries().get(joinIndex1).equals(row2.getEntries().get(joinIndex2))) {
                    LinkedList<Row> tmp = new LinkedList<Row>() {{
                        add(row1);
                        add(row2);
                    }};
                    joinedRows.add(combineRow(tmp));
                }
            }
        }
        return joinedRows;
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
        Entry[] entries = new Entry[index.size()];
        for (int i = 0; i < index.size(); ++i) {
            entries[i] = row.getEntries().get(index.get(i));
        }
        return new Row(entries);
    }
}