package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;

import java.lang.reflect.Array;
import java.util.*;

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
    private JoinCondition.JoinType joinType;
    private QueryTable[] queryTables;

    private int orderByIndex;
    private boolean desc;
    private boolean distinct;

    public QueryResult(QueryTable[] queryTables, List<Integer> index, boolean needJoin, int joinIndex1, int joinIndex2) {
        // TODO
        this.index = index;
        this.needJoin = needJoin;
        this.joinIndex1 = joinIndex1;
        this.joinIndex2 = joinIndex2;
        this.attrs = new ArrayList<>();
        this.queryTables = queryTables;
        this.joinType = JoinCondition.JoinType.INNER;
        this.orderByIndex = -1;
        this.desc = false;
        this.distinct = false;
    }

    public QueryResult(QueryTable[] queryTables,
                       List<Integer> index,
                       boolean needJoin,
                       int joinIndex1, int joinIndex2,
                       JoinCondition.JoinType type) {
        this(queryTables, index, needJoin, joinIndex1, joinIndex2);
        this.joinType = type;
    }

    public QueryResult(QueryTable[] queryTables,
                       List<Integer> index,
                       boolean needJoin,
                       int joinIndex1, int joinIndex2,
                       JoinCondition.JoinType type,
                       int orderByIndex,
                       boolean desc,
                       boolean distinct) {
        this(queryTables, index, needJoin, joinIndex1, joinIndex2,type);
        this.orderByIndex = orderByIndex;
        this.desc = desc;
        this.distinct = distinct;
    }

    public List<Row> query() {
        List<Row> result;
        if (needJoin) {
            List<List<Row>> rowLists = new ArrayList<>();
            rowLists.add(new ArrayList<>());
            rowLists.add(new ArrayList<>());
            for (int i = 0; i < 2; ++i) {
                while (queryTables[i].hasNext()) {
                    rowLists.get(i).add(queryTables[i].next());
                }
            }
            switch (joinType) {
                case INNER:
                    result = join(rowLists.get(0), rowLists.get(1));
                    break;
                case LEFT_OUTER:
                    result = leftOuterJoin(rowLists.get(0), rowLists.get(1), queryTables[1].getWidth());
                    break;
                case RIGHT_OUTER:
                    result = rightOuterJoin(rowLists.get(0), rowLists.get(1), queryTables[0].getWidth());
                    break;
                default:
                    result = new ArrayList<>();
                    break;
            }
        } else {
            result = new ArrayList<>();
            while (queryTables[0].hasNext()) {
                result.add(queryTables[0].next());
            }
        }
        List<Row> finalRes = new ArrayList<>();
        for (Row r : result) {
            finalRes.add(generateQueryRecord(r));
        }
        if (distinct) {
            distinctRow(finalRes);
        }
        if (orderByIndex >= 0) {
            sort(finalRes);
        }
        return finalRes;
    }

    public List<Row> join(List<Row> r1, List<Row> r2) {
        List<Row> joinedRows = new ArrayList<>();
        for (Row r : r1) {
            Entry e1 = r.getEntries().get(joinIndex1);
            if (e1 == null) {
                continue;
            }
            for (Row rr : r2) {
                Entry e2 = rr.getEntries().get(joinIndex2);
                if (e2 == null){
                    continue;
                }
                if (e1.equals(e2)) {
                    LinkedList<Row> tmp = new LinkedList<Row>() {{
                        add(r);
                        add(rr);
                    }};
                    joinedRows.add(combineRow(tmp));
                }
            }
        }
        return joinedRows;
    }

    public List<Row> leftOuterJoin(List<Row> r1, List<Row> r2, int r2Width) {
        List<Row> joinedRows = new ArrayList<>();
        for (Row r : r1) {
            boolean found = false;
            Entry e1 = r.getEntries().get(joinIndex1);
            if (e1 == null) {
                continue;
            }
            for (Row rr : r2) {
                Entry e2 = rr.getEntries().get(joinIndex2);
                if (e2 == null) {
                    continue;
                }
                if (e1.equals(e2)) {
                    LinkedList<Row> tmp = new LinkedList<Row>() {{
                        add(r);
                        add(rr);
                    }};
                    joinedRows.add(combineRow(tmp));
                    found = true;
                }
            }
            if (!found) {
                LinkedList<Row> tmp = new LinkedList<Row>() {{
                    add(r);
                    add(new Row(new Entry[r2Width]));
                }};
                joinedRows.add(combineRow(tmp));
            }
        }
        return joinedRows;
    }

    public List<Row> rightOuterJoin(List<Row> r1, List<Row> r2, int r1Width) {
        List<Row> joinedRows = new ArrayList<>();
        for (Row rr : r2) {
            boolean found = false;
            Entry e2 = rr.getEntries().get(joinIndex2);
            if (e2 == null) {
                continue;
            }
            for (Row r : r1) {
                Entry e1 = r.getEntries().get(joinIndex1);
                if (e1 == null) {
                    continue;
                }
                if (e2.equals(e1)) {
                    LinkedList<Row> tmp = new LinkedList<Row>() {{
                        add(r);
                        add(rr);
                    }};
                    joinedRows.add(combineRow(tmp));
                    found = true;
                }
            }
            if (!found) {
                LinkedList<Row> tmp = new LinkedList<Row>() {{
                    add(new Row(new Entry[r1Width]));
                    add(rr);
                }};
                joinedRows.add(combineRow(tmp));
            }
        }
        return joinedRows;
    }

    private void sort(List<Row> rows) {
        rows.sort(new Row.RowComparator(orderByIndex, desc));
    }

    private void distinctRow(List<Row> rows) {
        Set<Row> set = new HashSet<>(rows);
        rows.clear();
        rows.addAll(set);
    }

    public static Row combineRow(LinkedList<Row> rows) {
        // TODO
        ArrayList<Entry> entries = new ArrayList<>();
        for (Row r : rows) {
            entries.addAll(r.getEntries());
        }
        int nEntries = entries.size();
        Entry[] es = new Entry[nEntries];
        for (int i = 0; i < nEntries; ++i) {
            es[i] = entries.get(i);
        }
        return new Row(es);
    }

    public Row generateQueryRecord(Row row) {
        Entry[] entries = new Entry[index.size()];
        for (int i = 0; i < index.size(); ++i) {
            entries[i] = row.getEntries().get(index.get(i));
        }
        return new Row(entries);
    }
}