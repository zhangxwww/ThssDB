package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import org.w3c.dom.Attr;

import java.util.Iterator;
import java.util.List;

public class QueryTable implements Iterator<Row> {
    private AttrCompare attrCompare;
    private Iterator<Row> iterator;
    private boolean isAttrInstCmp;
    private boolean needWhere;
    private int index1 = -1;
    private int index2 = -1;
    private Entry instant;
    private Row nextRow;

    QueryTable(AttrCompare attrCompare, Table table, String attr1, String attr2) {
        // TODO
        this.attrCompare = attrCompare;
        this.iterator = table.iterator();
        List<Column> columns = table.getColumns();
        int numColumns = columns.size();
        for (int i = 0; i < numColumns; i++) {
            Column c = columns.get(i);
            if (c.getName().toUpperCase().equals(attr1.toUpperCase())) {
                this.index1 = i;
            } else if (c.getName().toUpperCase().equals(attr2.toUpperCase())) {
                this.index2 = i;
            }
        }
        this.isAttrInstCmp = false;
        this.needWhere = true;
    }

    public QueryTable(AttrCompare attrCompare, Table table, String attr, Entry instant) {
        needWhere = attrCompare != null;
        if (needWhere) {
            this.attrCompare = attrCompare;
            List<Column> columns = table.getColumns();
            int numColumns = columns.size();
            for (int i = 0; i < numColumns; i++) {
                Column c = columns.get(i);
                if (c.getName().toUpperCase().equals(attr.toUpperCase())) {
                    this.index1 = i;
                }
            }
            this.instant = instant;
        }
        this.isAttrInstCmp = true;
    }

    @Override
    public boolean hasNext() {
        while (this.iterator.hasNext()) {
            Row next = this.iterator.next();
            if (!needWhere) {
                this.nextRow = next;
                return true;
            }
            Entry entry1 = next.getEntries().get(this.index1);
            Entry entry2 = instant;
            if (!this.isAttrInstCmp) {
                entry2 = next.getEntries().get(this.index2);
            }
            if (this.attrCompare.eval(entry1, entry2)) {
                this.nextRow = next;
                return true;
            }
        }
        return false;
    }

    @Override
    public Row next() {
        Row result = this.nextRow;
        this.nextRow = null;
        return result;
    }
}