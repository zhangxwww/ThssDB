package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.*;

public class Row implements Serializable {
    private static final long serialVersionUID = -5809782578272943999L;
    protected ArrayList<Entry> entries;

    public Row() {
        this.entries = new ArrayList<>();
    }

    public Row(Entry[] entries) {
        this.entries = new ArrayList<>(Arrays.asList(entries));
    }

    public ArrayList<Entry> getEntries() {
        return entries;
    }

    public void appendEntries(ArrayList<Entry> entries) {
        this.entries.addAll(entries);
    }

    public String getEntry(int index) {
        return this.entries.get(index).toString();
    }

    public String toString() {
        if (entries == null)
            return "EMPTY";
        StringJoiner sj = new StringJoiner(", ");
        for (Entry e : entries)
            sj.add(e.toString());
        return sj.toString();
    }

    @Override
    public int hashCode() {
        int code = 0;
        for (Entry e : entries) {
            code += e.value.hashCode();
        }
        return code;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        Row r = (Row) obj;
        List<Entry> entries1 = r.getEntries();
        if (entries.size() != entries1.size()) {
            return false;
        }
        int nEntries = entries.size();
        for (int i = 0; i < nEntries; ++i) {
            if (!entries.get(i).equals(entries1.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static class RowComparator implements Comparator<Row> {

        private int compareIndex;
        private boolean desc;

        public RowComparator(int compareIndex, boolean desc) {
            this.compareIndex = compareIndex;
            this.desc = desc;
        }

        @Override
        public int compare(Row r1, Row r2) {
            Row left = r1;
            Row right = r2;
            if (desc) {
                left = r2;
                right = r1;
            }
            Entry e1 = left.getEntries().get(compareIndex);
            Entry e2 = right.getEntries().get(compareIndex);
            return e1.compareTo(e2);
        }
    }
}
