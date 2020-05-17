package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Entry;

public class WhereCondition extends Condition {
    public String tableName;
    public String attr;
    public Entry val;

    public WhereCondition(String op, String t, String a, Entry v) {
        super(op);
        this.tableName = t;
        this.attr = a;
        this.val = v;
    }
}
