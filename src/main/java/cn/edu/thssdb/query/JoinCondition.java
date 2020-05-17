package cn.edu.thssdb.query;

public class JoinCondition extends Condition {
    public String table1;
    public String table2;
    public String attr1;
    public String attr2;

    public JoinCondition(String op, String t1, String t2, String a1, String a2) {
        super(op);
        this.table1 = t1;
        this.table2 = t2;
        this.attr1 = a1;
        this.attr2 = a2;
    }
}
