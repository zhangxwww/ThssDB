package cn.edu.thssdb.query;

public class OrderBy {
    public String tableName;
    public String attr;
    public boolean desc;

    public OrderBy(String table, String attr, boolean desc) {
        this.tableName = table;
        this.attr = attr;
        this.desc = desc;
    }
}
