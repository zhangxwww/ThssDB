package cn.edu.thssdb.service;

import cn.edu.thssdb.query.JoinCondition;
import cn.edu.thssdb.query.WhereCondition;
import cn.edu.thssdb.schema.*;
import com.sun.corba.se.impl.orb.DataCollectorBase;
import javafx.scene.control.Tab;
import javafx.util.Pair;

import javax.xml.crypto.Data;
import java.util.List;
import java.util.Map;

public class StatementAdapter {

    private Manager m;
    private Database dbTEST;

    public StatementAdapter() {
        m = new Manager();
        m.createDatabaseIfNotExists("TEST");
        dbTEST = m.switchDatabase("TEST")
    }

    public void createTable(String tbName, Column[] cols) {
        dbTEST.createTable(tbName, cols);
    }

    public void dropTable(String tbName) {
        dbTEST.dropTable(tbName);
    }

    public int tableAttrsNum(String tbName) {
        //获取该表的属性个数
        Table t = dbTEST.getTable(tbName);
        return t.getColumns().size();
    }

    public void insertTableRow(String tbName, Row r) {
        Table t = dbTEST.getTable(tbName);
        t.insert(r);
    }

    public void insertTableRow(String tbName, String[] attrNames, Entry[] entries) {
        Table t = dbTEST.getTable(tbName);
        return;
    }

    public String getTablePrimaryAttr(String tbName) {
        return "ID";
    }

    public Table select(List<Pair<String, String>> results, String table1, String table2, JoinCondition jc, WhereCondition wc) {
        // TODO

    }

}
