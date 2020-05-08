package cn.edu.thssdb.test;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;

import java.io.IOException;

public class metadataTest {
    public static void main(String[] args) throws IOException {
        Manager m = new Manager();
        m.createDatabaseIfNotExists("THSS");
        m.createDatabaseIfNotExists("PEKU");

        Database dbTHSS = m.switchDatabase("THSS");

        int nCol = 3;
        Column[] cols = new Column[nCol];
        cols[0] = new Column("ID", ColumnType.INT,1,true,10);
        cols[1] = new Column("name",ColumnType.STRING,0,true,8);
        cols[2] = new Column("sig",ColumnType.STRING,0,true,8);

        Table tbStu = dbTHSS.createTable("student",cols);

        cols[0] = new Column("ID2", ColumnType.INT,1,true,10);
        cols[1] = new Column("name2",ColumnType.STRING,0,true,8);
        cols[2] = new Column("sig2",ColumnType.STRING,0,true,8);

        Table tbStu2 = dbTHSS.createTable("student2",cols);

        Database dbPEKU = m.switchDatabase("PEKU"); //在这一步中，THSS的student表存好了

        dbTHSS = m.switchDatabase("THSS");

        dbTHSS.quit();

    }
}
