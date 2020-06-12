package cn.edu.thssdb.service;

import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransactionTest {
    Manager manager = null;
    StatementExecuter executer1 = null;
    StatementExecuter executer2 = null;

    @Before
    public void setUp() {
        manager = new Manager();
        executer1 = new StatementExecuter(manager,223);
        executer2 = new StatementExecuter(manager,110);
        String statement1 = "create database UNITTEST";
        executer1.execute(statement1);
        String statement2 = "use UNITTEST";
        executer1.execute(statement2);
        executer2.execute(statement2);
        String statement3 = "create table student(ID  int, name String(24), PRIMARY Key(id));";
        executer1.execute(statement3);
        executer1.execute("INSERT INTO student VALUES (010136, 'myq');");
        executer1.execute("INSERT INTO student VALUES (233333, 'zxw');");
        executer1.execute("INSERT INTO Student Values (10086,  'wyb');");
    }

    @Test
    public void testWriteLock() {
        List<String> columnList = new ArrayList<>();
        List<List<String>> rowList = new ArrayList<>();
        executer1.execute("SELECT  id, name  FROM  student");
        assertTrue(executer1.getResult(columnList, rowList));
        assertEquals(rowList.size(), 3);

        List<String> testStatements = new ArrayList<String>() {{
            add("begin transaction");
            add("INSERT INTO student VALUES (052424, 'mym');");
            add("UPDATE student SET name = 'myq' where id = 010136;");
            add("SELECT  id, name  FROM  student;");
        }};
        executer2.batchExecute(testStatements);


        executer1.execute("begin transaction");
        executer1.execute("SELECT  id, name  FROM  student");
        assertTrue(executer1.getResult(columnList, rowList));
        // TODO 应该是卡住的
        // assertEquals(4,rowList.size());

        executer2.execute("Commit");
        Database database = executer1.getDatabase();
        database.recoverUncommittedCmd(110);
        executer2.execute("SELECT  id, name  FROM  student");
        assertTrue(executer2.getResult(columnList, rowList));
        assertEquals(4, rowList.size());
    }

    @Test
    public void testRecover() {
        List<String> columnList = new ArrayList<>();
        List<List<String>> rowList = new ArrayList<>();



        executer1.execute("begin transaction");
        executer1.execute("UPDATE student SET name = 'mym66' where id = 052424;");


        List<String> testStatements = new ArrayList<String>() {{
            add("begin transaction");
            add("INSERT INTO student VALUES (052424, 'mym');");
            add("UPDATE student SET name = 'myq66' where id = 010136;");
        }};
        executer2.batchExecute(testStatements);

        executer1.execute("commit");

        Database database = executer1.getDatabase();
        database.recoverUncommittedCmd(110); //模拟这时候断电导致executer2没有commit, 那么executer2的事情需要恢复

        executer2.execute("SELECT name FROM  student;");
        assertTrue(executer2.getResult(columnList, rowList));
        assertEquals(3,rowList.size());
        executer2.execute("SELECT name FROM  student where name = 'myq66';");
        assertTrue(executer2.getResult(columnList, rowList));
        assertEquals(0,rowList.size());
        executer2.execute("SELECT name FROM  student where name = 'myq';");
        assertTrue(executer2.getResult(columnList, rowList));
        assertEquals(1,rowList.size());

    }

    @After
    public void tearDown(){
        Database database = executer1.getDatabase();
        database.drop();
        manager.deleteDatabase(database.getName());

    }
}
