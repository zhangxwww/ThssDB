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

public class StorageTest {

    @Before
    public void setUp() {
//        setUp里创建数据库和表，并插入信息，只进行一次即可，所以注释掉。
        Manager manager = new Manager();
        StatementExecuter executer1 = new StatementExecuter(manager,223);
        String statement1 = "create database STOTEST";
        executer1.execute(statement1);
        String statement2 = "use STOTEST";
        executer1.execute(statement2);

        List<String> testStatements = new ArrayList<String>() {{
            add("create table student(ID  int, name String(24), PRIMARY Key(id));");
        }};

        for (int i = 0; i < 30; i++){
            String s = "INSERT INTO student VALUES (" + String.valueOf(i+201600)+", 'name" + String.valueOf(i) + "');";
            testStatements.add(s);
        }
        executer1.batchExecute(testStatements);
        executer1.getDatabase().persist();
    }

    @Test
    public void testRecoverDatabase() {
        Manager manager = new Manager();
        StatementExecuter executer2 = new StatementExecuter(manager,110);
        String statement2 = "use STOTEST";
        executer2.execute(statement2);
        //证明读取完毕后是30个学生信息
        List<String> columnList = new ArrayList<>();
        List<List<String>> rowList = new ArrayList<>();
        executer2.execute("SELECT name FROM  student;");
        assertTrue(executer2.getResult(columnList, rowList));
        assertEquals(30,rowList.size());


        //删除5个学生信息
        List<String> testStatements = new ArrayList<String>();

        for (int i = 0; i < 5; i++){
            String s = "DELETE FROM student Where id = " + String.valueOf(i*4+201600)+ ";";
            testStatements.add(s);
        }
        executer2.batchExecute(testStatements);
        executer2.getDatabase().persist();

        //然后重新打开系统，证明删除后确实是25个同学
        Manager manager2 = new Manager();
        StatementExecuter executer3 = new StatementExecuter(manager,190);
        executer3.execute(statement2);
        executer3.execute("SELECT name FROM  student;");
        assertTrue(executer3.getResult(columnList, rowList));
        assertEquals(25,rowList.size());

        //为了单元测试方便，把五位同学加回去
        for (int i = 0; i < 5; i++){
            String s = "INSERT INTO student VALUES (" + String.valueOf(i*4+201600)+", 'name" + String.valueOf(i*5) + "');";
            testStatements.add(s);
        }
        executer3.batchExecute(testStatements);
        executer3.getDatabase().persist();


    }
    @After
    public void tearDown(){
        Manager manager = new Manager();
        StatementExecuter executer = new StatementExecuter(manager,666);
        String statement2 = "use STOTEST";
        executer.execute(statement2);
        Database database = executer.getDatabase();
        database.drop();
        manager.deleteDatabase(database.getName());

    }



}
