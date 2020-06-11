package cn.edu.thssdb.service;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.service.StatementExecuter;
import cn.edu.thssdb.type.ColumnType;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class QueryTest {
	@Before
	public void setUp() {
		// Zhang Xinwei tai qiang le ???
	}

	@Test
	public void testCreateTable() {
		Manager manager = new Manager();
		StatementExecuter executer = new StatementExecuter(manager, 43);
		String statement1 = "create database UNITTEST";
		executer.execute(statement1);
		String statement2 = "use UNITTEST";
		executer.execute(statement2);
		String statement = "create table student(ID  int, name String(24), PRIMARY Key(id));";
		executer.execute(statement);
		Database database = executer.getDatabase();
		assertNotNull(database);
		Table table = database.getTable("STUDENT");
		assertNotNull(table);
		List<Column> columns = table.getColumns();
		assertEquals(columns.size(), 2);
		assertEquals(columns.get(0).getName(), "ID");
		assertEquals(columns.get(1).getName(), "NAME");
		assertEquals(columns.get(0).getType(), ColumnType.INT);
		assertEquals(columns.get(1).getType(), ColumnType.STRING);
		assertTrue(columns.get(0).isPrimary());
	}

	@Test
	public void testInsert() {
		Manager manager = new Manager();

		StatementExecuter executer = new StatementExecuter(manager, 43);

		String statement1 = "create database UNITTEST";
		executer.execute(statement1);
		String statement2 = "use UNITTEST";
		executer.execute(statement2);

		executer.execute("create table student(ID  int, name String(24), PRIMARY Key(id));");
		executer.execute("INSERT INTO student VALUES (010136, 'myq');");
		executer.execute("INSERT INTO student VALUES (233333, 'zxw');");
		executer.execute("INSERT INTO Student Values (10086,  'wyb');");

		Database database = executer.getDatabase();
		assertNotNull(database);

		Table table = database.getTable("STUDENT");

		HashMap<String, String> entries = new HashMap<String, String>();
		for (Row row: table) {
			entries.put(row.getEntry(0), row.getEntry(1));
		}
		assertEquals(entries.size(), 3);
		assertEquals(entries.get("010136"), "myq");
		assertEquals(entries.get("233333"), "zxw");
		assertEquals(entries.get("10086"), "wyb");
	}

	@Test
	public void testSelect() {
		Manager manager = new Manager();
		StatementExecuter executer = new StatementExecuter(manager, 43);

		String statement1 = "create database UNITTEST";
		executer.execute(statement1);
		String statement2 = "use UNITTEST";
		executer.execute(statement2);

		List<String> testStatements = new ArrayList<String>() {{
			add("create table student(ID  int, name String(24), PRIMARY Key(id));");
			add("INSERT INTO student VALUES (010136, 'myq');");
			add("INSERT INTO student VALUES (233333, 'zxw');");
			add("INSERT INTO Student Values (10086,  'wyb');");
			add("create table gpa(name String(24), score float, primary key(name));");
			add("insert into gpa values ('myq', 4.0);");
			add("insert into gpa values ('wyb', 4.0);");
		}};
		executer.batchExecute(testStatements);

		Database database = executer.getDatabase();
		assertNotNull(database);

		Table studentTable = database.getTable("STUDENT");
		Table gpaTable = database.getTable("GPA");

		// Test first select statement
		List<String> columnList = new ArrayList<>();
		List<List<String>> rowList = new ArrayList<>();
		executer.execute("SELECT  id, name  FROM  student WHERE  name = 'myq'");
		assertTrue(executer.getResult(columnList, rowList));
		assertEquals(columnList.size(), 2);
		String[] expectedColumnlist = new String[] {"ID", "NAME"};
		assertArrayEquals(columnList.toArray(), expectedColumnlist);
		ArrayList<String[]> expectedRowList = new ArrayList<String[]> () {{
			add(new String[] {"10136", "myq"});
		}};
		int rowListSize = rowList.size();
		assertEquals(rowListSize, 1);
		for (int i = 0; i < rowListSize; i++) {
			assertArrayEquals(rowList.get(i).toArray(), expectedRowList.get(i));
		}

		// Test second select statement
		columnList = new ArrayList<>();
		rowList = new ArrayList<>();
		executer.execute("SELECT  name  FROM  student WHERE  id = 233333");
		assertTrue(executer.getResult(columnList, rowList));
		assertEquals(columnList.size(), 1);
		assertEquals(columnList.get(0), "NAME");
		expectedRowList = new ArrayList<String[]> () {{
			add(new String[] {"zxw"});
		}};
		rowListSize = rowList.size();
		assertEquals(rowListSize, 1);
		for (int i = 0; i < rowListSize; i++) {
			assertArrayEquals(rowList.get(i).toArray(), expectedRowList.get(i));
		}

		// Test third select statement
		columnList = new ArrayList<>();
		rowList = new ArrayList<>();
		executer.execute("SELECT  id  FROM  student WHERE  name = 'wyb'");
		assertTrue(executer.getResult(columnList, rowList));
		assertEquals(columnList.size(), 1);
		assertEquals(columnList.get(0), "ID");
		expectedRowList = new ArrayList<String[]> () {{
			add(new String[] {"10086"});
		}};
		rowListSize = rowList.size();
		assertEquals(rowListSize, 1);
		for (int i = 0; i < rowListSize; i++) {
			assertArrayEquals(rowList.get(i).toArray(), expectedRowList.get(i));
		}

		// Test fourth select statement
		columnList = new ArrayList<>();
		rowList = new ArrayList<>();
		executer.execute("SELECT  student.id  FROM  student WHERE  name = 'wyb'");
		assertTrue(executer.getResult(columnList, rowList));
		assertEquals(columnList.size(), 1);
		assertEquals(columnList.get(0), "ID");
		expectedRowList = new ArrayList<String[]> () {{
			add(new String[] {"10086"});
		}};
		rowListSize = rowList.size();
		assertEquals(rowListSize, 1);
		for (int i = 0; i < rowListSize; i++) {
			assertArrayEquals(rowList.get(i).toArray(), expectedRowList.get(i));
		}

		// Test fifth select statement
		columnList = new ArrayList<>();
		rowList = new ArrayList<>();
		executer.execute("SELECT  student.id  FROM  student WHERE  name = 'wyb'");
		assertTrue(executer.getResult(columnList, rowList));
		assertEquals(columnList.size(), 1);
		assertEquals(columnList.get(0), "ID");
		expectedRowList = new ArrayList<String[]> () {{
			add(new String[] {"10086"});
		}};
		rowListSize = rowList.size();
		assertEquals(rowListSize, 1);
		for (int i = 0; i < rowListSize; i++) {
			assertArrayEquals(rowList.get(i).toArray(), expectedRowList.get(i));
		}
	}
}
