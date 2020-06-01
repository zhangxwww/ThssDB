package cn.edu.thssdb.service;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.service.StatementExecuter;
import cn.edu.thssdb.type.ColumnType;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class QueryTest {
	@Before
	public void setUp() {
		// Zhang Xinwei tai qiang le
	}

	@Test
	public void testCreateTable() {
		Manager manager = new Manager();
		manager.createDatabaseIfNotExists("UnitTest");
		Database database = manager.switchDatabase("UnitTest");
		StatementExecuter executer = new StatementExecuter(database, 43);
		String statement = "create table student(ID  int, name String(24), PRIMARY Key(id));";
		executer.execute(statement);
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
		manager.createDatabaseIfNotExists("UnitTest");
		Database database = manager.switchDatabase("UnitTest");
		StatementExecuter executer = new StatementExecuter(database, 43);
		executer.execute("create table student(ID  int, name String(24), PRIMARY Key(id));");
		Table table = database.getTable("STUDENT");
		executer.execute("INSERT INTO student VALUES (010136, 'myq');");
		executer.execute("INSERT INTO student VALUES (233333, 'zxw');");
		executer.execute("INSERT INTO Student Values (10086,  'wyb');");
		int i = 0;
		for (Row row: table) {
			switch (i) {
				case 0:
					assertEquals("010136", row.getEntry(0));
					assertEquals("myq", row.getEntry(1));
					break;
				case 1:
					assertEquals("233333", row.getEntry(0));
					assertEquals("zxw", row.getEntry(1));
					break;
				case 2:
					assertEquals("10086", row.getEntry(0));
					assertEquals("wyb", row.getEntry(1));
					break;
				default:
					fail();
					break;
			}
			i += 1;
		}
	}

	@Test
	public void testSelect() {
		Manager manager = new Manager();
		manager.createDatabaseIfNotExists("UnitTest");
		Database database = manager.switchDatabase("UnitTest");
		StatementExecuter executer = new StatementExecuter(database, 43);

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
		Table studentTable = database.getTable("STUDENT");
		Table gpaTable = database.getTable("GPA");

	}
}
