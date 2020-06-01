package cn.edu.thssdb.index;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.service.StatementExecuter;
import cn.edu.thssdb.type.ColumnType;
import org.junit.Before;
import org.junit.Test;
import cn.edu.thssdb.schema.Manager;

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
	}
}
