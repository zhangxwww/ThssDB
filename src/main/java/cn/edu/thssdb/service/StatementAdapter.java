package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.WrongInsertArgumentNumException;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thssdb.schema.*;

public class StatementAdapter {
	private Database database;
	private boolean isInTransaction = false;
	private List<Table> exclusiveLockedTables = new ArrayList<Table>();

	public StatementAdapter(Database database) {
		this.database = database;
	}

	public void initializeTransaction() {
		this.isInTransaction = true;
	}

	public void createTable(String tbName, Column[] cols) {
		database.createTable(tbName, cols);
	}

	public void dropTable(String tbName) {
		database.dropTable(tbName);
	}

	public int tableAttrsNum(String tbName) {
		//获取该表的属性个数
		Table t = database.getTable(tbName);
		return t.getColumns().size();
	}

	public void insertTableRow(String tbName, String[] attrValues) {
		//插入整行
		int attrsNum = tableAttrsNum(tbName);
		if (attrValues.length != attrsNum) {
			//TODO 异常处理 所给values个数不对
			System.out.println("Insert Failure! valueEntries.size()! = attrsNum");
			return;
		} else {
			//TODO 插入整行
			Table t = database.getTable(tbName);
			if (this.isInTransaction) {
			    t.getLock().writeLock().lock();
			    this.exclusiveLockedTables.add(t);
            }
			ArrayList<Column> attrs = t.getColumns();
			Entry[] entries = new Entry[attrsNum];
			for (int i = 0; i < attrs.size(); i++) {
				Column tmpAttr = attrs.get(i);
				ColumnType attrType = tmpAttr.getType();
				Entry e = null;
				switch (attrType) {
					case INT:
						e = new Entry(Integer.parseInt(attrValues[i]));
						break;
					case LONG:
						e = new Entry(Long.parseLong(attrValues[i]));
						break;
					case FLOAT:
						e = new Entry(Float.parseFloat(attrValues[i]));
						break;
					case DOUBLE:
						e = new Entry(Double.parseDouble(attrValues[i]));
						break;
					case STRING:
						e = new Entry(attrValues[i]);
						break;
				}
				entries[i] = e;
			}
			t.insert(new Row(entries));
		}
	}

	public void insertTableRow(String tbName, String[] attrNames, String[] attrValues) {
		//value的个数要与colomn个数一致
		if (attrNames.length != attrValues.length) {
			//TODO 异常处理 所给长度不一致
			throw new WrongInsertArgumentNumException();
		} else {
			String primaryKeyName = getTablePrimaryAttr(tbName);
			boolean hasPrimaryKey = false;
			for (String attrName : attrNames) {
				if (attrName.toUpperCase().equals(primaryKeyName.toUpperCase())) {
					hasPrimaryKey = true;
				}
			}
			if (!hasPrimaryKey) {
				//TODO 异常处理 插入的属性中没有主键
				return;
			}

			//插入一条Row
			Table t = database.getTable(tbName);
            if (this.isInTransaction) {
                t.getLock().writeLock().lock();
                this.exclusiveLockedTables.add(t);
            }
			ArrayList<Column> attrs = t.getColumns();
			Entry[] entries = new Entry[attrs.size()];
			for (int i = 0; i < attrs.size(); i++) {
				Column tmpAttr = attrs.get(i);
				Entry e = null;
				for (int j = 0; j < attrNames.length; j++) {
					if (attrNames[j].toUpperCase().equals(tmpAttr.getName().toUpperCase())) {
						ColumnType attrType = tmpAttr.getType();
						e = parseValue(attrType, attrValues[j]);
						break;
					}//为给出的属性值就为null
				}
				entries[i] = e;
			}
			t.insert(new Row(entries));
		}
	}


	public void delFromTable(String tbName, WhereCondition wherecond) {
		//whereCondition可能为空
		Table t = database.getTable(tbName);
        if (this.isInTransaction) {
            t.getLock().writeLock().lock();
            this.exclusiveLockedTables.add(t);
        }
		QueryTable q = getQueryTable(tbName, wherecond);
		while (q.hasNext()) {
			t.delete(q.next());
		}
	}

	public String getTablePrimaryAttr(String tbName) {
		Table t = database.getTable(tbName);
		return t.getPrimaryKeyName();
	}

	public void updateTable(String tbName, String colName, String attrValue, WhereCondition wherecond) {
		Table t = database.getTable(tbName);
        if (this.isInTransaction) {
            t.getLock().writeLock().lock();
            this.exclusiveLockedTables.add(t);
        }
		QueryTable q = getQueryTable(tbName, wherecond);
		ColumnType cType = null;
		int attrIndex = -1;
		for (int i = 0; i < t.getColumns().size(); i++) {
			Column c = t.getColumns().get(i);
			if (c.getName().toUpperCase().equals(colName.toUpperCase())) {
				cType = c.getType();
				attrIndex = i;
				break;
			}
		}
		if (attrIndex == -1) {
			// TODO throw error: attr not in columns
		}
		assert cType != null;
		Entry e = parseValue(cType, attrValue);

		while (q.hasNext()) {
			t.update(attrIndex, e, q.next());
		}
	}


	public Table select(List<Pair<String, String>> results, String table1, String table2, JoinCondition jc, WhereCondition wc) {
		// TODO
        if (this.isInTransaction) {
            database.getTable(table1).getLock().readLock().lock();
            if (table2 != null && table2.length() > 0) {
                database.getTable(table2).getLock().readLock().lock();
            }
        }
		QueryTable[] q;
		boolean needJoin;
		int jIndex1 = -1, jIndex2 = -1;
		if (table2 == null || table2.equals("") || jc == null) {
			// 不需要join
			q = new QueryTable[]{getQueryTable(table1, wc)};
			needJoin = false;
		} else {
			// 检查where中是否存在歧义
			if (!checkAmbiguousColumnForWhere(table1, table2, wc)) {
				// TODO throw error: ambiguous column in where condition
			}
			q = new QueryTable[]{getQueryTable(table1, wc), getQueryTable(table2, wc)};
			// 哪两列被join
			List<Integer> joinIndex = getJoinIndex(table1, table2, jc);
			jIndex1 = joinIndex.get(0);
			jIndex2 = joinIndex.get(1);
			needJoin = true;
		}
		// select哪些列
		List<Integer> index = getSelectIndex(results, table1, table2);
		List<Row> qResult = new QueryResult(q, index, needJoin, jIndex1, jIndex2).query();
		Table result = generateTmpTable(qResult, table1, table2, index);
		if (this.isInTransaction) {
            database.getTable(table1).getLock().readLock().unlock();
            if (table2 != null && table2.length() > 0) {
                database.getTable(table2).getLock().readLock().unlock();
            }
        }
		return result;
	}

	private QueryTable getQueryTable(String table, WhereCondition wc) {
		QueryTable q;
		Table t = database.getTable(table);
		if (wc != null && (wc.tableName.equals(table) || wc.tableName.equals(""))) {
			AttrCompare compare = new AttrCompare(wc.op);
			String attr = wc.attr;
			ColumnType cType = null;
			for (Column c : t.getColumns()) {
				if (c.getName().toUpperCase().equals(attr.toUpperCase())) {
					cType = c.getType();
					break;
				}
				// TODO throw error: attr not in columns
			}
			String value = wc.val;
			Entry instant = parseValue(cType, value);
			q = new QueryTable(compare, t, attr, instant);
		} else {
			q = new QueryTable(null, t, null, null);
		}
		return q;
	}


	private Entry parseValue(ColumnType type, String value) {
		Entry instant;
		switch (type) {
			case INT:
				instant = new Entry(Integer.parseInt(value));
				break;
			case LONG:
				instant = new Entry(Long.parseLong(value));
				break;
			case FLOAT:
				instant = new Entry(Float.parseFloat(value));
				break;
			case DOUBLE:
				instant = new Entry(Double.parseDouble(value));
				break;
			case STRING:
			default:
				instant = new Entry(value);
				break;
		}
		return instant;
	}

	private List<Integer> getSelectIndex(List<Pair<String, String>> res, String t1, String t2) {
		List<Integer> results = new ArrayList<>();
		List<String> attrs = new ArrayList<>();
		int midIndex = mergeAttrs(t1, t2, attrs);
		for (Pair<String, String> p : res) {
			String tbName = p.getKey();
			String attrName = p.getValue();
			int index = 0;
			if (tbName.equals("")) {
				int first = attrs.indexOf(attrName);
				int last = attrs.lastIndexOf(attrName);
				if (first != last) {
					// TODO throw error: ambiguous column name
				} else if (first < 0) {
					// TODO throw error: column not exists
				} else {
					index = first;
				}
			} else {
				if (tbName.toUpperCase().equals(t1.toUpperCase())) {
					int tmp = attrs.indexOf(attrName);
					if (tmp >= midIndex) {
						// TODO throw error: attrName not exists in table
					} else {
						index = tmp;
					}
				} else if (tbName.toUpperCase().equals(t2.toUpperCase())) {
					int tmp = attrs.lastIndexOf(attrName);
					if (tmp < midIndex) {
						// TODO throw error: attrName not exists in table
					} else {
						index = tmp;
					}
				} else {
					// TODO throw error: table not exists
				}
			}
			results.add(index);
		}
		return results;
	}

	private List<Integer> getJoinIndex(String t1, String t2, JoinCondition jc) {
		List<Integer> results = new ArrayList<>();
		List<String> attrs = new ArrayList<>();
		int midIndex = mergeAttrs(t1, t2, attrs);
		ArrayList<Column> c1 = database.getTable(t1).getColumns();
		ArrayList<Column> c2 = database.getTable(t2).getColumns();
		if (jc.table1.equals(t1) && jc.table2.equals(t2)) {
			for (int i = 0; i < c1.size(); ++i) {
				if (c1.get(i).getName().toUpperCase().equals(jc.attr1.toUpperCase())) {
					results.set(0, i);
					break;
				}
				// TODO throw error: attr not in table
			}
			for (int i = 0; i < c2.size(); ++i) {
				if (c2.get(i).getName().toUpperCase().equals(jc.attr2.toUpperCase())) {
					results.set(1, i + midIndex);
					break;
				}
				// TODO throw error: attr not in table
			}
		} else if (jc.table1.toUpperCase().equals(t2.toUpperCase()) && jc.table2.toUpperCase().equals(t1.toUpperCase())) {
			for (int i = 0; i < c1.size(); ++i) {
				if (c1.get(i).getName().toUpperCase().equals(jc.attr2.toUpperCase())) {
					results.set(0, i);
					break;
				}
				// TODO throw error: attr not in table
			}
			for (int i = 0; i < c2.size(); ++i) {
				if (c2.get(i).getName().toUpperCase().equals(jc.attr1.toUpperCase())) {
					results.set(1, i + midIndex);
					break;
				}
				// TODO throw error: attr not in table
			}
		} else {
			// TODO throw error: table name can't match
		}
		return results;
	}

	private boolean checkAmbiguousColumnForWhere(String t1, String t2, WhereCondition wc) {
		if (wc == null) {
			return true;
		}
		if (wc.tableName.toUpperCase().equals(t1.toUpperCase()) || wc.tableName.toUpperCase().equals(t2.toUpperCase())) {
			return true;
		}
		if (wc.tableName.equals("")) {
			Table table1 = database.getTable(t1);
			Table table2 = database.getTable(t2);
			boolean found = false;
			for (Column c : table1.getColumns()) {
				if (c.getName().toUpperCase().equals(wc.attr.toUpperCase())) {
					found = true;
					break;
				}
			}
			if (!found) {
				return true;
			}
			for (Column c : table2.getColumns()) {
				if (c.getName().toUpperCase().equals(wc.attr.toUpperCase())) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	private Table generateTmpTable(List<Row> rows, String t1, String t2, List<Integer> index) {
		List<Column> newColumn = new ArrayList<>();
		List<String> attrs = new ArrayList<>();
		int midIndex = mergeAttrs(t1, t2, attrs);
		Table table1, table2;
		List<Column> c1 = new ArrayList<>();
		List<Column> c2 = new ArrayList<>();
		table1 = database.getTable(t1);
		c1 = table1.getColumns();
		if (t2 != null && !t2.equals("")) {
			table2 = database.getTable(t2);
			c2 = table2.getColumns();
		}
		for (int i : index) {
			if (i < midIndex) {
				newColumn.add(new Column(c1.get(i)));
			} else {
				newColumn.add(new Column(c2.get(i - midIndex)));
			}
		}
		Table tmpTable = new Table("", "__TEMP", (Column[]) newColumn.toArray(), table1.primaryIndex);
		for (Row r : rows) {
			tmpTable.insert(r);
		}
		return tmpTable;
	}

	private int mergeAttrs(String t1, String t2, List<String> mergedAttrs) {
		Table table1, table2;
		int midIndex = 0;
		if (t1 != null && !t1.equals("")) {
			table1 = database.getTable(t1);
			midIndex = tableAttrsNum(t1);
			for (Column c : table1.getColumns()) {
				mergedAttrs.add(c.getName());
			}
		}
		if (t2 != null && !t2.equals("")) {
			table2 = database.getTable(t2);
			for (Column c : table2.getColumns()) {
				mergedAttrs.add(c.getName());
			}
		}
		return midIndex;
	}

	void setInTransaction(boolean value) {
		this.isInTransaction = value;
	}

	boolean getInTransaction() {
		return this.isInTransaction;
	}

	void terminateTransaction() {
		for (Table t : this.exclusiveLockedTables) {
			t.getLock().writeLock().unlock();
		}
		this.exclusiveLockedTables.clear();
		this.isInTransaction = false;
	}
}
