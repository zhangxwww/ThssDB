package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.SyntaxErrorException;
import cn.edu.thssdb.query.Condition;
import cn.edu.thssdb.query.JoinCondition;
import cn.edu.thssdb.query.OrderBy;
import cn.edu.thssdb.query.WhereCondition;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.service.StatementAdapter;
import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.swing.plaf.nimbus.State;
import java.util.ArrayList;
import java.util.List;

public class Visitor extends SQLBaseVisitor {
	private StatementAdapter statementAdapter;

	public Visitor(StatementAdapter adpt) {
		this.statementAdapter = adpt;
	}

	@Override
	public Object visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {

		String tableName = ctx.table_name().getText().toUpperCase();
		List<SQLParser.Column_defContext> rawCols = ctx.column_def();
		Column[] cols = new Column[rawCols.size()];
		String primary = ctx.table_constraint().column_name(0).getText().toUpperCase();
		for (int i = 0; i < rawCols.size(); i++) {
			SQLParser.Column_defContext c = rawCols.get(i);
			List<SQLParser.Column_constraintContext> constraints = c.column_constraint();

			String typeName = c.type_name().getText().toUpperCase();
			String colName = c.column_name().getText().toUpperCase();
			ColumnType type;
			int length = -1;
			int isPrimary = 0;
			boolean notNull = false;
			boolean unique = false;
			int nConstraints = constraints.size();
			for (int j = 0; j < nConstraints; ++j) {
				if (constraints.get(j).getText().toUpperCase().equals("NOTNULL")) {
					notNull = true;
				} else if (constraints.get(j).getText().toUpperCase().equals("UNIQUE")) {
					unique = true;
				}
			}

			if (typeName.startsWith("STRING")) {
				type = ColumnType.STRING;
				length = Integer.parseInt(c.type_name().NUMERIC_LITERAL().getText());
			} else {
				type = ColumnType.valueOf(typeName);
			}
			if (primary.equals(colName)) {
				isPrimary = 1;
			}
			cols[i] = new Column(colName, type, isPrimary, notNull, length, unique);
		}
		statementAdapter.createTable(tableName, cols);
		return null;
	}

	@Override
	public Object visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
		String tableName = ctx.table_name().getText().toUpperCase();
		statementAdapter.dropTable(tableName);
		return null;
	}

	@Override
	public Object visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
		//如果没有column name, 说明是插入整行，所以需要获取表的columns信息
		String tableName = ctx.table_name().getText().toUpperCase();
		List<SQLParser.Column_nameContext> colNames = ctx.column_name();
		List<SQLParser.Literal_valueContext> values = ctx.value_entry(0).literal_value();

		//转换
		String[] attrNames = new String[colNames.size()];
		for (int j = 0; j < attrNames.length; j++) {
			attrNames[j] = colNames.get(j).getText().toUpperCase();
		}
		String[] attrValues = new String[values.size()];
		for (int i = 0; i < attrValues.length; i++) {
			SQLParser.Literal_valueContext val = values.get(i);
			String tmp;
			TerminalNode stringVal = val.STRING_LITERAL();
			TerminalNode numericVal = val.NUMERIC_LITERAL();
			if (stringVal == null) {
				tmp = numericVal.getText();
			} else {
				tmp = stringVal.getText();
			}
			attrValues[i] = tmp;
		}

		if (colNames.size() == 0) {
			statementAdapter.insertTableRow(tableName, attrValues);
		} else {
			statementAdapter.insertTableRow(tableName, attrNames, attrValues);
		}

		//且必须含有主键
		return null;
	}

	@Override
	public Object visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
		String tableName = ctx.table_name().getText().toUpperCase();
		WhereCondition wherecond = (WhereCondition) visitMultiple_condition(ctx.multiple_condition());
		statementAdapter.delFromTable(tableName, wherecond);
		return null;
	}

	@Override
	public Object visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
		String tableName = ctx.table_name().getText().toUpperCase();
		String colName = ctx.column_name().getText().toUpperCase();
		String attrValue = ctx.expression().getText();
		WhereCondition wherecond = (WhereCondition) visitMultiple_condition(ctx.multiple_condition());
		statementAdapter.updateTable(tableName, colName, attrValue, wherecond);
		return null;
	}

	@Override
	public Object visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
		// select clause
		List<SQLParser.Result_columnContext> cols = ctx.result_column();
		int m = cols.size();
		List<Pair<String, String>> results = new ArrayList<Pair<String, String>>(m);
		for (int i = 0; i < m; i++) {
			SQLParser.Column_full_nameContext c = cols.get(i).column_full_name();
			SQLParser.Table_nameContext tableNameContext = c.table_name();
			String tableName = "";
			if (tableNameContext != null) {
				tableName = tableNameContext.getText();
			}
			String attrName = c.column_name().getText();
			results.add(new Pair<>(tableName.toUpperCase(), attrName.toUpperCase()));
//			results.set(i, new Pair<>(tableName, attrName));
		}


		// from clause
		List<SQLParser.Table_queryContext> tables = ctx.table_query();
		JoinCondition joinCondition = null;
		String table1 = "";
		String table2 = "";
		int numTables = tables.get(0).table_name().size();
		if (numTables == 1) {
			table1 = tables.get(0).table_name().get(0).getText().toUpperCase();
		} else {
			// TODO: Exception Handle
			table1 = tables.get(0).table_name().get(0).getText().toUpperCase();
			table2 = tables.get(0).table_name().get(1).getText().toUpperCase();
			// judge outer join
			JoinCondition.JoinType joinType = JoinCondition.JoinType.INNER;
			if (tables.get(0).getChild(2).getText().toUpperCase().equals("OUTER")) {
				if (tables.get(0).getChild(1).getText().toUpperCase().equals("LEFT")) {
					joinType = JoinCondition.JoinType.LEFT_OUTER;
				} else if (tables.get(0).getChild(1).getText().toUpperCase().equals("RIGHT")) {
					joinType = JoinCondition.JoinType.RIGHT_OUTER;
				} else {
					// TODO: EXCEPTION HANDLE
					throw new SyntaxErrorException();
				}
			}
			SQLParser.Multiple_conditionContext joinConditionContext = tables.get(0).multiple_condition();
			if (joinConditionContext != null) {
				joinCondition = (JoinCondition) visitMultiple_condition(joinConditionContext);
				joinCondition.type = joinType;
			}
		}

		// where clause
		WhereCondition whereCondition = null;
		SQLParser.Multiple_conditionContext whereConditionContext = ctx.multiple_condition();
		if (whereConditionContext != null) {
			whereCondition = (WhereCondition) visitMultiple_condition(ctx.multiple_condition());
		}

		//distinct
		boolean distinct = false;
		if (ctx.K_DISTINCT() != null) {
			distinct = true;
		}
		//order
		OrderBy orderBy = null;
		SQLParser.Order_columnContext order_column = ctx.order_column();
		if (order_column != null){
			SQLParser.Column_full_nameContext order_columnContext = ctx.order_column().column_full_name();
			SQLParser.Table_nameContext table_nameContext = order_columnContext.table_name();
			String orderTableName = "";
			if (table_nameContext != null){
				orderTableName = table_nameContext.getText().toUpperCase();
			}

			String orderColName = order_columnContext.column_name().getText().toUpperCase();

			boolean desc = false;
			if (ctx.K_DESC() != null) {
				desc = true;
			}
			orderBy = new OrderBy(orderTableName, orderColName, desc);
		}

		statementAdapter.select(results, table1, table2, joinCondition, whereCondition, distinct, orderBy);
		return null;
	}

	@Override
	public Condition visitCondition(SQLParser.ConditionContext ctx) {
		SQLParser.ExpressionContext exp1 = ctx.expression(0);
		SQLParser.ExpressionContext exp2 = ctx.expression(1);

		// Comparator
		String operator = ctx.comparator().getText();

		// First Comparer
		SQLParser.Column_full_nameContext attrContext1 = exp1.comparer().column_full_name();
		String tableName1 = "";
		if (attrContext1.table_name() != null) {
			tableName1 = attrContext1.table_name().getText().toUpperCase();
		}
		String columnName1 = attrContext1.column_name().getText().toUpperCase();

		// Second Comparer
		SQLParser.Literal_valueContext literal = exp2.comparer().literal_value();
		if (literal != null) {
			// where condition
			String value = literal.getText();
			return new WhereCondition(operator, tableName1, columnName1, value);
		} else {
			// join condition
			SQLParser.Column_full_nameContext attrContext2 = exp2.comparer().column_full_name();
			String tableName2 = "";
			if (attrContext2.table_name() != null) {
				tableName2 = attrContext2.table_name().getText().toUpperCase();
			}
			String columnName2 = attrContext2.column_name().getText().toUpperCase();
			return new JoinCondition(operator, tableName1, tableName2, columnName1, columnName2);
		}
	}

	@Override
	public Object visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
		// return super.visitShow_meta_stmt(ctx);
		String tableName = ctx.table_name().getText().toUpperCase();
		statementAdapter.showTable(tableName);
		return null;
	}

	@Override
	public Object visitShow_table_stmt(SQLParser.Show_table_stmtContext ctx) {
		String tableName = ctx.table_name().getText().toUpperCase();
		statementAdapter.showTable(tableName);
		return null;
	}
}

