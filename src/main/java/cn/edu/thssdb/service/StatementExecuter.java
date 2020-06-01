package cn.edu.thssdb.service;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.Visitor;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Global;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.util.ArrayList;
import java.util.List;

public class StatementExecuter {
	private final static String transaction_text = "BEGIN TRANSACTION";
	private final static String commit_text = "COMMIT";
	private final static String rollback_text = "ROLLBACK";
	private StatementAdapter adapter;
	private Database database;
	private long sessionId;

	public StatementExecuter(Database db, long sessionId) {
		this.database = db;
		this.sessionId = sessionId;
		adapter = new StatementAdapter(this.database, this.sessionId);
	}

	public void execute(String statement) {
		if (statement.toUpperCase().trim().equals(transaction_text)) {
			if (!this.adapter.getInTransaction()) {
				this.adapter.initializeTransaction();
			} // TODO: ERROR HANDLING
		} else if (statement.toUpperCase().trim().equals(commit_text)) {
			if (this.adapter.getInTransaction()) {
				this.adapter.getLogHandler().commit(sessionId);
				this.adapter.terminateTransaction();
			}
		} else if (statement.toUpperCase().trim().equals(rollback_text)) {
			if (this.adapter.getInTransaction()) {
				this.database.recoverUncommittedCmd(this.sessionId);
				this.adapter.terminateTransaction();
			} // TODO: ERROR HANDLING
		} else {
			CharStream input = CharStreams.fromString(statement);
			SQLLexer lexer = new SQLLexer(input);
			CommonTokenStream tokens = new CommonTokenStream(lexer);
			SQLParser parser = new SQLParser(tokens);
			Visitor visitor = new Visitor(adapter);
			SQLParser.ParseContext ctxTest = parser.parse();
			visitor.visitParse(ctxTest);
		}
	}

	public boolean getResult(List<String> columnList, List<List<String>> rowList) {
		// TODO
		return adapter.getResult(columnList, rowList);
	}
}
