package cn.edu.thssdb.service;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.Visitor;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.util.ArrayList;
import java.util.List;

public class StatementExecuter {
	private final static String transaction_text = "BEGIN TRANSACTION";
	private final static String commit_text = "COMMIT";
	private StatementAdapter adapter;
	private Manager m;

	StatementExecuter() {
		Manager m = new Manager();
		m.createDatabaseIfNotExists("TEST");
		Database database = m.switchDatabase("TEST");
		adapter = new StatementAdapter(database);
	}

	public void execute(String statement) {
		if (statement.toUpperCase().trim().equals(transaction_text)) {
			this.adapter.initializeTransaction();
		} else if (statement.toUpperCase().trim().equals(commit_text)) {
			if (this.adapter.getInTransaction()) {
				this.adapter.terminateTransaction();
			}
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
}
