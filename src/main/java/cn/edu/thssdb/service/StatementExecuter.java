package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.SyntaxErrorException;
import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.Visitor;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import org.antlr.v4.runtime.*;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StatementExecuter {
    private final static String transaction_text = "BEGIN TRANSACTION";
    private final static String commit_text = "COMMIT";
    private final static String rollback_text = "ROLLBACK";
    private StatementAdapter adapter;
    private Manager manager;
    private Database database;
    private long sessionId;

    public StatementExecuter(Manager manager, long sessionId) {
        this.database = null;
        this.manager = manager;
        this.sessionId = sessionId;
        adapter = null;
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
        } else if (parseCreateDatabaseStatement(statement) != null) {
            createDatabase(parseCreateDatabaseStatement(statement));
        } else if (parseSwitchDatabaseStatement(statement) != null) {
            switchDatabase(parseSwitchDatabaseStatement(statement));
        } else {
            CharStream input = CharStreams.fromString(statement);
            SQLLexer lexer = new SQLLexer(input);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SQLParser parser = new SQLParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(new BaseErrorListener() {
                @Override
                public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                    super.syntaxError(recognizer, offendingSymbol, line, charPositionInLine, msg, e);
                    throw new SyntaxErrorException();
                }
            });
            Visitor visitor = new Visitor(adapter);
            SQLParser.ParseContext ctxTest = parser.parse();
            visitor.visitParse(ctxTest);
        }
    }

    public void disconnect() {
        if (database != null) {
            database.persist();
        }
    }

    private String parseCreateDatabaseStatement(String statement) {
        String pattern = "CREATE\\s+DATABASE\\s+([^;]*);?";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(statement.toUpperCase().trim());
        if (m.find()) {
            return m.group(1);
        } else {
            return null;
        }
    }

    private String parseSwitchDatabaseStatement(String statement) {
        String pattern = "USE\\s+([^;]*);?";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(statement.toUpperCase().trim());
        if (m.find()) {
            return m.group(1);
        } else {
            return null;
        }
    }

    public boolean getResult(List<String> columnList, List<List<String>> rowList) {
        // TODO
        if (adapter == null) {
            return false;
        }
        return adapter.getResult(columnList, rowList);
    }

    private void createDatabase(String dbName) {
        manager.createDatabaseIfNotExists(dbName.toUpperCase());
    }

    private void switchDatabase(String dbName) {
        database = manager.switchDatabase(dbName.toUpperCase());
        adapter = new StatementAdapter(this.database, this.sessionId);
    }

    public void batchExecute(List<String> statements) {
        for (String statement : statements) {
            this.execute(statement);
        }
    }

    public Database getDatabase() {
        return database;
    }
}
