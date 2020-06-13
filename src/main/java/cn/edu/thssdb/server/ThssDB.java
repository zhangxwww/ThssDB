package cn.edu.thssdb.server;

import cn.edu.thssdb.query.WhereCondition;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.service.IServiceHandler;
import cn.edu.thssdb.service.LogHandler;
import cn.edu.thssdb.service.StatementAdapter;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Statement;
import java.util.ArrayList;

public class ThssDB {

	private static final Logger logger = LoggerFactory.getLogger(ThssDB.class);

	private static IServiceHandler handler;
	private static IService.Processor processor;
//	private static TServerSocket transport;
	private static TServerSocket transport;
	private static TServer server;


	private Manager manager;
	private Database database;

	public static ThssDB getInstance() {
		return ThssDBHolder.INSTANCE;
	}

	public static void main(String[] args) {
		ThssDB server = ThssDB.getInstance();
		server.start();
	}

	private void start() {
		manager = new Manager();
		// manager.createDatabaseIfNotExists("TEST");
		// database = manager.switchDatabase("TEST");
		//并且自动查询log文件，看看有没有要恢复的内容
		// this.database.recoverUncommittedCmd(0);
		handler = new IServiceHandler(manager);
		processor = new IService.Processor(handler);
		Runnable setup = () -> setUp(processor);
		new Thread(setup).start();
	}

	private static void setUp(IService.Processor processor) {
		try {
			transport = new TServerSocket(Global.DEFAULT_SERVER_PORT);
			server = new TSimpleServer(new TServer.Args(transport).processor(processor));
//			transport = new TServerSocket(Global.DEFAULT_SERVER_PORT);
//			TThreadPoolServer.Args tArgs = new TThreadPoolServer.Args(transport);
//			tArgs.processor(processor);
////			客户端协议要一致
//			tArgs.protocolFactory(new TBinaryProtocol.Factory());
//			server = new TThreadPoolServer(tArgs);
			logger.info("Starting ThssDB ...");
			server.serve();
		} catch (TTransportException e) {
			logger.error(e.getMessage());
		}
	}

	private static class ThssDBHolder {
		private static final ThssDB INSTANCE = new ThssDB();
		private ThssDBHolder() {

		}
	}
}
