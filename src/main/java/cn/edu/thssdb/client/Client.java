package cn.edu.thssdb.client;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.utils.Global;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.List;
import java.util.Scanner;

public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  static final String HOST_ARGS = "h";
  static final String HOST_NAME = "host";

  static final String HELP_ARGS = "help";
  static final String HELP_NAME = "help";

  static final String PORT_ARGS = "p";
  static final String PORT_NAME = "port";

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
  private static final Scanner SCANNER = new Scanner(System.in);

  private static TTransport transport;
  private static TProtocol protocol;
  private static IService.Client client;
  private static CommandLine commandLine;

  private static long session_id = 0;

  public static void main(String[] args) {
    commandLine = parseCmd(args);
    if (commandLine.hasOption(HELP_ARGS)) {
      showHelp();
      return;
    }
    try {
      echoStarting();
      String host = commandLine.getOptionValue(HOST_ARGS, Global.DEFAULT_SERVER_HOST);
      int port = Integer.parseInt(commandLine.getOptionValue(PORT_ARGS, String.valueOf(Global.DEFAULT_SERVER_PORT)));
      transport = new TFramedTransport(new TSocket(host, port));
      transport.open();
      protocol = new TBinaryProtocol(transport);
      client = new IService.Client(protocol);
      boolean open = true;
      while (true) {
        print(Global.CLI_PREFIX);
        String msg = SCANNER.nextLine();
        long startTime = System.currentTimeMillis();
        switch (msg.trim()) {
          case Global.SHOW_TIME:
            getTime();
            break;
          case Global.QUIT:
            open = false;
            break;
          default:
            // println("Invalid statements!");
            parseMsg(msg);
            break;
        }
        long endTime = System.currentTimeMillis();
        println("It costs " + (endTime - startTime) + " ms.");
        if (!open) {
          break;
        }
      }
      transport.close();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }

  private static void parseMsg(String msg) {
    String[] ms = msg.split(" ");
    if (ms[0].equals("connect")) {
      String username = ms[1];
      String password = ms[2];
      connect(username, password);
    } else if (ms[0].equals("disconnect")) {
      disconnect();
    } else {
      executeStatement(msg);
    }
  }

  private static void connect(String username, String password) {
    ConnectReq req = new ConnectReq(username, password);
    try {
      ConnectResp resp = client.connect(req);
      Status s = resp.getStatus();
      if (s.getCode() == Global.SUCCESS_CODE) {
        session_id = resp.getSessionId();
      } else {
        println("Fail to connect");
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void disconnect() {
    DisconnetReq req = new DisconnetReq(session_id);
    try {
      DisconnetResp resp = client.disconnect(req);
      Status s = resp.getStatus();
      if (s.getCode() == Global.SUCCESS_CODE) {
        println("Success");
      } else {
        println("Fail to disconnect");
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void executeStatement(String statement) {
    ExecuteStatementReq req = new ExecuteStatementReq(session_id, statement);
    try {
      ExecuteStatementResp resp = client.executeStatement(req);
      Status s = resp.getStatus();
      if (s.getCode() == Global.SUCCESS_CODE) {
        if (resp.isIsAbort()) {
          // TODO
        } else {
          if (resp.isHasResult()) {
            // TODO show the result
            List<String> columnsList = resp.getColumnsList();
            List<List<String>> rowList = resp.getRowList();
          }
        }
      } else {
        println("Fail to execute");
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void getTime() {
    GetTimeReq req = new GetTimeReq();
    try {
      println(client.getTime(req).getTime());
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  static Options createOptions() {
    Options options = new Options();
    options.addOption(Option.builder(HELP_ARGS)
        .argName(HELP_NAME)
        .desc("Display help information(optional)")
        .hasArg(false)
        .required(false)
        .build()
    );
    options.addOption(Option.builder(HOST_ARGS)
        .argName(HOST_NAME)
        .desc("Host (optional, default 127.0.0.1)")
        .hasArg(false)
        .required(false)
        .build()
    );
    options.addOption(Option.builder(PORT_ARGS)
        .argName(PORT_NAME)
        .desc("Port (optional, default 6667)")
        .hasArg(false)
        .required(false)
        .build()
    );
    return options;
  }

  static CommandLine parseCmd(String[] args) {
    Options options = createOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error(e.getMessage());
      println("Invalid command line argument!");
      System.exit(-1);
    }
    return cmd;
  }

  static void showHelp() {
    // TODO
    println("DO IT YOURSELF");
  }

  static void echoStarting() {
    println("----------------------");
    println("Starting ThssDB Client");
    println("----------------------");
  }

  static void print(String msg) {
    SCREEN_PRINTER.print(msg);
  }

  static void println() {
    SCREEN_PRINTER.println();
  }

  static void println(String msg) {
    SCREEN_PRINTER.println(msg);
  }
}
