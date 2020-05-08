package cn.edu.thssdb.utils;

public class Global {

  public static String USERNAME = "admin";
  public static String PASSWORD = "admin";

  public static int fanout = 129;

  public static int SUCCESS_CODE = 0;
  public static int FAILURE_CODE = -1;

  public static String DEFAULT_SERVER_HOST = "127.0.0.1";
  public static int DEFAULT_SERVER_PORT = 6667;

  public static String CLI_PREFIX = "ThssDB>";
  public static final String SHOW_TIME = "show time;";
  public static final String QUIT = "quit;";
  // public static final String CONNECT = "connect"

  public static final String S_URL_INTERNAL = "jdbc:default:connection";

  public static final String ROOT_PATH = "./data/";

  public static final int BUFFER_POOL_PAGE_NUM = 20;
}
