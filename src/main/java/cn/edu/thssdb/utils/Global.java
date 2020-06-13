package cn.edu.thssdb.utils;

public class Global {

	public static String USERNAME = "username";
	public static String PASSWORD = "password";

	public static int fanout = 129;

	public static final int SUCCESS_CODE = 0;
	public static final int FAILURE_CODE = -1;

	public static final int WRONG_INSERT_ARGUMENT_NUM_EXCEPTION_CODE = 400;
	public static final int PRIMARY_KEY_REQUIRED_EXCEPTION_CODE = 401;
	public static final int ATTR_NOT_EXISTS_EXCEPTION_CODE = 402;
	public static final int AMBIGUOUS_COLUMN_EXCEPTION_CODE = 403;
	public static final int TABLE_NOT_EXISTS_EXCEPTION_CODE = 404;
	public static final int DUPLICATE_KEY_EXCEPTION_CODE = 405;
	public static final int KEY_NOT_EXIST_EXCEPTION_CODE = 406;
	public static final int DUPLICATE_TABLE_NAME_EXCEPTION_CODE = 407;
	public static final int NOT_NULL_ATTRIBUTE_ASSIGNED_NULL_EXCEPTION_CODE = 408;
	public static final int COLUMN_TYPE_WRONG_EXCEPTION_CODE = 409;
	public static final int STRING_VALUE_EXCEED_MAX_LENGTH_EXCEPTION_CODE = 410;
	public static final int SYNTAX_ERROR_EXCEPTION_CODE = 411;
    public static final int TABLE_ALREADY_EXISTS_EXCEPTION_CODE = 412;

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
