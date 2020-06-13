package cn.edu.thssdb.pagefile;

public interface PageFileConst {
    public static final int PAGE_SIZE = 1024;
    public static final int NUM_FILE_PAGES = 1024;
    public static final int INVALID_PAGEID = -1;
    public static final int FIRST_PAGEID = 0;
    public static final int MAX_NAME_LEN = 50;
    public static final int EMPTY_SLOT = -1;
    public static final int MAX_COLUMN_SIZE = 1001;
    public static final int MAX_TUPLE_SIZE = 1004;
}