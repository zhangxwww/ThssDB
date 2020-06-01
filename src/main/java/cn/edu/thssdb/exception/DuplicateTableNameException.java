package cn.edu.thssdb.exception;

import cn.edu.thssdb.utils.Global;

public class DuplicateTableNameException extends RuntimeException {
    public int code() {
        return Global.DUPLICATE_TABLE_NAME_EXCEPTION_CODE;
    }
}
