package cn.edu.thssdb.exception;

import cn.edu.thssdb.utils.Global;

public class TableNotExistsException extends RuntimeException {
    public int code() {
        return Global.TABLE_NOT_EXISTS_EXCEPTION_CODE;
    }
}
