package cn.edu.thssdb.exception;

import cn.edu.thssdb.utils.Global;

public class AmbiguousColumnException extends RuntimeException {
    public int code() {
        return Global.AMBIGUOUS_COLUMN_EXCEPTION_CODE;
    }
}
