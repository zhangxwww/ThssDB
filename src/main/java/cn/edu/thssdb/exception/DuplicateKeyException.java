package cn.edu.thssdb.exception;

import cn.edu.thssdb.utils.Global;

public class DuplicateKeyException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Exception: insertion caused duplicated keys!";
    }

    public int code() {
        return Global.DUPLICATE_KEY_EXCEPTION_CODE;
    }
}
