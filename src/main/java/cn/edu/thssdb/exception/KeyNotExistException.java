package cn.edu.thssdb.exception;

import cn.edu.thssdb.utils.Global;

public class KeyNotExistException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Exception: key doesn't exist!";
    }

    public int code() {
        return Global.KEY_NOT_EXIST_EXCEPTION_CODE;
    }
}
