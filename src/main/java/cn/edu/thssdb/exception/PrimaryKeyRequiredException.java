package cn.edu.thssdb.exception;

import cn.edu.thssdb.utils.Global;

public class PrimaryKeyRequiredException extends RuntimeException {
    public int code() {
        return Global.PRIMARY_KEY_REQUIRED_EXCEPTION_CODE;
    }
}
