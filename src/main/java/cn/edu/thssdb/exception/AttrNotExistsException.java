package cn.edu.thssdb.exception;

import cn.edu.thssdb.utils.Global;

public class AttrNotExistsException extends RuntimeException {
    public int code() {
        return Global.ATTR_NOT_EXISTS_EXCEPTION_CODE;
    }
}
