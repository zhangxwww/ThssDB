package cn.edu.thssdb.exception;

import cn.edu.thssdb.utils.Global;

public class StringValueExceedLengthException extends RuntimeException {
    String attrName = null;
    int maxLength = 0;
    public StringValueExceedLengthException(String attr, int maxLength) {
        this.attrName = attr;
        this.maxLength = maxLength;
    }
    @Override
    public String getMessage() {
        return "Exception: The max length of attribute: "+attrName+" is "+ String.valueOf(maxLength);
    }

    public int code() {
        return Global.STRING_VALUE_EXCEED_MAX_LENGTH_EXCEPTION_CODE;
    }

}
