package cn.edu.thssdb.exception;

import cn.edu.thssdb.utils.Global;

public class NotNullAttributeAssignedNullException extends RuntimeException {
    String attrName = null;
    public NotNullAttributeAssignedNullException(String attr) {
        this.attrName = attr;
    }
    @Override
    public String getMessage() {
        return "Exception: NOT NULL Attribute:"+attrName+" is assigned to null.";
    }

    public int code() {
        return Global.NOT_NULL_ATTRIBUTE_ASSIGNED_NULL_EXCEPTION_CODE;
    }
}
