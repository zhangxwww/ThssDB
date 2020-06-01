package cn.edu.thssdb.exception;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;

public class ColumnTypeWrongException extends RuntimeException {
    String wrongVal = null;
    ColumnType correct = null;
    public ColumnTypeWrongException(String wrongVal,ColumnType correct){
        this.wrongVal = wrongVal;
        this.correct = correct;
    }

    @Override
    public String getMessage() {
        return "Exception: Value = "+ wrongVal +" can't be parsed to "+ correct.toString();
    }

    public int code() {
        return Global.COLUMN_TYPE_WRONG_EXCEPTION_CODE;
    }
}
