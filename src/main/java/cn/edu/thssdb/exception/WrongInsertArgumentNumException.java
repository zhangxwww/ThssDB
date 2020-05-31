package cn.edu.thssdb.exception;

import cn.edu.thssdb.utils.Global;

public class WrongInsertArgumentNumException extends RuntimeException{
  @Override
  public String getMessage() {
    return "Exception: Wrong insertion argument number.";
  }

  public int code() {
    return Global.WRONG_INSERT_ARGUMENT_NUM_EXCEPTION_CODE;
  }
}
