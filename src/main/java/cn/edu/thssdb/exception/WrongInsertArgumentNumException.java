package cn.edu.thssdb.exception;

public class WrongInsertArgumentNumException extends RuntimeException{
  @Override
  public String getMessage() {
    return "Exception: Wrong insertion argument number.";
  }
}
