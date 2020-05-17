package cn.edu.thssdb.exception;

public class InsertLackPrimaryKeyException extends RuntimeException{
  @Override
  public String getMessage() {
    return "Exception: Insertion attributes must include primary key.";
  }
}
