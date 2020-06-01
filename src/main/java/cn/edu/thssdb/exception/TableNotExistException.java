package cn.edu.thssdb.exception;

public class TableNotExistException extends Exception{
    @Override
    public String getMessage() {
        return "Exception: table doesn't exist!";
    }
}
