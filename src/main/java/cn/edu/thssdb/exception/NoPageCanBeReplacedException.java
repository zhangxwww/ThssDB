package cn.edu.thssdb.exception;

public class NoPageCanBeReplacedException extends Exception {
    @Override
    public String getMessage() {
        return "In Replacer.pickVictims. No enough pages can be replaced.";
    }
}
