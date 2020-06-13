package cn.edu.thssdb.exception;

import cn.edu.thssdb.utils.Global;

public class TableAlreadyExistsException extends RuntimeException {
	public int code() {
		return Global.TABLE_ALREADY_EXISTS_EXCEPTION_CODE;
	}
}