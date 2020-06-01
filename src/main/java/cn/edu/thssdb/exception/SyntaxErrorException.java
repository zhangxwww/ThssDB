package cn.edu.thssdb.exception;

import cn.edu.thssdb.utils.Global;

public class SyntaxErrorException extends RuntimeException {
	public int code() {
		return Global.SYNTAX_ERROR_EXCEPTION_CODE;
	}
}
