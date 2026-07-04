package com.sopengin.neo.exception;

import com.sopengin.neo.common.ApiResponse;
import com.sopengin.neo.enums.BaseCode;
import lombok.Data;

/**
 * 异常类
 **/

@Data
public class NeoAgentFrameException extends BaseException {

	private Integer code;

	private String message;

	public NeoAgentFrameException() {
		super();
	}

	public NeoAgentFrameException(String message) {
		super(message);
	}

	public NeoAgentFrameException(String code, String message) {
		super(message);
		this.code = Integer.parseInt(code);
		this.message = message;
	}

	public NeoAgentFrameException(Integer code, String message) {
		super(message);
		this.code = code;
		this.message = message;
	}

	public NeoAgentFrameException(BaseCode baseCode) {
		super(baseCode.getMsg());
		this.code = baseCode.getCode();
		this.message = baseCode.getMsg();
	}

	public NeoAgentFrameException(ApiResponse apiResponse) {
		super(apiResponse.getMessage());
		this.code = apiResponse.getCode();
		this.message = apiResponse.getMessage();
	}

	public NeoAgentFrameException(Throwable cause) {
		super(cause);
	}

	public NeoAgentFrameException(String message, Throwable cause) {
		super(message, cause);
		this.message = message;
	}

	public NeoAgentFrameException(Integer code, String message, Throwable cause) {
		super(message, cause);
		this.code = code;
		this.message = message;
	}
}
