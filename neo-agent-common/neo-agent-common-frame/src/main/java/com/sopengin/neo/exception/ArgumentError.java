package com.sopengin.neo.exception;

import lombok.Data;

/**
 * 异常类
 **/

@Data
public class ArgumentError {

	private String argumentName;

	private String message;
}
