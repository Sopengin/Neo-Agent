package com.sopengin.neo.ai.manage.support;

/**
 * 支撑组件
 **/

public record DocumentStructureLogicalLine(
    int lineNo,
    int sourceLineNo,
    int segmentIndex,
    int indentLevel,
    String rawText,
    String normalizedText
) {
}
