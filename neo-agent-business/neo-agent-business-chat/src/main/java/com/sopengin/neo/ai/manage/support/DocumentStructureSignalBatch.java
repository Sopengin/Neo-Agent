package com.sopengin.neo.ai.manage.support;

import java.util.List;

/**
 * 支撑组件
 **/

public record DocumentStructureSignalBatch(
    List<String> contextLines,
    List<DocumentStructureSignal> signals
) {
}
