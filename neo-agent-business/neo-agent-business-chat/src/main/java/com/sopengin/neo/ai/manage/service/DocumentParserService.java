package com.sopengin.neo.ai.manage.service;

import com.sopengin.neo.enums.DocumentFileTypeEnum;
import com.sopengin.neo.ai.manage.support.DocumentAnalysisResult;

/**
 * 服务层
 **/

public interface DocumentParserService {

    DocumentAnalysisResult parse(byte[] bytes, String originalFileName, String mimeType, DocumentFileTypeEnum fileType);
}
