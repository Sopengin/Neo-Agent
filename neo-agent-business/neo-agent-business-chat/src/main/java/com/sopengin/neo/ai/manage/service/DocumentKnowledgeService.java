package com.sopengin.neo.ai.manage.service;

import com.sopengin.neo.ai.manage.model.DocumentRetrieveRequest;
import com.sopengin.neo.ai.manage.model.KnowledgeDocumentDescriptor;
import org.springframework.ai.document.Document;

import java.util.List;

/**
 * 服务层
 **/

public interface DocumentKnowledgeService {

    List<KnowledgeDocumentDescriptor> listRetrievableDocuments();

    List<Document> vectorSearch(DocumentRetrieveRequest request);

    List<Document> keywordSearch(DocumentRetrieveRequest request);

    List<Document> elevateToParentBlocks(List<Document> childDocuments, int maxChars);
}
