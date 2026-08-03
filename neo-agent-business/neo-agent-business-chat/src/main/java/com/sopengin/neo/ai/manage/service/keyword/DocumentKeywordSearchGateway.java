package com.sopengin.neo.ai.manage.service.keyword;

import com.sopengin.neo.ai.manage.data.NeoAgentDocumentChunk;
import com.sopengin.neo.ai.manage.model.DocumentRetrieveRequest;
import org.springframework.ai.document.Document;

import java.util.List;

/**
 * 服务层
 **/

public interface DocumentKeywordSearchGateway {

    void indexChunks(List<NeoAgentDocumentChunk> chunkList);

    List<Document> search(DocumentRetrieveRequest request);

    void deleteByDocumentId(Long documentId);
}
