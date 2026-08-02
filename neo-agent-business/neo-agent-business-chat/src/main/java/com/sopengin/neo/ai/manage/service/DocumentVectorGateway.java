package com.sopengin.neo.ai.manage.service;

import com.sopengin.neo.ai.manage.data.NeoAgentDocumentChunk;

import java.util.List;

/**
 * 服务层
 **/

public interface DocumentVectorGateway {

    void vectorize(List<NeoAgentDocumentChunk> chunkList);

    void deleteByDocumentId(Long documentId);
}
