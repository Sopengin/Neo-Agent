package com.sopengin.neo.ai.manage.service;

import java.util.List;

/**
 * 服务层
 **/

public interface KnowledgeRouteIndexService {

    void refreshIfNeeded();

    List<RouteLexicalHit> search(String routingText, String entityType, int size);

    void deleteDocumentRoute(Long documentId);

    record RouteLexicalHit(
        String routeId,
        String entityCode,
        String entityType,
        Long documentId,
        String scopeCode,
        String topicCode,
        String documentName,
        double score
    ) {
    }
}
