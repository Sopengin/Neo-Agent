package com.sopengin.neo.ai.manage.data;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import com.sopengin.neo.database.data.BaseTableData;

/**
 * 数据实体
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
@TableName("neo_agent_document_chunk")
@EqualsAndHashCode(callSuper = true)
public class NeoAgentDocumentChunk extends BaseTableData {

    @TableId(value = "id", type = IdType.INPUT)
    private Long id;

    private Long documentId;

    private Long taskId;

    private Long planId;

    private Long parentBlockId;

    private Integer chunkNo;

    private Integer sourceType;

    private String sectionPath;

    private Long structureNodeId;

    private Integer structureNodeType;

    private String canonicalPath;

    private Integer itemIndex;

    private String chunkText;

    private Integer charCount;

    private Integer tokenCount;

    private Integer vectorStatus;

    private Integer vectorStoreType;

    private String vectorId;
}
