package com.sopengin.neo.ai.chatagent.data;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
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
@TableName("neo_agent_chat_dialogue")
@EqualsAndHashCode(callSuper = true)
public class NeoAgentChatDialogue extends BaseTableData {

    @TableId(value = "id", type = IdType.INPUT)
    private Long id;

    @TableField("dialogue_code")
    private String conversationId;

    @TableField("dialogue_stage")
    private Integer sessionStatus;

    @TableField("chat_mode")
    private Integer chatMode;

    @TableField("selected_document_id")
    private Long selectedDocumentId;

    @TableField("selected_document_name")
    private String selectedDocumentName;
}
