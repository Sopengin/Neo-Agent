package com.sopengin.neo.ai.chatagent.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import com.sopengin.neo.ai.chatagent.data.NeoAgentChatDialogue;

/**
 * Mapper层
 **/

@Mapper
public interface NeoAgentChatDialogueMapper extends BaseMapper<NeoAgentChatDialogue> {
}
