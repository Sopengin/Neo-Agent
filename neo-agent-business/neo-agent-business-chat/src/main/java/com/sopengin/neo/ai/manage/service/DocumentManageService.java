package com.sopengin.neo.ai.manage.service;

import com.sopengin.neo.ai.manage.dto.DocumentIndexBuildDto;
import com.sopengin.neo.ai.manage.dto.DocumentChunkQueryDto;
import com.sopengin.neo.ai.manage.dto.DocumentChunkDetailQueryDto;
import com.sopengin.neo.ai.manage.dto.DocumentDetailQueryDto;
import com.sopengin.neo.ai.manage.dto.DocumentDeleteDto;
import com.sopengin.neo.ai.manage.dto.DocumentPageQueryDto;
import com.sopengin.neo.ai.manage.dto.DocumentStrategyConfirmDto;
import com.sopengin.neo.ai.manage.dto.DocumentStrategyPlanQueryDto;
import com.sopengin.neo.ai.manage.dto.DocumentTaskLogQueryDto;
import com.sopengin.neo.ai.manage.dto.DocumentUploadDto;
import com.sopengin.neo.ai.manage.vo.DocumentIndexBuildVo;
import com.sopengin.neo.ai.manage.vo.DocumentChunkQueryVo;
import com.sopengin.neo.ai.manage.vo.DocumentChunkDetailVo;
import com.sopengin.neo.ai.manage.vo.DocumentDeleteVo;
import com.sopengin.neo.ai.manage.vo.DocumentListItemVo;
import com.sopengin.neo.ai.manage.vo.DocumentPageQueryVo;
import com.sopengin.neo.ai.manage.vo.DocumentStrategyConfirmVo;
import com.sopengin.neo.ai.manage.vo.DocumentStrategyPlanQueryVo;
import com.sopengin.neo.ai.manage.vo.DocumentTaskLogQueryVo;
import com.sopengin.neo.ai.manage.vo.DocumentUploadVo;
import org.springframework.web.multipart.MultipartFile;

/**
 * 服务层
 **/

public interface DocumentManageService {

    DocumentUploadVo upload(MultipartFile file, DocumentUploadDto dto);

    DocumentPageQueryVo queryDocumentPage(DocumentPageQueryDto dto);

    DocumentListItemVo queryDocumentDetail(DocumentDetailQueryDto dto);

    DocumentDeleteVo deleteDocument(DocumentDeleteDto dto);

    DocumentStrategyPlanQueryVo queryStrategyPlan(DocumentStrategyPlanQueryDto dto);

    DocumentStrategyConfirmVo confirmStrategy(DocumentStrategyConfirmDto dto);

    DocumentIndexBuildVo buildIndex(DocumentIndexBuildDto dto);

    DocumentChunkQueryVo queryDocumentChunks(DocumentChunkQueryDto dto);

    DocumentChunkDetailVo queryDocumentChunkDetail(DocumentChunkDetailQueryDto dto);

    DocumentTaskLogQueryVo queryTaskLogs(DocumentTaskLogQueryDto dto);
}
