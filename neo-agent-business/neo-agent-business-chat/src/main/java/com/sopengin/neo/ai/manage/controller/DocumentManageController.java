package com.sopengin.neo.ai.manage.controller;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
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
import com.sopengin.neo.ai.manage.service.DocumentManageService;
import com.sopengin.neo.ai.manage.vo.DocumentIndexBuildVo;
import com.sopengin.neo.ai.manage.vo.DocumentChunkQueryVo;
import com.sopengin.neo.ai.manage.vo.DocumentChunkDetailVo;
import com.sopengin.neo.ai.manage.vo.DocumentListItemVo;
import com.sopengin.neo.ai.manage.vo.DocumentDeleteVo;
import com.sopengin.neo.ai.manage.vo.DocumentPageQueryVo;
import com.sopengin.neo.ai.manage.vo.DocumentStrategyConfirmVo;
import com.sopengin.neo.ai.manage.vo.DocumentStrategyPlanQueryVo;
import com.sopengin.neo.ai.manage.vo.DocumentTaskLogQueryVo;
import com.sopengin.neo.ai.manage.vo.DocumentUploadVo;
import com.sopengin.neo.common.ApiResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

/**
 * 控制层
 **/

@RestController
@RequestMapping("/manage/document")
public class DocumentManageController {

    private final DocumentManageService documentManageService;

    public DocumentManageController(DocumentManageService documentManageService) {
        this.documentManageService = documentManageService;
    }

    @Operation(summary = "上传文档并投递解析任务")
    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ApiResponse<DocumentUploadVo> upload(@RequestPart("file") MultipartFile file,
                                                @Valid @RequestPart(value = "meta", required = false) DocumentUploadDto dto) {

        return ApiResponse.ok(documentManageService.upload(file, dto == null ? new DocumentUploadDto() : dto));
    }

    @Operation(summary = "分页查询文档列表")
    @PostMapping("/page/query")
    public ApiResponse<DocumentPageQueryVo> queryDocumentPage(@Valid @RequestBody DocumentPageQueryDto dto) {
        return ApiResponse.ok(documentManageService.queryDocumentPage(dto));
    }

    @Operation(summary = "查询文档详情")
    @PostMapping("/detail/query")
    public ApiResponse<DocumentListItemVo> queryDocumentDetail(@Valid @RequestBody DocumentDetailQueryDto dto) {
        return ApiResponse.ok(documentManageService.queryDocumentDetail(dto));
    }

    @Operation(summary = "删除文档及其关联数据")
    @PostMapping("/delete")
    public ApiResponse<DocumentDeleteVo> deleteDocument(@Valid @RequestBody DocumentDeleteDto dto) {
        return ApiResponse.ok(documentManageService.deleteDocument(dto));
    }

    @Operation(summary = "查询文档策略推荐结果")
    @PostMapping("/strategy/plan/query")
    public ApiResponse<DocumentStrategyPlanQueryVo> queryStrategyPlan(@Valid @RequestBody DocumentStrategyPlanQueryDto dto) {
        return ApiResponse.ok(documentManageService.queryStrategyPlan(dto));
    }

    @Operation(summary = "确认文档策略方案")
    @PostMapping("/strategy/confirm")
    public ApiResponse<DocumentStrategyConfirmVo> confirmStrategy(@Valid @RequestBody DocumentStrategyConfirmDto dto) {
        return ApiResponse.ok(documentManageService.confirmStrategy(dto));
    }

    @Operation(summary = "执行文档索引构建")
    @PostMapping("/index/build")
    public ApiResponse<DocumentIndexBuildVo> buildIndex(@Valid @RequestBody DocumentIndexBuildDto dto) {
        return ApiResponse.ok(documentManageService.buildIndex(dto));
    }

    @Operation(summary = "查询文档 chunk 列表")
    @PostMapping("/chunk/query")
    public ApiResponse<DocumentChunkQueryVo> queryDocumentChunks(@Valid @RequestBody DocumentChunkQueryDto dto) {
        return ApiResponse.ok(documentManageService.queryDocumentChunks(dto));
    }

    @Operation(summary = "查询单个文档 chunk 详情")
    @PostMapping("/chunk/detail/query")
    public ApiResponse<DocumentChunkDetailVo> queryDocumentChunkDetail(@Valid @RequestBody DocumentChunkDetailQueryDto dto) {
        return ApiResponse.ok(documentManageService.queryDocumentChunkDetail(dto));
    }

    @Operation(summary = "查询任务执行日志")
    @PostMapping("/task/log/query")
    public ApiResponse<DocumentTaskLogQueryVo> queryTaskLogs(@Valid @RequestBody DocumentTaskLogQueryDto dto) {
        return ApiResponse.ok(documentManageService.queryTaskLogs(dto));
    }

}
