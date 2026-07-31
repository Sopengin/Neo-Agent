package com.sopengin.neo.ai.manage.support;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 支撑组件
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DocumentStrategyPlanDraft {

    private String strategySnapshot;

    private String recommendReason;

    private List<DocumentStrategyStepDraft> parentSteps;

    private List<DocumentStrategyStepDraft> childSteps;
}
