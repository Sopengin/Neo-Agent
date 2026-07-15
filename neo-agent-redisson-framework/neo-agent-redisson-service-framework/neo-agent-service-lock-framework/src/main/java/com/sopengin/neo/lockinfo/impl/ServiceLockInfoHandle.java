package com.sopengin.neo.lockinfo.impl;

import com.sopengin.neo.lockinfo.AbstractLockInfoHandle;

/**
 * 锁信息实现(分布式锁)
 **/
public class ServiceLockInfoHandle extends AbstractLockInfoHandle {

    private static final String LOCK_PREFIX_NAME = "SERVICE_LOCK";

    @Override
    protected String getLockPrefixName() {
        return LOCK_PREFIX_NAME;
    }
}
