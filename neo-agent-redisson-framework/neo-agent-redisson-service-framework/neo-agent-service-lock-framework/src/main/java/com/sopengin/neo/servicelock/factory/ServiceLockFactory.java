package com.sopengin.neo.servicelock.factory;

import com.sopengin.neo.core.ManageLocker;
import com.sopengin.neo.servicelock.LockType;
import com.sopengin.neo.servicelock.ServiceLocker;
import lombok.AllArgsConstructor;

/**
 * 分布式锁类型工厂
 **/
@AllArgsConstructor
public class ServiceLockFactory {

    private final ManageLocker manageLocker;

    public ServiceLocker getLock(LockType lockType){
        ServiceLocker lock;
        switch (lockType) {
            case Fair:
                lock = manageLocker.getFairLocker();
                break;
            case Write:
                lock = manageLocker.getWriteLocker();
                break;
            case Read:
                lock = manageLocker.getReadLocker();
                break;
            default:
                lock = manageLocker.getReentrantLocker();
                break;
        }
        return lock;
    }
}
