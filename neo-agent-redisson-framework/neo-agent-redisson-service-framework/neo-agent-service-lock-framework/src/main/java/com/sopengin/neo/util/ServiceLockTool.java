package com.sopengin.neo.util;

import com.sopengin.neo.constant.LockInfoType;
import com.sopengin.neo.lockinfo.LockInfoHandle;
import com.sopengin.neo.lockinfo.factory.LockInfoHandleFactory;
import com.sopengin.neo.servicelock.LockType;
import com.sopengin.neo.servicelock.ServiceLocker;
import com.sopengin.neo.servicelock.factory.ServiceLockFactory;
import com.sopengin.neo.servicelock.info.LockTimeOutStrategy;
import lombok.AllArgsConstructor;
import org.redisson.api.RLock;

import java.util.concurrent.TimeUnit;

/**
 * 分布式锁 方法类型操作
 **/
@AllArgsConstructor
public class ServiceLockTool {

    private final LockInfoHandleFactory lockInfoHandleFactory;

    private final ServiceLockFactory serviceLockFactory;

    public void execute(TaskRun taskRun,String name,String [] keys) {
        execute(taskRun,name,keys,20);
    }

    public void execute(TaskRun taskRun,String name,String [] keys,long waitTime){
        execute(LockType.Reentrant,taskRun,name,keys,waitTime);
    }

    public void execute(LockType lockType,TaskRun taskRun,String name,String [] keys) {
        execute(lockType,taskRun,name,keys,20);
    }

    public void execute(LockType lockType,TaskRun taskRun,String name,String [] keys,long waitTime) {
        LockInfoHandle lockInfoHandle = lockInfoHandleFactory.getLockInfoHandle(LockInfoType.SERVICE_LOCK);
        String lockName = lockInfoHandle.simpleGetLockName(name,keys);
        ServiceLocker lock = serviceLockFactory.getLock(lockType);
        boolean result = lock.tryLock(lockName, TimeUnit.SECONDS, waitTime);
        if (result) {
            try {
                taskRun.run();
            }finally {
                lock.unlock(lockName);
            }
        }else {
            LockTimeOutStrategy.FAIL.handler(lockName);
        }
    }

    public <T> T submit(TaskCall<T> taskCall,String name,String [] keys){
        LockInfoHandle lockInfoHandle = lockInfoHandleFactory.getLockInfoHandle(LockInfoType.SERVICE_LOCK);
        String lockName = lockInfoHandle.simpleGetLockName(name,keys);
        ServiceLocker lock = serviceLockFactory.getLock(LockType.Reentrant);
        boolean result = lock.tryLock(lockName, TimeUnit.SECONDS, 30);
        if (result) {
            try {
                return taskCall.call();
            }finally {
                lock.unlock(lockName);
            }
        }else {
            LockTimeOutStrategy.FAIL.handler(lockName);
        }
        return null;
    }

    public RLock getLock(LockType lockType, String name, String [] keys) {
        LockInfoHandle lockInfoHandle = lockInfoHandleFactory.getLockInfoHandle(LockInfoType.SERVICE_LOCK);
        String lockName = lockInfoHandle.simpleGetLockName(name,keys);
        ServiceLocker lock = serviceLockFactory.getLock(lockType);
        return lock.getLock(lockName);
    }

    public RLock getLock(LockType lockType, String lockName) {
        ServiceLocker lock = serviceLockFactory.getLock(lockType);
        return lock.getLock(lockName);
    }
}
