package com.sopengin.neo.config;

import com.sopengin.neo.constant.LockInfoType;
import com.sopengin.neo.core.ManageLocker;
import com.sopengin.neo.lockinfo.LockInfoHandle;
import com.sopengin.neo.lockinfo.factory.LockInfoHandleFactory;
import com.sopengin.neo.lockinfo.impl.ServiceLockInfoHandle;
import com.sopengin.neo.servicelock.aspect.ServiceLockAspect;
import com.sopengin.neo.servicelock.factory.ServiceLockFactory;
import com.sopengin.neo.util.ServiceLockTool;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Bean;

/**
 * 分布式锁 配置
 **/
public class ServiceLockAutoConfiguration {

    @Bean(LockInfoType.SERVICE_LOCK)
    public LockInfoHandle serviceLockInfoHandle(){
        return new ServiceLockInfoHandle();
    }

    @Bean
    public ManageLocker manageLocker(RedissonClient redissonClient){
        return new ManageLocker(redissonClient);
    }

    @Bean
    public ServiceLockFactory serviceLockFactory(ManageLocker manageLocker){
        return new ServiceLockFactory(manageLocker);
    }

    @Bean
    public ServiceLockAspect serviceLockAspect(LockInfoHandleFactory lockInfoHandleFactory,ServiceLockFactory serviceLockFactory){
        return new ServiceLockAspect(lockInfoHandleFactory,serviceLockFactory);
    }

    @Bean
    public ServiceLockTool serviceLockUtil(LockInfoHandleFactory lockInfoHandleFactory,ServiceLockFactory serviceLockFactory){
        return new ServiceLockTool(lockInfoHandleFactory,serviceLockFactory);
    }
}
