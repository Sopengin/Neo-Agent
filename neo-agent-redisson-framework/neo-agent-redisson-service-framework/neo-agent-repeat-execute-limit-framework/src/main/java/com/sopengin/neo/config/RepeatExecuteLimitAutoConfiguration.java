package com.sopengin.neo.config;

import com.sopengin.neo.constant.LockInfoType;
import com.sopengin.neo.handle.RedissonDataHandle;
import com.sopengin.neo.locallock.LocalLockCache;
import com.sopengin.neo.lockinfo.LockInfoHandle;
import com.sopengin.neo.lockinfo.factory.LockInfoHandleFactory;
import com.sopengin.neo.lockinfo.impl.RepeatExecuteLimitLockInfoHandle;
import com.sopengin.neo.repeatexecutelimit.aspect.RepeatExecuteLimitAspect;
import com.sopengin.neo.servicelock.factory.ServiceLockFactory;
import org.springframework.context.annotation.Bean;

/**
 * 防重复幂等配置
 **/
public class RepeatExecuteLimitAutoConfiguration {

    @Bean(LockInfoType.REPEAT_EXECUTE_LIMIT)
    public LockInfoHandle repeatExecuteLimitHandle(){
        return new RepeatExecuteLimitLockInfoHandle();
    }

    @Bean
    public RepeatExecuteLimitAspect repeatExecuteLimitAspect(LocalLockCache localLockCache,
                                                             LockInfoHandleFactory lockInfoHandleFactory,
                                                             ServiceLockFactory serviceLockFactory,
                                                             RedissonDataHandle redissonDataHandle){
        return new RepeatExecuteLimitAspect(localLockCache, lockInfoHandleFactory,serviceLockFactory,redissonDataHandle);
    }
}
