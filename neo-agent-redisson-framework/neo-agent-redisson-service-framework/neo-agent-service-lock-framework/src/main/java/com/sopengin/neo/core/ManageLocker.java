package com.sopengin.neo.core;

import com.sopengin.neo.servicelock.LockType;
import com.sopengin.neo.servicelock.ServiceLocker;
import com.sopengin.neo.servicelock.impl.RedissonFairLocker;
import com.sopengin.neo.servicelock.impl.RedissonReadLocker;
import com.sopengin.neo.servicelock.impl.RedissonReentrantLocker;
import com.sopengin.neo.servicelock.impl.RedissonWriteLocker;
import org.redisson.api.RedissonClient;

import java.util.HashMap;
import java.util.Map;

import static com.sopengin.neo.servicelock.LockType.Fair;
import static com.sopengin.neo.servicelock.LockType.Read;
import static com.sopengin.neo.servicelock.LockType.Reentrant;
import static com.sopengin.neo.servicelock.LockType.Write;

/**
 * 分布式锁 锁缓存
 **/
public class ManageLocker {

    private final Map<LockType, ServiceLocker> cacheLocker = new HashMap<>();

    public ManageLocker(RedissonClient redissonClient){
        cacheLocker.put(Reentrant,new RedissonReentrantLocker(redissonClient));
        cacheLocker.put(Fair,new RedissonFairLocker(redissonClient));
        cacheLocker.put(Write,new RedissonWriteLocker(redissonClient));
        cacheLocker.put(Read,new RedissonReadLocker(redissonClient));
    }

    public ServiceLocker getReentrantLocker(){
        return cacheLocker.get(Reentrant);
    }

    public ServiceLocker getFairLocker(){
        return cacheLocker.get(Fair);
    }

    public ServiceLocker getWriteLocker(){
        return cacheLocker.get(Write);
    }

    public ServiceLocker getReadLocker(){
        return cacheLocker.get(Read);
    }
}
