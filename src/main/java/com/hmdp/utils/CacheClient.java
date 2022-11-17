package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public <R,ID> R queryByIdWithoutCachePenetration(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1. try to get from cache
        String objectJson = stringRedisTemplate.opsForValue().get(key);
        // in cache and valid
        if (StrUtil.isNotBlank(objectJson)) {
            return JSONUtil.toBean(objectJson, type);
        }
        // 2. in cache(not null) but is "", which means invalid id
        if ("".equals(objectJson)) {
            return null;
        }
        // 3. not in cache -> get from database and build cache
        R r = dbFallback.apply(id);
        if (r == null) {
            // write null into redis
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 3. store in cache
        long ttl = RandomUtil.randomLong(RedisConstants.CACHE_SHOP_TTL, RedisConstants.CACHE_SHOP_MAX_TTL);
        this.set(key, r, ttl, TimeUnit.MINUTES);
        return r;
    }

     /*
    avoid cache breakdown / hot key invalid
     */
     public <R,ID> R queryByIdByMutexWithoutCacheBreakdown(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
         String key = keyPrefix + id;
         // 1. try to get from cache
         String objectJson = stringRedisTemplate.opsForValue().get(key);
         // in cache and valid
         if (StrUtil.isNotBlank(objectJson)) {
             return JSONUtil.toBean(objectJson, type);
         }
         // 2. in cache(not null) but is "", which means invalid id
         if ("".equals(objectJson)) {
             return null;
         }
         // 3. not in cache -> get from database and build cache
         String lockKey = RedisConstants.LOCK_KEY + keyPrefix + id;
         R r = null;
         try {
             boolean isLock = tryLock(lockKey);
             if (!isLock) {
                 // sleep and retry
                 Thread.sleep(50);
                 return queryByIdByMutexWithoutCacheBreakdown(keyPrefix, id, type, dbFallback, time, unit);
             }
             r = dbFallback.apply(id);
             if (r == null) {
                 // write null into redis
                 stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                 return null;
             }
             // store in cache
             long ttl = RandomUtil.randomLong(RedisConstants.CACHE_SHOP_TTL, RedisConstants.CACHE_SHOP_MAX_TTL);
             this.set(key, r, ttl, TimeUnit.MINUTES);
         } catch (Exception e) {
             throw new RuntimeException(e);
         } finally {
             unlock(lockKey);
         }
         return r;
     }

    /*
    using redis setnx to achieve the goal of mutex lock
     */
    private boolean tryLock(String key) {
        Boolean result = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(result);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
