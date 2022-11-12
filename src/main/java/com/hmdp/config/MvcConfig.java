package com.hmdp.config;

import com.hmdp.utils.LoginInterceptor;
import com.hmdp.utils.RefreshTokenInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

@Configuration
public class MvcConfig implements WebMvcConfigurer {
    @Resource
    StringRedisTemplate stringRedisTemplate;
    /*
    配置文件，这里注入了stringRedisTemplate再传给拦截器，因为拦截器本身是在生成容器之前就执行的，我们必须找到
    更早的时间点，传入这个temp，因此我们在配置的时候就传进去，然后用构造器（见RefreshTokenInterceptor）传参
     */
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new LoginInterceptor()).
                excludePathPatterns(
                        "/shop/**",
                        "/voucher/**",
                        "/shop-type/**",
                        "/upload/**",
                        "/blog/hot",
                        "/user/code",
                        "/user/login"
                ).order(1);
    registry.addInterceptor(new RefreshTokenInterceptor(stringRedisTemplate)).order(0);
    }
}
