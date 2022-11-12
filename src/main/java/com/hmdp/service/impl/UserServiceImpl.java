package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("phone number format wrong！");
        }
        // 2. 生成验证码
        String code = RandomUtil.randomNumbers(6);
        // 3. 保存验证码到 redis instead of session, 加前缀方便区分与其他业务
        // session.setAttribute("code",code);
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY + phone, code, RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);
        // 4.发送验证码 目前只是模拟发送，并没有真的发送，只是打印了日志返回成功；以后可以考虑阿里云等发送服务
        log.debug("verification code sent successful，code：{}", code);

        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1.校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("phone number format wrong！");
        }
        // 2.获取然后校验验证码，这里key使用
        Object cacheCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.toString().equals(code)){
            return Result.fail("wrong verification code");
        }
        // 3. 根据手机号查询用户
        User user = query().eq("phone", phone).one();
        if (user == null) {
            user =  createUserWithPhone(phone);
        }
        // 4.保存用户信息到redis中, 只保存部分信息，节省内存、隐藏用户敏感信息
        //session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        // 4.1.随机生成token，作为登录令牌
        String token = UUID.randomUUID().toString(true);
        // 4.2.将User对象转为HashMap存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        // 这里为了long转string格式才自定义的，自己写工具也行，todo： make bean converter util
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        // 4.3.存储， 加入前缀进行区分
        String tokenKey = RedisConstants.LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        // 4.4.设置token有效期, 暂定和session时长一致， 30min
        stringRedisTemplate.expire(tokenKey, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);

        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomNumbers(10));
        // save user to table
        save(user);
        return user;
    }
}
