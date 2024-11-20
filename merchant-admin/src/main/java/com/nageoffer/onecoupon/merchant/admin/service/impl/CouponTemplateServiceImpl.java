/*
 * 牛券（oneCoupon）优惠券平台项目
 *
 * 版权所有 (C) [2024-至今] [山东流年网络科技有限公司]
 *
 * 保留所有权利。
 *
 * 1. 定义和解释
 *    本文件（包括其任何修改、更新和衍生内容）是由[山东流年网络科技有限公司]及相关人员开发的。
 *    "软件"指的是与本文件相关的任何代码、脚本、文档和相关的资源。
 *
 * 2. 使用许可
 *    本软件的使用、分发和解释均受中华人民共和国法律的管辖。只有在遵守以下条件的前提下，才允许使用和分发本软件：
 *    a. 未经[山东流年网络科技有限公司]的明确书面许可，不得对本软件进行修改、复制、分发、出售或出租。
 *    b. 任何未授权的复制、分发或修改都将被视为侵犯[山东流年网络科技有限公司]的知识产权。
 *
 * 3. 免责声明
 *    本软件按"原样"提供，没有任何明示或暗示的保证，包括但不限于适销性、特定用途的适用性和非侵权性的保证。
 *    在任何情况下，[山东流年网络科技有限公司]均不对任何直接、间接、偶然、特殊、典型或间接的损害（包括但不限于采购替代商品或服务；使用、数据或利润损失）承担责任。
 *
 * 4. 侵权通知与处理
 *    a. 如果[山东流年网络科技有限公司]发现或收到第三方通知，表明存在可能侵犯其知识产权的行为，公司将采取必要的措施以保护其权利。
 *    b. 对于任何涉嫌侵犯知识产权的行为，[山东流年网络科技有限公司]可能要求侵权方立即停止侵权行为，并采取补救措施，包括但不限于删除侵权内容、停止侵权产品的分发等。
 *    c. 如果侵权行为持续存在或未能得到妥善解决，[山东流年网络科技有限公司]保留采取进一步法律行动的权利，包括但不限于发出警告信、提起民事诉讼或刑事诉讼。
 *
 * 5. 其他条款
 *    a. [山东流年网络科技有限公司]保留随时修改这些条款的权利。
 *    b. 如果您不同意这些条款，请勿使用本软件。
 *
 * 未经[山东流年网络科技有限公司]的明确书面许可，不得使用此文件的任何部分。
 *
 * 本软件受到[山东流年网络科技有限公司]及其许可人的版权保护。
 */

package com.nageoffer.onecoupon.merchant.admin.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.digest.DigestUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.mzt.logapi.context.LogRecordContext;
import com.mzt.logapi.starter.annotation.LogRecord;
import com.nageoffer.onecoupon.framework.exception.ClientException;
import com.nageoffer.onecoupon.merchant.admin.common.constant.MerchantAdminRedisConstant;
import com.nageoffer.onecoupon.merchant.admin.common.context.UserContext;
import com.nageoffer.onecoupon.merchant.admin.common.enums.CouponTemplateStatusEnum;
import com.nageoffer.onecoupon.merchant.admin.common.enums.DiscountTargetEnum;
import com.nageoffer.onecoupon.merchant.admin.common.enums.DiscountTypeEnum;
import com.nageoffer.onecoupon.merchant.admin.dao.entity.CouponTemplateDO;
import com.nageoffer.onecoupon.merchant.admin.dao.mapper.CouponTemplateMapper;
import com.nageoffer.onecoupon.merchant.admin.dto.req.CouponTemplateSaveReqDTO;
import com.nageoffer.onecoupon.merchant.admin.dto.resp.CouponTemplateQueryRespDTO;
import com.nageoffer.onecoupon.merchant.admin.service.CouponTemplateService;
import com.nageoffer.onecoupon.merchant.admin.service.basics.chain.MerchantAdminChainContext;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.nageoffer.onecoupon.merchant.admin.common.enums.ChainBizMarkEnum.MERCHANT_ADMIN_CREATE_COUPON_TEMPLATE_KEY;

/**
 * 优惠券模板业务逻辑实现层
 */

@Service
@RequiredArgsConstructor
public class CouponTemplateServiceImpl extends ServiceImpl<CouponTemplateMapper, CouponTemplateDO> implements CouponTemplateService {

    private final CouponTemplateMapper couponTemplateMapper;
    private final MerchantAdminChainContext merchantAdminChainContext;
    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;

    @LogRecord(
            success = """
                创建优惠券：{{#requestParam.name}}， \
                优惠对象：{COMMON_ENUM_PARSE{'DiscountTargetEnum' + '_' + #requestParam.target}}， \\
                优惠类型：{COMMON_ENUM_PARSE{'DiscountTypeEnum' + '_' + #requestParam.type}}， \\
                库存数量：{{#requestParam.stock}}， \
                优惠商品编码：{{#requestParam.goods}}， \
                有效期开始时间：{{#requestParam.validStartTime}}， \
                有效期结束时间：{{#requestParam.validEndTime}}， \
                领取规则：{{#requestParam.receiveRule}}， \
                消耗规则：{{#requestParam.consumeRule}};
                """,
            type = "CouponTemplate",
            bizNo = "{{#bizNo}}",
            extra = "{{#requestParam.toString()}}"
    )
    @Override
    public void createCouponTemplate(CouponTemplateSaveReqDTO requestParam) {
//        // 获取分布式锁标识
//        String lockKey = String.format("no-duplicate-submit:path:%s:currentUserId:%s:md5:%s", getServletPath(), getCurrentUserId(), calcArgsMD5(requestParam));
//        RLock lock = redissonClient.getLock(lockKey);
//
//        // 尝试获取锁，获取锁失败就意味着已经重复提交，直接抛出异常
//        if (!lock.tryLock()) {
//            throw new ClientException("请勿短时间内重复提交优惠券模板");
//        }

//        try {
            // 执行常规业务代码
        // 通过责任链验证请求参数是否正确
        merchantAdminChainContext.handler(MERCHANT_ADMIN_CREATE_COUPON_TEMPLATE_KEY.name(), requestParam);

        // 新增优惠券模板信息到数据库
        CouponTemplateDO couponTemplateDO = BeanUtil.toBean(requestParam, CouponTemplateDO.class);
        couponTemplateDO.setStatus(CouponTemplateStatusEnum.ACTIVE.getStatus());
        couponTemplateDO.setShopNumber(UserContext.getShopNumber());
        couponTemplateMapper.insert(couponTemplateDO);

        // 因为模板 ID 是运行中生成的，@LogRecord 默认拿不到，所以我们需要手动设置
        LogRecordContext.putVariable("bizNo", couponTemplateDO.getId());

        // 缓存预热：通过将数据库的记录序列化成 JSON 字符串放入 Redis 缓存
        CouponTemplateQueryRespDTO actualRespDTO = BeanUtil.toBean(couponTemplateDO, CouponTemplateQueryRespDTO.class);
        Map<String, Object> cacheTargetMap = BeanUtil.beanToMap(actualRespDTO, false, true);
        Map<String, String> actualCacheTargetMap = cacheTargetMap.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue() != null ? entry.getValue().toString() : ""
                ));
        String couponTemplateCacheKey = String.format(MerchantAdminRedisConstant.COUPON_TEMPLATE_KEY, couponTemplateDO.getId());

        // 通过 LUA 脚本执行设置 Hash 数据以及设置过期时间
        String luaScript = "redis.call('HMSET', KEYS[1], unpack(ARGV, 1, #ARGV - 1)) " +
                "redis.call('EXPIREAT', KEYS[1], ARGV[#ARGV])";

        // 用于指定具体的键key
        List<String> keys = Collections.singletonList(couponTemplateCacheKey);
        // 设置哈希表中的键值对 预留一个参数位置作为过期时间
        List<String> args = new ArrayList<>(actualCacheTargetMap.size() * 2 + 1);
        actualCacheTargetMap.forEach((key, value) -> {
            args.add(key);
            args.add(value);
        });

        // 优惠券活动过期时间转换为秒级别的 Unix 时间戳
        args.add(String.valueOf(couponTemplateDO.getValidEndTime().getTime() / 1000));

        // 执行 LUA 脚本 保证原子性
        stringRedisTemplate.execute(
                new DefaultRedisScript<>(luaScript, Long.class),
                keys,
                args.toArray()
        );
//
//        } finally {
//            lock.unlock();
//        }
    }

//    /**
//     * @return 获取当前线程上下文 ServletPath
//     */
//    private String getServletPath() {
//        ServletRequestAttributes sra = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
//        return sra.getRequest().getServletPath();
//    }
//
//    /**
//     * @return 当前操作用户 ID
//     */
//    private String getCurrentUserId() {
//        // 用户属于非核心功能，这里先通过模拟的形式代替。后续如果需要后管展示，会重构该代码
//        return "1810518709471555585";
//    }
//
//    /**
//     * @return joinPoint md5
//     */
//    private String calcArgsMD5(CouponTemplateSaveReqDTO requestParam) {
//        return DigestUtil.md5Hex(JSON.toJSONBytes(requestParam));
//    }
}
