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

package com.nageoffer.onecoupon.engine.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Singleton;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.extension.toolkit.SqlHelper;
import com.nageoffer.onecoupon.engine.common.constant.EngineRedisConstant;
import com.nageoffer.onecoupon.engine.common.context.UserContext;
import com.nageoffer.onecoupon.engine.common.enums.RedisStockDecrementErrorEnum;
import com.nageoffer.onecoupon.engine.dao.entity.UserCouponDO;
import com.nageoffer.onecoupon.engine.dao.mapper.CouponTemplateMapper;
import com.nageoffer.onecoupon.engine.dao.mapper.UserCouponMapper;
import com.nageoffer.onecoupon.engine.dto.req.CouponTemplateQueryReqDTO;
import com.nageoffer.onecoupon.engine.dto.req.CouponTemplateRedeemReqDTO;
import com.nageoffer.onecoupon.engine.dto.resp.CouponTemplateQueryRespDTO;
import com.nageoffer.onecoupon.engine.mq.event.UserCouponDelayCloseEvent;
import com.nageoffer.onecoupon.engine.mq.producer.UserCouponDelayCloseProducer;
import com.nageoffer.onecoupon.engine.service.CouponTemplateService;
import com.nageoffer.onecoupon.engine.service.UserCouponService;
import com.nageoffer.onecoupon.engine.toolkit.StockDecrementReturnCombinedUtil;
import com.nageoffer.onecoupon.framework.exception.ClientException;
import com.nageoffer.onecoupon.framework.exception.ServiceException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Date;

/**
 * 用户优惠券业务逻辑实现层
 * 总结 用户领券模块 首先对优惠券是否存在 是否超时 然后通过lua脚本判断库存是否充足，用户是否超领，并存储领取次数信息 然后使用编程式事务进行数据库修改 保存用户领券时间信息并设置过期时间
 * 优化 对于用户领券 在一个事务中操作了很多 Redis 和 RocketMQ导致事务时间延长以及接口响应速度变慢等问题。所以使用binlog监听mysql通过canal发送操作消息到Rocketmq 再由单独线程顺序处理
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class UserCouponServiceImpl implements UserCouponService {

    private final CouponTemplateService couponTemplateService;
    private final UserCouponMapper userCouponMapper;
    private final CouponTemplateMapper couponTemplateMapper;
    private final UserCouponDelayCloseProducer couponDelayCloseProducer;

    private final StringRedisTemplate stringRedisTemplate;
    // 难点 通过编程式事务解决库存扣减事务回滚问题
    private final TransactionTemplate transactionTemplate;

    @Value("${one-coupon.user-coupon-list.save-cache.type}")
    private String userCouponListSaveCacheType;

    private final static String STOCK_DECREMENT_AND_SAVE_USER_RECEIVE_LUA_PATH = "lua/stock_decrement_and_save_user_receive.lua";

    @Override
    public void redeemUserCoupon(CouponTemplateRedeemReqDTO requestParam) {
        // 验证缓存是否存在，保障数据存在并且缓存中存在
        CouponTemplateQueryRespDTO couponTemplate = couponTemplateService.findCouponTemplate(BeanUtil.toBean(requestParam, CouponTemplateQueryReqDTO.class));

        // 验证领取的优惠券是否在活动有效时间
        boolean isInTime = DateUtil.isIn(new Date(), couponTemplate.getValidStartTime(), couponTemplate.getValidEndTime());
        if (!isInTime) {
            // 一般来说优惠券领取时间不到的时候，前端不会放开调用请求，可以理解这是用户调用接口在“攻击”
            throw new ClientException("不满足优惠券领取时间");
        }

        // 获取 LUA 脚本，并保存到 Hutool 的单例管理容器，下次直接获取不需要加载
        DefaultRedisScript<Long> buildLuaScript = Singleton.get(STOCK_DECREMENT_AND_SAVE_USER_RECEIVE_LUA_PATH, () -> {
            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
            redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource(STOCK_DECREMENT_AND_SAVE_USER_RECEIVE_LUA_PATH)));
            redisScript.setResultType(Long.class);
            return redisScript;
        });

        // 验证用户是否符合优惠券领取条件
        JSONObject receiveRule = JSON.parseObject(couponTemplate.getReceiveRule());
        String limitPerPerson = receiveRule.getString("limitPerPerson");

        // 执行 LUA 脚本进行扣减库存以及增加 Redis 用户领券记录次数
        // 难点 这里通过lua脚本判断库存是否充足，用户是否超领，如果正常则减库存，并保存用户领券次数，并设置过期时间 得到用户领取次数
        // (这里通过保存每一次用户领券记录 保证在分发优惠券时被唯一键约束)
        String couponTemplateCacheKey = String.format(EngineRedisConstant.COUPON_TEMPLATE_KEY, requestParam.getCouponTemplateId());
        String userCouponTemplateLimitCacheKey = String.format(EngineRedisConstant.USER_COUPON_TEMPLATE_LIMIT_KEY, UserContext.getUserId(), requestParam.getCouponTemplateId());
        Long stockDecrementLuaResult = stringRedisTemplate.execute(
                buildLuaScript,
                ListUtil.of(couponTemplateCacheKey, userCouponTemplateLimitCacheKey),
                String.valueOf(couponTemplate.getValidEndTime().getTime()), limitPerPerson
        );

        // 判断 LUA 脚本执行返回类，如果失败根据类型返回报错提示(只要不是成功状态 直接报错)
        long firstField = StockDecrementReturnCombinedUtil.extractFirstField(stockDecrementLuaResult);
        if (RedisStockDecrementErrorEnum.isFail(firstField)) {
            throw new ServiceException(RedisStockDecrementErrorEnum.fromType(firstField));
        }

        // 通过编程式事务执行优惠券库存自减以及增加用户优惠券领取记录
        long extractSecondField = StockDecrementReturnCombinedUtil.extractSecondField(stockDecrementLuaResult);
        transactionTemplate.executeWithoutResult(status -> {
            try {
                // 通过乐观锁进行库存扣减 由于行锁不会出现超卖现象
                int decremented = couponTemplateMapper.decrementCouponTemplateStock(Long.parseLong(requestParam.getShopNumber()), Long.parseLong(requestParam.getCouponTemplateId()), 1L);
                // 兜底 防止缓存并行得到相同库存 但是进行数据库库存扣减顺序执行 所以不会出现超卖现象
                if (!SqlHelper.retBool(decremented)) {
                    throw new ServiceException("优惠券已被领取完啦");
                }

                // 添加 Redis 用户领取的优惠券记录列表 通过唯一键约束 防止用户重复领券(数据库还没有保存成功 此时又有一个领券 但是数据库在插入时会有行锁 所以不会并发插入 进而被唯一键约束)
                Date now = new Date();
                DateTime validEndTime = DateUtil.offsetHour(now, JSON.parseObject(couponTemplate.getConsumeRule()).getInteger("validityPeriod"));
                UserCouponDO userCouponDO = UserCouponDO.builder()
                        .couponTemplateId(Long.parseLong(requestParam.getCouponTemplateId()))
                        .userId(Long.parseLong(UserContext.getUserId()))
                        .source(requestParam.getSource())
                        .receiveCount(Long.valueOf(extractSecondField).intValue())
                        .status(0)
                        .receiveTime(now)
                        .validStartTime(now)
                        .validEndTime(validEndTime)
                        .build();
                userCouponMapper.insert(userCouponDO);

                // 保存优惠券缓存集合有两个选项：direct 在流程里直接操作，binlog 通过解析数据库日志后操作
                if (StrUtil.equals(userCouponListSaveCacheType, "direct")) {
                    // 添加用户领取优惠券模板缓存记录 数据不一定是能持久化成功的，因为在极端宕机情况下，持久化配置中 AOF 最优结果下，也是会丢一条数据的。
                    String userCouponListCacheKey = String.format(EngineRedisConstant.USER_COUPON_TEMPLATE_LIST_KEY, UserContext.getUserId());
                    String userCouponItemCacheKey = StrUtil.builder()
                            .append(requestParam.getCouponTemplateId())
                            .append("_")
                            .append(userCouponDO.getId())
                            .toString();
                    stringRedisTemplate.opsForZSet().add(userCouponListCacheKey, userCouponItemCacheKey, now.getTime());
                    // 难点 Redis 如果是主从配置，主从数据同步是异步的，如果主节点写入成功，返回客户端成功，还没来得及同步从节点宕机了，从节点成为主节点，那么这个数据就成糊涂账了。
                    // 由于 Redis 在持久化或主从复制的极端情况下可能会出现数据丢失，而我们对指令丢失几乎无法容忍，因此我们采用经典的写后查询策略来应对这一问题
                    Double scored;
                    try {
                        scored = stringRedisTemplate.opsForZSet().score(userCouponListCacheKey, userCouponItemCacheKey);
                        // scored 为空意味着可能 Redis Cluster 主从同步丢失了数据，比如 Redis 主节点还没有同步到从节点就宕机了，解决方案就是再新增一次
                        if (scored == null) {
                            // 如果这里也新增失败了怎么办？我们大概率做不到绝对的万无一失，只能尽可能增加成功率
                            stringRedisTemplate.opsForZSet().add(userCouponListCacheKey, userCouponItemCacheKey, now.getTime());
                        }
                    } catch (Throwable ex) {
                        log.warn("查询Redis用户优惠券记录为空或抛异常，可能Redis宕机或主从复制数据丢失，基础错误信息：{}", ex.getMessage());
                        // 如果直接抛异常大概率 Redis 宕机了，所以应该写个延时队列向 Redis 重试放入值。为了避免代码复杂性，这里直接写新增，大家知道最优解决方案即可
                        stringRedisTemplate.opsForZSet().add(userCouponListCacheKey, userCouponItemCacheKey, now.getTime());
                    }

                    // 发送延时消息队列，等待优惠券到期后，将优惠券信息从缓存中删除
                    UserCouponDelayCloseEvent userCouponDelayCloseEvent = UserCouponDelayCloseEvent.builder()
                            .couponTemplateId(requestParam.getCouponTemplateId())
                            .userCouponId(String.valueOf(userCouponDO.getId()))
                            .userId(UserContext.getUserId())
                            .delayTime(validEndTime.getTime())
                            .build();
                    SendResult sendResult = couponDelayCloseProducer.sendMessage(userCouponDelayCloseEvent);

                    // 发送消息失败解决方案简单且高效的逻辑之一：打印日志并报警，通过日志搜集并重新投递
                    if (ObjectUtil.notEqual(sendResult.getSendStatus().name(), "SEND_OK")) {
                        log.warn("发送优惠券关闭延时队列失败，消息参数：{}", JSON.toJSONString(userCouponDelayCloseEvent));
                    }
                }
            } catch (Exception ex) {
                status.setRollbackOnly();
                // 优惠券已被领取完业务异常
                if (ex instanceof ServiceException) {
                    throw (ServiceException) ex;
                }
                if (ex instanceof DuplicateKeyException) {
                    log.error("用户重复领取优惠券，用户ID：{}，优惠券模板ID：{}", UserContext.getUserId(), requestParam.getCouponTemplateId());
                    throw new ServiceException("用户重复领取优惠券");
                }
                throw new ServiceException("优惠券领取异常，请稍候再试");
            }
        });
    }
}
