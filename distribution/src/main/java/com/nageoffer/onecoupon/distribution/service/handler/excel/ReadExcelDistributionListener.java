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

package com.nageoffer.onecoupon.distribution.service.handler.excel;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Singleton;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.alibaba.excel.util.ListUtils;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.extension.toolkit.SqlHelper;
import com.nageoffer.onecoupon.distribution.common.constant.DistributionRedisConstant;
import com.nageoffer.onecoupon.distribution.common.constant.EngineRedisConstant;
import com.nageoffer.onecoupon.distribution.common.context.UserContext;
import com.nageoffer.onecoupon.distribution.common.enums.CouponSourceEnum;
import com.nageoffer.onecoupon.distribution.common.enums.CouponStatusEnum;
import com.nageoffer.onecoupon.distribution.common.enums.CouponTaskStatusEnum;
import com.nageoffer.onecoupon.distribution.common.enums.RedisStockDecrementErrorEnum;
import com.nageoffer.onecoupon.distribution.dao.entity.CouponTaskDO;
import com.nageoffer.onecoupon.distribution.dao.entity.CouponTaskFailDO;
import com.nageoffer.onecoupon.distribution.dao.entity.CouponTemplateDO;
import com.nageoffer.onecoupon.distribution.dao.entity.UserCouponDO;
import com.nageoffer.onecoupon.distribution.dao.mapper.CouponTaskFailMapper;
import com.nageoffer.onecoupon.distribution.dao.mapper.CouponTaskMapper;
import com.nageoffer.onecoupon.distribution.dao.mapper.CouponTemplateMapper;
import com.nageoffer.onecoupon.distribution.dao.mapper.UserCouponMapper;
import com.nageoffer.onecoupon.distribution.mq.event.CouponTemplateDistributionEvent;
import com.nageoffer.onecoupon.distribution.mq.producer.CouponExecuteDistributionProducer;
import com.nageoffer.onecoupon.distribution.toolkit.StockDecrementReturnCombinedUtil;
import com.nageoffer.onecoupon.framework.exception.ServiceException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.executor.BatchExecutorException;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;

import java.util.*;

/**
 * 优惠券任务读取 Excel 分发监听器 这个主要用来Excel模板解析和前置校验
 */
@RequiredArgsConstructor
@Slf4j
public class ReadExcelDistributionListener extends AnalysisEventListener<CouponTaskExcelObject> {

    // 解析excel和分发模块分离使用的属性
    private final CouponTaskDO couponTaskDO;
    private final CouponTemplateDO couponTemplateDO;
    private final CouponTaskFailMapper couponTaskFailMapper;

    private final StringRedisTemplate stringRedisTemplate;
    private final CouponExecuteDistributionProducer couponExecuteDistributionProducer;

    // 原来解析excel和分发一起操作时使用的属性
    private final UserCouponMapper userCouponMapper;
    private final CouponTaskMapper couponTaskMapper;
    private final CouponTemplateMapper couponTemplateMapper;
    private final Long couponTaskId;

    // 用来记录当前已经读取过的行数
    private int rowCount = 1;
    private final static String STOCK_DECREMENT_AND_BATCH_SAVE_USER_RECORD_LUA_PATH = "lua/stock_decrement_and_batch_save_user_record.lua";
    private final static int BATCH_USER_COUPON_SIZE = 10;

    // TODO 目前还是存在问题 将excel的一行数据进行处理时花费时间过长 目前分析应该是redis处理时间过长 后续可以进一步分析问题
    /**
     * 难点 优化后的解析excel模块 每次解析excel都进行批处理 并且设置一个当前进度rowCount防止重头开始 并通过位运算提升了返回数据的性能 和分发模块分离(防止当个任务处理时长过长失效) 加速mq执行
     * 库存不足或者插入出现错误 会记录当前处理到的行数progress 之后所有的excel行都会加入到couponTaskFail表中(开头progress每次判断都是小于rowCount)
     * 库存充足 在lua脚本中会将每一行的用户id和当前行数加到缓存 如果不满足批量达到5000 记录当前处理行数progress 如果满足则直接发送分发消息到消费者进行批量分发
     * 最后不论是否满足批量5000 都会发送一个分发消息并将distributionEndFlag结束标志位设为true 表示这次解析结束
     * @param data
     * @param context
     */
    @Override
    public void invoke(CouponTaskExcelObject data, AnalysisContext context) {
        Long couponTaskId = couponTaskDO.getId();
        log.info("开始：{}", System.currentTimeMillis());
        // 获取当前进度，判断是否已经执行过。如果已执行，则跳过即可，防止执行到一半应用宕机
        String templateTaskExecuteProgressKey = String.format(DistributionRedisConstant.TEMPLATE_TASK_EXECUTE_PROGRESS_KEY, couponTaskId);
        String progress = stringRedisTemplate.opsForValue().get(templateTaskExecuteProgressKey);
        if (StrUtil.isNotBlank(progress) && Integer.parseInt(progress) >= rowCount) {
            ++rowCount;
            return;
        }

        // 获取 LUA 脚本，并保存到 Hutool 的单例管理容器，下次直接获取不需要加载
        DefaultRedisScript<Long> buildLuaScript = Singleton.get(STOCK_DECREMENT_AND_BATCH_SAVE_USER_RECORD_LUA_PATH, () -> {
            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
            redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource(STOCK_DECREMENT_AND_BATCH_SAVE_USER_RECORD_LUA_PATH)));
            redisScript.setResultType(Long.class);
            return redisScript;
        });

        // 限制用户领券次数
        JSONObject receiveRule = JSON.parseObject(couponTemplateDO.getReceiveRule());
        String limitPerPerson = receiveRule.getString("limitPerPerson");
        String userCouponTemplateLimitCacheKey = String.format(EngineRedisConstant.USER_COUPON_TEMPLATE_LIMIT_KEY, UserContext.getUserId(), couponTemplateDO.getId());

        // 执行 LUA 脚本进行扣减库存以及增加 Redis 用户领券记录
        String couponTemplateKey = String.format(EngineRedisConstant.COUPON_TEMPLATE_KEY, couponTemplateDO.getId());
        String batchUserSetKey = String.format(DistributionRedisConstant.TEMPLATE_TASK_EXECUTE_BATCH_USER_KEY, couponTaskId);
        Map<Object, Object> userRowNumMap = MapUtil.builder()
                .put("userId", data.getUserId())
                .put("rowNum", rowCount + 1)
                .build();
        Long combinedFiled = stringRedisTemplate.execute(buildLuaScript, ListUtil.of(couponTemplateKey, batchUserSetKey), userCouponTemplateLimitCacheKey,
                com.alibaba.fastjson.JSON.toJSONString(userRowNumMap), limitPerPerson);

        // firstField 为 false 说明优惠券已经没有库存了
        // 难点1 库存不足或者插入出现错误 会记录下当前处理到的行数 之后所有的excel行都会加入到couponTaskFail表中(开头progress每次判断都是小于rowCount)
        long firstField = StockDecrementReturnCombinedUtil.extractFirstField(combinedFiled);
        if (RedisStockDecrementErrorEnum.isFail(firstField)) {
            RedisStockDecrementErrorEnum.fromType(firstField);
            // 同步当前执行进度到缓存
            stringRedisTemplate.opsForValue().set(templateTaskExecuteProgressKey, String.valueOf(rowCount));
            ++rowCount;

            // 添加到 t_coupon_task_fail 并标记错误原因，方便后续查看未成功发送的原因和记录
            Map<Object, Object> objectMap = MapUtil.builder()
                    .put("rowNum", rowCount + 1)
                    .put("cause", RedisStockDecrementErrorEnum.fromType(firstField))
                    .build();
            CouponTaskFailDO couponTaskFailDO = CouponTaskFailDO.builder()
                    .batchId(couponTaskDO.getBatchId())
                    .jsonObject(com.alibaba.fastjson.JSON.toJSONString(objectMap, SerializerFeature.WriteMapNullValue))
                    .build();
            couponTaskFailMapper.insert(couponTaskFailDO);

            return;
        }

        // 获取用户领券集合长度
        int batchUserSetSize = StockDecrementReturnCombinedUtil.extractSecondField(combinedFiled.intValue());

        // batchUserSetSize = BATCH_USER_COUPON_SIZE 时发送消息消费，不满足条件仅记录执行进度即可
        if (batchUserSetSize < BATCH_USER_COUPON_SIZE) {
            // 同步当前 Excel 执行进度到缓存
            stringRedisTemplate.opsForValue().set(templateTaskExecuteProgressKey, String.valueOf(rowCount));
            ++rowCount;
            log.info("完成：{}", System.currentTimeMillis());
            return;
        }
        ListUtils.newArrayListWithExpectedSize(BATCH_USER_COUPON_SIZE);
        // 发送消息队列执行用户优惠券模板分发逻辑
        CouponTemplateDistributionEvent couponTemplateDistributionEvent = CouponTemplateDistributionEvent.builder()
                .couponTaskId(couponTaskId)
                .shopNumber(couponTaskDO.getShopNumber())
                .couponTemplateId(couponTemplateDO.getId())
                .couponTaskBatchId(couponTaskDO.getBatchId())
                .couponTemplateConsumeRule(couponTemplateDO.getConsumeRule())
                .batchUserSetSize(batchUserSetSize)
                .distributionEndFlag(Boolean.FALSE)
                .build();
        couponExecuteDistributionProducer.sendMessage(couponTemplateDistributionEvent);
        log.info("彻底结束：{}", System.currentTimeMillis());
        // 同步当前执行进度到缓存
        stringRedisTemplate.opsForValue().set(templateTaskExecuteProgressKey, String.valueOf(rowCount));
        ++rowCount;
    }

    @Override
    public void doAfterAllAnalysed(AnalysisContext context) {
        // 发送 Excel 解析完成标识，即使不满足批量保存的数量也得保存到数据库
        CouponTemplateDistributionEvent couponTemplateExecuteEvent = CouponTemplateDistributionEvent.builder()
                .distributionEndFlag(Boolean.TRUE) // 设置解析完成标识
                .shopNumber(couponTaskDO.getShopNumber())
                .couponTemplateId(couponTemplateDO.getId())
                .couponTemplateConsumeRule(couponTemplateDO.getConsumeRule())
                .couponTaskBatchId(couponTaskDO.getBatchId())
                .couponTaskId(couponTaskDO.getId())
                .build();
        couponExecuteDistributionProducer.sendMessage(couponTemplateExecuteEvent);
    }

    //  解析excel和分发在一起 效率太低 大约 5000 条 Excel 分发记录需要约 1 分钟。
    // 难点 分发版本一 保证一人一单 这里用用户id+券id+领取次数作为唯一索引 保证一个用户只推送优惠券一次(通过easyExcel逐行读取将数据放到data中 然后更新缓存和数据库)
    // TODO 如果用户之前领取过多次 这次分发还会成功 如果用户之前只领取过一次 那么这次分发就会失败
    //  应该先查询用户领取该优惠券次数是否为0，为0再发放，不为0则说明用户已经领取过了不用再发放了。
    // TODO 已解决：如果用户之前领取过多次 这次分发不会成功 因为在用户领取优惠券时 会将每一次领取都记录下来 所以如果用户领取过优惠券 那么一定存在领取过一次的记录 通过唯一索引约束
    // 保证数据库与redis数据一致性 在更新数据库时先操作数据库 再删缓存
    // 在发券的时候与秒杀并行执行，如果操作顺序不同，可能会导致数据不一致 例如 只剩1个库存 在发送时先减数据库(0) 此时秒杀时先减缓存(0) 然后秒杀再减数据库(-1) 最后发送时减缓存(-1)发生问题
    public void invoke1(CouponTaskExcelObject data, AnalysisContext context) {
        // 通过缓存判断优惠券模板记录库存是否充足
        String couponTemplateKey = String.format(EngineRedisConstant.COUPON_TEMPLATE_KEY, couponTemplateDO.getId());
        Long stock = stringRedisTemplate.opsForHash().increment(couponTemplateKey, "stock", -1);
        if (stock < 0) {
            // 优惠券模板缓存库存不足扣减失败
            return;
        }

        // 难点 乐观锁类似cas不带自旋的方法解决原子扣库存操作 避免超卖
        // 扣减优惠券模板库存，如果扣减成功，这里会返回 1，代表修改记录成功；否则返回 0，代表没有修改成功
        int decrementResult = couponTemplateMapper.decrementCouponTemplateStock(couponTemplateDO.getShopNumber(), couponTemplateDO.getId(), 1);
        if (!SqlHelper.retBool(decrementResult)) {
            // 优惠券模板数据库库存不足扣减失败
            return;
        }

        // 添加用户领券记录到数据库
        Date now = new Date();
        DateTime validEndTime = DateUtil.offsetHour(now, JSON.parseObject(couponTemplateDO.getConsumeRule()).getInteger("validityPeriod"));
        UserCouponDO userCouponDO = UserCouponDO.builder()
                .couponTemplateId(couponTemplateDO.getId())
                .userId(Long.parseLong(data.getUserId()))
                .receiveTime(now)
                .receiveCount(1) // 代表第一次领取该优惠券
                .validStartTime(now)
                .validEndTime(validEndTime)
                .source(CouponSourceEnum.PLATFORM.getType())
                .status(CouponStatusEnum.EFFECTIVE.getType())
                .createTime(new Date())
                .updateTime(new Date())
                .delFlag(0)
                .build();
        try {
            userCouponMapper.insert(userCouponDO);
        } catch (BatchExecutorException bee) {
            // 用户已领取优惠券，会被唯一索引校验住，直接返回即可
            // 恢复之前扣除的库存
            stringRedisTemplate.opsForHash().increment(couponTemplateKey, "stock", 1);
            couponTemplateMapper.incrementCouponTemplateStock(couponTemplateDO.getShopNumber(), couponTemplateDO.getId(), 1);
            return;
        }

        // 添加优惠券到用户已领取的 Redis 优惠券列表中
        // 一个优惠券可能被用户领取多次 所以需要在值的key中追加领券记录的 ID sortedSet会过滤重复元素
        String userCouponListCacheKey = String.format(EngineRedisConstant.USER_COUPON_TEMPLATE_LIST_KEY, data.getUserId());
        String userCouponItemCacheKey = StrUtil.builder()
                .append(couponTemplateDO.getId())
                .append("_")
                .append(userCouponDO.getId())
                .toString();
        stringRedisTemplate.opsForZSet().add(userCouponListCacheKey, userCouponItemCacheKey, now.getTime());
    }

    public void doAfterAllAnalysed1(AnalysisContext analysisContext) {
        // 确保所有用户都已经接到优惠券后，设置优惠券推送任务完成时间
        CouponTaskDO couponTaskDO = CouponTaskDO.builder()
                .id(couponTaskId)
                .status(CouponTaskStatusEnum.SUCCESS.getStatus())
                .completionTime(new Date())
                .build();
        couponTaskMapper.updateById(couponTaskDO);
    }
}
