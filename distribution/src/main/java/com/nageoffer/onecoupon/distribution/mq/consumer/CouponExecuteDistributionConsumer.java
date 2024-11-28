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

package com.nageoffer.onecoupon.distribution.mq.consumer;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Singleton;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.toolkit.SqlHelper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nageoffer.onecoupon.distribution.common.constant.DistributionRedisConstant;
import com.nageoffer.onecoupon.distribution.common.constant.EngineRedisConstant;
import com.nageoffer.onecoupon.distribution.common.enums.CouponSourceEnum;
import com.nageoffer.onecoupon.distribution.common.enums.CouponStatusEnum;
import com.nageoffer.onecoupon.distribution.common.enums.CouponTaskStatusEnum;
import com.nageoffer.onecoupon.distribution.dao.entity.CouponTaskDO;
import com.nageoffer.onecoupon.distribution.dao.entity.CouponTaskFailDO;
import com.nageoffer.onecoupon.distribution.dao.entity.CouponTemplateDO;
import com.nageoffer.onecoupon.distribution.dao.entity.UserCouponDO;
import com.nageoffer.onecoupon.distribution.dao.mapper.CouponTaskFailMapper;
import com.nageoffer.onecoupon.distribution.dao.mapper.CouponTaskMapper;
import com.nageoffer.onecoupon.distribution.dao.mapper.CouponTemplateMapper;
import com.nageoffer.onecoupon.distribution.dao.mapper.UserCouponMapper;
import com.nageoffer.onecoupon.distribution.mq.base.MessageWrapper;
import com.nageoffer.onecoupon.distribution.mq.event.CouponTemplateDistributionEvent;
import com.nageoffer.onecoupon.distribution.service.handler.excel.UserCouponTaskFailExcelObject;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.executor.BatchExecutorException;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.file.Paths;
import java.util.*;
// 存在问题可以继续优化:
// 如果 batchSaveUserCouponList 执行时数据库宕机，那么就会面临事务回滚问题，意味着我们需要将从 Redis 中获取的领券用户记录再保存到 Redis。
// 批量新增时有部分用户优惠券已存在，那么新增数量和预期就会不一致，也就意味着我们需要将这些失败的库存返回给数据库和缓存。
/**
 * 优惠券执行分发到用户消费者 消息队列需要顺序消息 具体单队列(并发太慢) 多队列(根据分发任务 ID 进行 Hash 投递到固定队列：并发较高，推荐使用该策略。)
 * 将优惠券分发到用户账户，包括数据库和缓存等多个存储介质。
 * 难点 优化后的分发模块 每一次当excel批量处理了5000个任务发送消息后 分发模块消费者进行批处理 将没有到excel结尾false和到结尾true的任务分情况批处理 并保存处理失败的记录 更新缓存
 * 难点 spring中默认事务隔离级别是PROPAGATION_REQUIRED 表示如果没有事务新开事务 有则融入 因为已经声明了onMessage是事务 所以其中调用的方法会默认开启事务
 */
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = "one-coupon_distribution-service_coupon-execute-distribution_topic${unique-name:}",
        consumerGroup = "one-coupon_distribution-service_coupon-execute-distribution_cg${unique-name:}"
)
@Slf4j(topic = "CouponExecuteDistributionConsumer")
public class CouponExecuteDistributionConsumer implements RocketMQListener<MessageWrapper<CouponTemplateDistributionEvent>> {

    private final UserCouponMapper userCouponMapper;
    private final CouponTemplateMapper couponTemplateMapper;
    private final CouponTaskMapper couponTaskMapper;
    private final CouponTaskFailMapper couponTaskFailMapper;
    private final StringRedisTemplate stringRedisTemplate;

    @Lazy
    @Autowired
    private CouponExecuteDistributionConsumer couponExecuteDistributionConsumer;

    private final static int BATCH_USER_COUPON_SIZE = 500;
    private static final String BATCH_SAVE_USER_COUPON_LUA_PATH = "lua/batch_user_coupon_list.lua";
    private final String excelPath = Paths.get("").toAbsolutePath() + "/tmp";

    @Transactional(rollbackFor = Exception.class)
    @Override
    public void onMessage(MessageWrapper<CouponTemplateDistributionEvent> messageWrapper) {
        // 开头打印日志，平常可 Debug 看任务参数，线上可报平安（比如消息是否消费，重新投递时获取参数等）
        log.info("[消费者] 优惠券任务执行推送@分发到用户账号 - 执行消费逻辑，消息体：{}", JSON.toJSONString(messageWrapper));

        // 难点1 当保存用户优惠券集合达到批量保存数量 没有到excel尾必须满足5000(%5000 = 0)
        // 也可能没有到excel尾 第一次接收到5000消息 但是此时还没有减去缓存中的列表 所以继续读excel时缓存列表中的数目还是5000 所以添加一行后就是5001 所以又发送了一条消息
        // 这条消息不能被消费 所以这里用%5000确保每次都是批量处理5000
        CouponTemplateDistributionEvent event = messageWrapper.getMessage();
        if (!event.getDistributionEndFlag() && event.getBatchUserSetSize() % BATCH_USER_COUPON_SIZE == 0) {
            decrementCouponTemplateStockAndSaveUserCouponList(event);
            return;
        }
        // 分发任务结束标识为 TRUE，代表已经没有 Excel 记录了(如果没有到达excel尾但是批量超过5000 即%5000不是0 则证明之前批量5000的没有处理掉 生产者又产生一条消息)
        if (event.getDistributionEndFlag()) {
            String batchUserSetKey = String.format(DistributionRedisConstant.TEMPLATE_TASK_EXECUTE_BATCH_USER_KEY, event.getCouponTaskId());
            Long batchUserIdsSize = stringRedisTemplate.opsForSet().size(batchUserSetKey);
            event.setBatchUserSetSize(batchUserIdsSize.intValue());

            // 将excel尾的一批分发任务处理 扣库存 保存记录
            decrementCouponTemplateStockAndSaveUserCouponList(event);
            List<String> batchUserMaps = stringRedisTemplate.opsForSet().pop(batchUserSetKey, Integer.MAX_VALUE);
            // 此时待保存入库用户优惠券列表如果还有值，就意味着可能库存不足引起的
            if (CollUtil.isNotEmpty(batchUserMaps)) {
                // 添加到 t_coupon_task_fail 并标记错误原因，方便后续查看未成功发送的原因和记录
                List<CouponTaskFailDO> couponTaskFailDOList = new ArrayList<>(batchUserMaps.size());
                for (String batchUserMapStr : batchUserMaps) {
                    Map<Object, Object> objectMap = MapUtil.builder()
                            .put("rowNum", JSON.parseObject(batchUserMapStr).get("rowNum"))
                            .put("cause", "用户已领取该优惠券")
                            .build();
                    CouponTaskFailDO couponTaskFailDO = CouponTaskFailDO.builder()
                            .batchId(event.getCouponTaskBatchId())
                            .jsonObject(com.alibaba.fastjson.JSON.toJSONString(objectMap))
                            .build();
                    couponTaskFailDOList.add(couponTaskFailDO);
                }

                // 添加到 t_coupon_task_fail 并标记错误原因
                couponTaskFailMapper.insert(couponTaskFailDOList);
            }

            long initId = 0;
            boolean isFirstIteration = true;  // 用于标识是否为第一次迭代
            String failFileAddress = excelPath + "/用户分发记录失败Excel-" + event.getCouponTaskBatchId() + ".xlsx";

            // 这里应该上传云 OSS 或者 MinIO 等存储平台，但是增加部署成功并且不太好往简历写就仅写入本地
            // 只有在第一次调用 excelWriter.write(xx, xx) 方法才会创建 Excel 文件 不用担心创建空记录 Excel 文件
            try (ExcelWriter excelWriter = EasyExcel.write(failFileAddress, UserCouponTaskFailExcelObject.class).build()) {
                WriteSheet writeSheet = EasyExcel.writerSheet("用户分发失败Sheet").build();
                while (true) {
                    // 优化 利用书签记录(直接根据有序的id进行向后或者向前查找)解决深分页问题
                    List<CouponTaskFailDO> couponTaskFailDOList = listUserCouponTaskFail(event.getCouponTaskBatchId(), initId);
                    if (CollUtil.isEmpty(couponTaskFailDOList)) {
                        // 如果是第一次迭代且集合为空，则设置 failFileAddress 为 null
                        if (isFirstIteration) {
                            failFileAddress = null;
                        }
                        break;

                    }

                    // 标记第一次迭代已经完成
                    isFirstIteration = false;

                    // 将失败行数和失败原因写入 Excel 文件
                    List<UserCouponTaskFailExcelObject> excelDataList = couponTaskFailDOList.stream()
                            .map(each -> JSONObject.parseObject(each.getJsonObject(), UserCouponTaskFailExcelObject.class))
                            .toList();
                    excelWriter.write(excelDataList, writeSheet);

                    // 查询出来的数据如果小于 BATCH_USER_COUPON_SIZE 意味着后面将不再有数据，返回即可
                    if (couponTaskFailDOList.size() < BATCH_USER_COUPON_SIZE) {
                        break;
                    }

                    // 更新 initId 为当前列表中最大 ID
                    initId = couponTaskFailDOList.stream()
                            .mapToLong(CouponTaskFailDO::getId)
                            .max()
                            .orElse(initId);
                }
            }

            // 确保所有用户都已经接到优惠券后，设置优惠券推送任务完成时间
            CouponTaskDO couponTaskDO = CouponTaskDO.builder()
                    .id(event.getCouponTaskId())
                    .status(CouponTaskStatusEnum.SUCCESS.getStatus())
                    .completionTime(new Date())
                    .build();
            couponTaskMapper.updateById(couponTaskDO);
        }
    }

    /**
     * 难点2 扣减库存(递归 乐观锁)并且批量保存(将失败记录保存fail表 成功记录保存user表) 最后将保存成功的记录保存到用户领券缓存中(key:prefix+userid value:couponId+领券id+时间)
     * @param event
     */
    @SneakyThrows
    private void decrementCouponTemplateStockAndSaveUserCouponList(CouponTemplateDistributionEvent event) {
        // 如果等于 0 意味着已经没有了库存，直接返回即可(如果库存不满足这次批处理的数量 那么就将库存中的数量先扣除)
        Integer couponTemplateStock = decrementCouponTemplateStock(event, event.getBatchUserSetSize());
        if (couponTemplateStock <= 0) {
            return;
        }

        // 获取 Redis 中待保存入库用户优惠券列表 将数据库中减去的库存数量相同的缓存中任务集合去除 并记录到数据库用户领券记录表中
        String batchUserSetKey = String.format(DistributionRedisConstant.TEMPLATE_TASK_EXECUTE_BATCH_USER_KEY, event.getCouponTaskId());
        List<String> batchUserMaps = stringRedisTemplate.opsForSet().pop(batchUserSetKey, couponTemplateStock);

        // 因为 batchUserIds 数据较多，ArrayList 会进行数次扩容，为了避免额外性能消耗，直接初始化 batchUserIds 大小的数组
        List<UserCouponDO> userCouponDOList = new ArrayList<>(batchUserMaps.size());
        Date now = new Date();

        // 构建 userCouponDOList 用户优惠券批量数组
        for (String each : batchUserMaps) {
            JSONObject userIdAndRowNumJsonObject = JSON.parseObject(each);
            DateTime validEndTime = DateUtil.offsetHour(now, JSON.parseObject(event.getCouponTemplateConsumeRule()).getInteger("validityPeriod"));
            UserCouponDO userCouponDO = UserCouponDO.builder()
                    .id(IdUtil.getSnowflakeNextId())
                    .couponTemplateId(event.getCouponTemplateId())
                    .rowNum(userIdAndRowNumJsonObject.getInteger("rowNum"))
                    .userId(userIdAndRowNumJsonObject.getLong("userId"))
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
            userCouponDOList.add(userCouponDO);
        }

        // 平台优惠券每个用户限领一次。批量新增用户优惠券记录，
        batchSaveUserCouponList(event.getCouponTemplateId(), event.getCouponTaskBatchId(), userCouponDOList);

        // 将这些优惠券添加到用户的领券记录中(用户id)
        List<String> userIdList = userCouponDOList.stream()
                .map(UserCouponDO::getUserId)
                .map(String::valueOf)
                .toList();
        String userIdsJson = new ObjectMapper().writeValueAsString(userIdList);
        // 券id + 领券id
        List<String> couponIdList = userCouponDOList.stream()
                .map(each -> StrUtil.builder()
                        .append(event.getCouponTemplateId())
                        .append("_")
                        .append(each.getId())
                        .toString())
                .map(String::valueOf)
                .toList();
        String couponIdsJson = new ObjectMapper().writeValueAsString(couponIdList);

        // 调用 Lua 脚本时，传递参数
        List<String> keys = Arrays.asList(EngineRedisConstant.USER_COUPON_TEMPLATE_LIST_KEY);
        List<String> args = Arrays.asList(userIdsJson, couponIdsJson, String.valueOf(new Date().getTime()));

        // 获取 LUA 脚本，并保存到 Hutool 的单例管理容器，下次直接获取不需要加载
        DefaultRedisScript<Void> buildLuaScript = Singleton.get(BATCH_SAVE_USER_COUPON_LUA_PATH, () -> {
            DefaultRedisScript<Void> redisScript = new DefaultRedisScript<>();
            redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource(BATCH_SAVE_USER_COUPON_LUA_PATH)));
            redisScript.setResultType(Void.class);
            return redisScript;
        });
        stringRedisTemplate.execute(buildLuaScript, keys, args.toArray());
    }

    /**
     * 递归扣库存 通过乐观锁避免超卖 返回扣除的库存量 单纯减数据库库存
     * @param event
     * @param decrementStockSize
     * @return
     */
    private Integer decrementCouponTemplateStock(CouponTemplateDistributionEvent event, Integer decrementStockSize) {
        // 通过乐观机制自减优惠券库存记录
        Long couponTemplateId = event.getCouponTemplateId();
        int decremented = couponTemplateMapper.decrementCouponTemplateStock(event.getShopNumber(), couponTemplateId, decrementStockSize);

        // 如果修改记录失败，意味着优惠券库存已不足，需要重试获取到可自减的库存数值
        if (!SqlHelper.retBool(decremented)) {
            LambdaQueryWrapper<CouponTemplateDO> queryWrapper = Wrappers.lambdaQuery(CouponTemplateDO.class)
                    .eq(CouponTemplateDO::getShopNumber, event.getShopNumber())
                    .eq(CouponTemplateDO::getId, couponTemplateId);
            CouponTemplateDO couponTemplateDO = couponTemplateMapper.selectOne(queryWrapper);
            return decrementCouponTemplateStock(event, couponTemplateDO.getStock());
        }

        return decrementStockSize;
    }

    /**
     * 批量保存 如果批量保存失败 说明中间有的用户已经领取过这个券了(唯一索引约数) 将批量保存转换为逐行保存 遇到失败 就将这个失败记录保存到fail表中 并将成功的保存到user表中
     * 难点 唯一索引的功能，即使事务没有提交，唯一索引也是生效的。举例：事务执行中，没提交数据；然后在数据库可视化工具里，新增同样的唯一索引记录，会报错。
     * @param couponTemplateId
     * @param couponTaskBatchId
     * @param userCouponDOList
     */
    private void batchSaveUserCouponList(Long couponTemplateId, Long couponTaskBatchId, List<UserCouponDO> userCouponDOList) {
        // MyBatis-Plus 批量执行用户优惠券记录
        try {
            userCouponMapper.insert(userCouponDOList, userCouponDOList.size());
        } catch (Exception ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof BatchExecutorException) {
                // 添加到 t_coupon_task_fail 并标记错误原因，方便后续查看未成功发送的原因和记录
                List<CouponTaskFailDO> couponTaskFailDOList = new ArrayList<>();
                List<UserCouponDO> toRemove = new ArrayList<>();

                // 调用批量新增失败后，为了避免大量重复失败，我们通过新增单条记录方式执行 也就是其中有用户已经领取过了
                userCouponDOList.forEach(each -> {
                    try {
                        userCouponMapper.insert(each);
                    } catch (Exception ignored) {
                        Boolean hasReceived = couponExecuteDistributionConsumer.hasUserReceivedCoupon(couponTemplateId, each.getUserId());
                        if (hasReceived) {
                            // 添加到 t_coupon_task_fail 并标记错误原因，方便后续查看未成功发送的原因和记录
                            Map<Object, Object> objectMap = MapUtil.builder()
                                    .put("rowNum", each.getRowNum())
                                    .put("cause", "用户已领取该优惠券")
                                    .build();
                            CouponTaskFailDO couponTaskFailDO = CouponTaskFailDO.builder()
                                    .batchId(couponTaskBatchId)
                                    .jsonObject(com.alibaba.fastjson.JSON.toJSONString(objectMap))
                                    .build();
                            couponTaskFailDOList.add(couponTaskFailDO);

                            // 从 userCouponDOList 中删除已经存在的记录
                            toRemove.add(each);
                        }
                    }
                });

                // 批量新增 t_coupon_task_fail 表
                couponTaskFailMapper.insert(couponTaskFailDOList, couponTaskFailDOList.size());

                // 删除已经重复的内容 避免在之后插入到缓存的用户领券记录时重复
                userCouponDOList.removeAll(toRemove);
                return;
            }

            throw ex;
        }
    }

    /**
     * 查询用户是否已经领取过优惠券
     * 难点 这里使用NOT_SUPPORTED传播方式 表示不支持事务 以非事务处理 避免查询到事务缓存中的消息 但是实际上还没有插入到数据库
     * 如果第一次批量保存数据库失败后，批量保存的一些记录会在事务缓冲区中，如果在同一个事务下，会查到数据库中本不存在的记录。
     * 我们保存用户已领取优惠券的失败记录，需要是数据库中真实存在的，所以不使用事务的方式查询。
     *
     * @param couponTemplateId 优惠券模板 ID
     * @param userId           用户 ID
     * @return 用户优惠券模板领取信息是否已存在
     */
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public Boolean hasUserReceivedCoupon(Long couponTemplateId, Long userId) {
        LambdaQueryWrapper<UserCouponDO> queryWrapper = Wrappers.lambdaQuery(UserCouponDO.class)
                .eq(UserCouponDO::getUserId, userId)
                .eq(UserCouponDO::getCouponTemplateId, couponTemplateId);
        return userCouponMapper.selectOne(queryWrapper) != null;
    }

    /**
     * 查询用户分发任务失败记录
     *
     * @param batchId 分发任务批次 ID
     * @param maxId   上次读取最大 ID
     * @return 用户分发任务失败记录集合
     */
    private List<CouponTaskFailDO> listUserCouponTaskFail(Long batchId, Long maxId) {
        LambdaQueryWrapper<CouponTaskFailDO> queryWrapper = Wrappers.lambdaQuery(CouponTaskFailDO.class)
                .eq(CouponTaskFailDO::getBatchId, batchId)
                .gt(CouponTaskFailDO::getId, maxId)
                .last("LIMIT " + BATCH_USER_COUPON_SIZE);
        return couponTaskFailMapper.selectList(queryWrapper);
    }
}
