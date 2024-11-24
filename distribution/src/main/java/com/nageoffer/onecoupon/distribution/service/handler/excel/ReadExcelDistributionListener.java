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

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.extension.toolkit.SqlHelper;
import com.nageoffer.onecoupon.distribution.common.constant.EngineRedisConstant;
import com.nageoffer.onecoupon.distribution.common.enums.CouponSourceEnum;
import com.nageoffer.onecoupon.distribution.common.enums.CouponStatusEnum;
import com.nageoffer.onecoupon.distribution.common.enums.CouponTaskStatusEnum;
import com.nageoffer.onecoupon.distribution.dao.entity.CouponTaskDO;
import com.nageoffer.onecoupon.distribution.dao.entity.CouponTemplateDO;
import com.nageoffer.onecoupon.distribution.dao.entity.UserCouponDO;
import com.nageoffer.onecoupon.distribution.dao.mapper.CouponTaskMapper;
import com.nageoffer.onecoupon.distribution.dao.mapper.CouponTemplateMapper;
import com.nageoffer.onecoupon.distribution.dao.mapper.UserCouponMapper;
import lombok.RequiredArgsConstructor;
import org.apache.ibatis.executor.BatchExecutorException;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Date;

/**
 * 优惠券任务读取 Excel 分发监听器
 */
@RequiredArgsConstructor
public class ReadExcelDistributionListener extends AnalysisEventListener<CouponTaskExcelObject> {

    private final Long couponTaskId;
    private final CouponTemplateDO couponTemplateDO;
    private final StringRedisTemplate stringRedisTemplate;
    private final CouponTemplateMapper couponTemplateMapper;
    private final UserCouponMapper userCouponMapper;
    private final CouponTaskMapper couponTaskMapper;

    // 难点 保证一人一单 这里用用户id+券id+领取次数作为唯一索引 保证一个用户只推送优惠券一次(通过easyExcel逐行读取将数据放到data中 然后更新缓存和数据库)
    // TODO 如果用户之前领取过多次 这次分发还会成功 如果用户之前只领取过一次 那么这次分发就会失败
    //  应该先查询用户领取该优惠券次数是否为0，为0再发放，不为0则说明用户已经领取过了不用再发放了。
    // 保证数据库与redis数据一致性 在更新数据库时先操作数据库 再删缓存
    // 在发券的时候与秒杀并行执行，如果操作顺序不同，可能会导致数据不一致 例如 只剩1个库存 在发送时先减数据库(0) 此时秒杀时先减缓存(0) 然后秒杀再减数据库(-1) 最后发送时减缓存(-1)发生问题
    @Override
    public void invoke(CouponTaskExcelObject data, AnalysisContext context) {
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

    @Override
    public void doAfterAllAnalysed(AnalysisContext analysisContext) {
        // 确保所有用户都已经接到优惠券后，设置优惠券推送任务完成时间
        CouponTaskDO couponTaskDO = CouponTaskDO.builder()
                .id(couponTaskId)
                .status(CouponTaskStatusEnum.SUCCESS.getStatus())
                .completionTime(new Date())
                .build();
        couponTaskMapper.updateById(couponTaskDO);
    }
}
