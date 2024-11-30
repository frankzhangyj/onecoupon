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
import cn.hutool.core.date.DateUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.nageoffer.onecoupon.engine.common.context.UserContext;
import com.nageoffer.onecoupon.engine.dao.entity.CouponTemplateRemindDO;
import com.nageoffer.onecoupon.engine.dao.mapper.CouponTemplateRemindMapper;
import com.nageoffer.onecoupon.engine.dto.req.CouponTemplateQueryReqDTO;
import com.nageoffer.onecoupon.engine.dto.req.CouponTemplateRemindCreateReqDTO;
import com.nageoffer.onecoupon.engine.dto.resp.CouponTemplateQueryRespDTO;
import com.nageoffer.onecoupon.engine.mq.event.CouponRemindDelayEvent;
import com.nageoffer.onecoupon.engine.mq.producer.CouponRemindDelayProducer;
import com.nageoffer.onecoupon.engine.service.CouponTemplateRemindService;
import com.nageoffer.onecoupon.engine.service.CouponTemplateService;
import com.nageoffer.onecoupon.engine.toolkit.CouponTemplateRemindUtil;
import com.nageoffer.onecoupon.framework.exception.ClientException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 优惠券预约提醒业务逻辑实现层
 * 难点 通过位图思想 将通知类型和通知时间合并为一个long字段 规定以五分钟为单位 只能最大1小时 所以每个类型对应12位 5中类型就是60位 所以用long
 */
@Service
@RequiredArgsConstructor
public class CouponTemplateServiceRemindImpl extends ServiceImpl<CouponTemplateRemindMapper, CouponTemplateRemindDO> implements CouponTemplateRemindService {

    private final CouponTemplateRemindMapper couponTemplateRemindMapper;
    private final CouponTemplateService couponTemplateService;
    private final CouponRemindDelayProducer couponRemindDelayProducer;

    @Override
    @Transactional
    public void createCouponRemind(CouponTemplateRemindCreateReqDTO requestParam) {
        // 验证优惠券是否存在，避免缓存穿透问题并获取优惠券开抢时间
        CouponTemplateQueryRespDTO couponTemplate = couponTemplateService
                .findCouponTemplate(new CouponTemplateQueryReqDTO(requestParam.getShopNumber(), requestParam.getCouponTemplateId()));

        // 查询用户是否已经预约过优惠券的提醒信息
        LambdaQueryWrapper<CouponTemplateRemindDO> queryWrapper = Wrappers.lambdaQuery(CouponTemplateRemindDO.class)
                .eq(CouponTemplateRemindDO::getUserId, UserContext.getUserId())
                .eq(CouponTemplateRemindDO::getCouponTemplateId, requestParam.getCouponTemplateId());
        CouponTemplateRemindDO couponTemplateRemindDO = couponTemplateRemindMapper.selectOne(queryWrapper);

        // 如果没创建过提醒
        if (couponTemplateRemindDO == null) {
            couponTemplateRemindDO = BeanUtil.toBean(requestParam, CouponTemplateRemindDO.class);

            // 设置优惠券开抢时间信息
            couponTemplateRemindDO.setStartTime(couponTemplate.getValidStartTime());
            couponTemplateRemindDO.setInformation(CouponTemplateRemindUtil.calculateBitMap(requestParam.getRemindTime(), requestParam.getType()));
            couponTemplateRemindDO.setUserId(Long.parseLong(UserContext.getUserId()));

            couponTemplateRemindMapper.insert(couponTemplateRemindDO);
        } else {
            Long information = couponTemplateRemindDO.getInformation();
            Long bitMap = CouponTemplateRemindUtil.calculateBitMap(requestParam.getRemindTime(), requestParam.getType());
            // 有重复 只要有两个相同位是1 那么就会判断为重复
            if ((information & bitMap) != 0L) {
                throw new ClientException("已经创建过该提醒了");
            }
            // 亦或相当于直接插入 相同为0 不同为1
            couponTemplateRemindDO.setInformation(information ^ bitMap);

            couponTemplateRemindMapper.update(couponTemplateRemindDO, queryWrapper);
        }
        // 难点 为什么使用mq
        // Redis 延时队列 / Redis 存储信息：Redis 以内存为主进行数据存储，当面对百万级消息时，即使使用 BitMap 进行压缩，也很难有效存储，尤其是这些消息可能需要几天后才被消费。
        // 这会导致内存占用巨大。而 RocketMQ 则通过硬盘存储数据，能够轻松处理和存储大量消息。

        // MySQL 存储消息：将消息存储在 MySQL 中，并通过定时任务在指定时间提取消息进行消费。MySQL 的 QPS 大约为 5000 多，消费百万条消息大量读取的 IO 容易成为系统瓶颈。
        // 此外，随着数据量的增加，MySQL 的迭代速度非常快，引入定时任务删除也增加了系统的复杂性。
        // 发送预约提醒抢购优惠券延时消息
        CouponRemindDelayEvent couponRemindDelayEvent = CouponRemindDelayEvent.builder()
                .couponTemplateId(couponTemplate.getId())
                .userId(UserContext.getUserId())
                .contact(UserContext.getUserId())
                .shopNumber(couponTemplate.getShopNumber())
                .type(requestParam.getType())
                .remindTime(requestParam.getRemindTime())
                .startTime(couponTemplate.getValidStartTime())
                .delayTime(DateUtil.offsetMinute(couponTemplate.getValidStartTime(), -requestParam.getRemindTime()).getTime())
                .build();
        couponRemindDelayProducer.sendMessage(couponRemindDelayEvent);
    }
}
