package com.nageoffer.onecoupon.merchant.admin.mq.consumer;

import com.alibaba.excel.EasyExcel;
import com.alibaba.fastjson2.JSONObject;
import com.nageoffer.onecoupon.merchant.admin.dao.entity.CouponTaskDO;
import com.nageoffer.onecoupon.merchant.admin.dao.mapper.CouponTaskMapper;
import com.nageoffer.onecoupon.merchant.admin.service.handler.excel.RowCountListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = "one-coupon_merchant-admin-service_coupon-template-task_topic${unique-name:}",
        consumerGroup = "one-coupon_merchant-admin-service_coupon-template-task-status_cg${unique-name:}"
)
@Slf4j(topic = "CouponTemplateTaskConsumer")
public class CouponTemplateTaskConsumer implements RocketMQListener<JSONObject> {

    private final CouponTaskMapper couponTaskMapper;

    @Override
    public void onMessage(JSONObject message) {
        // 开头打印日志，平常可 Debug 看任务参数，线上可报平安（比如消息是否消费，重新投递时获取参数等）
        log.info("[消费者] 优惠券模板推送执行@变更模板表状态 - 执行消费逻辑，消息体：{}", message.toString());

        CouponTaskDO couponTaskDO = message.toJavaObject(CouponTaskDO.class);

        // 通过 EasyExcel 监听器获取 Excel 中所有行数
        RowCountListener listener = new RowCountListener();
        EasyExcel.read(couponTaskDO.getFailFileAddress(), listener).sheet().doRead();

        // 为什么需要统计行数？因为发送后需要比对所有优惠券是否都已发放到用户账号
        int totalRows = listener.getRowCount();
        couponTaskDO.setSendNum(totalRows);

        // 保存优惠券推送任务记录到数据库
        couponTaskMapper.insert(couponTaskDO);
    }

}
