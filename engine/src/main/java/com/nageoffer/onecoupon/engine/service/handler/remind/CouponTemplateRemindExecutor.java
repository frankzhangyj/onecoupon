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

package com.nageoffer.onecoupon.engine.service.handler.remind;

import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson2.JSON;
import com.nageoffer.onecoupon.engine.common.enums.CouponRemindTypeEnum;
import com.nageoffer.onecoupon.engine.mq.event.CouponRemindDelayEvent;
import com.nageoffer.onecoupon.engine.mq.producer.CouponRemindDelayProducer;
import com.nageoffer.onecoupon.engine.service.handler.remind.dto.CouponTemplateRemindDTO;
import com.nageoffer.onecoupon.engine.service.handler.remind.impl.SendAppMessageRemindCouponTemplate;
import com.nageoffer.onecoupon.engine.service.handler.remind.impl.SendEmailRemindCouponTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.*;

import static com.nageoffer.onecoupon.engine.common.constant.EngineRedisConstant.COUPON_REMIND_CHECK_KEY;

/**
 * 执行相应的抢券提醒
 * 消息处理流程是收到消息后，通过 Redis 发送延时队列设置延时10s，任务立即丢进线程池执行
 */
@Component
@RequiredArgsConstructor
public class CouponTemplateRemindExecutor {

    private final SendEmailRemindCouponTemplate sendEmailRemindCouponTemplate;
    private final SendAppMessageRemindCouponTemplate sendAppMessageRemindCouponTemplate;

    private final RedissonClient redissonClient;
    private final StringRedisTemplate stringRedisTemplate;

    // 提醒用户属于 IO 密集型任务
    private final ExecutorService executorService = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors() << 1,
            Runtime.getRuntime().availableProcessors() << 2,
            60,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadPoolExecutor.CallerRunsPolicy()
    );
    public static final String REDIS_BLOCKING_DEQUE = "COUPON_REMIND_QUEUE";

    /**
     * 执行提醒
     *
     * @param couponTemplateRemindDTO 用户预约提醒请求信息
     */
    public void executeRemindCouponTemplate(CouponTemplateRemindDTO couponTemplateRemindDTO) {
        // 假设刚把消息提交到线程池，突然应用宕机了，我们通过延迟队列进行兜底 Refresh
        // 使用阻塞队列可以在队列为空时阻塞 不会使得cpu空转
        RBlockingDeque<String> blockingDeque = redissonClient.getBlockingDeque(REDIS_BLOCKING_DEQUE);
        RDelayedQueue<String> delayedQueue = redissonClient.getDelayedQueue(blockingDeque);
        String key = String.format(COUPON_REMIND_CHECK_KEY, couponTemplateRemindDTO.getUserId(), couponTemplateRemindDTO.getCouponTemplateId(), couponTemplateRemindDTO.getRemindTime(), couponTemplateRemindDTO.getType());
        stringRedisTemplate.opsForValue().set(key, JSON.toJSONString(couponTemplateRemindDTO));
        delayedQueue.offer(key, 10, TimeUnit.SECONDS);

        executorService.execute(() -> {
            // 用户没取消预约，则发出提醒
            // 向用户发起消息提醒
            switch (Objects.requireNonNull(CouponRemindTypeEnum.getByType(couponTemplateRemindDTO.getType()))) {
                case APP -> sendAppMessageRemindCouponTemplate.remind(couponTemplateRemindDTO);
                case EMAIL -> sendEmailRemindCouponTemplate.remind(couponTemplateRemindDTO);
                default -> {
                }
            }

            // 提醒用户后删除 Key
            stringRedisTemplate.delete(key);
        });
    }

    // 难点 使用实现CommandLineRunner接口重写run方法 spring自动将依赖注入 和 在外部类中使用postConstruct手动通过构造方法注入依赖效果类似
    // 都是在外部类没有完全初始化之前进行依赖注入 并运行逻辑
    // 但是postConstruct能够明确在外部类中的依赖注入完成后执行内部类的依赖注入 比较严谨
    // CommandLineRunner定义的类中依赖注入由IOC容器决定 一般都是在所有的 Bean 都初始化完成之后被调用。
    @Slf4j
    @Component
    @RequiredArgsConstructor
    static class RefreshCouponRemindDelayQueueRunner implements CommandLineRunner {

        private final CouponRemindDelayProducer couponRemindProducer;
        private final RedissonClient redissonClient;
        private final StringRedisTemplate stringRedisTemplate;

        @Override
        public void run(String... args) {
            Executors.newSingleThreadExecutor(
                            runnable -> {
                                Thread thread = new Thread(runnable);
                                thread.setName("delay_coupon-remind_consumer");
                                thread.setDaemon(Boolean.TRUE);
                                return thread;
                            })
                    .execute(() -> {
                        RBlockingDeque<String> blockingDeque = redissonClient.getBlockingDeque(REDIS_BLOCKING_DEQUE);
                        for (; ; ) {
                            try {
                                // 获取延迟队列待消费 Key
                                String key = blockingDeque.take();
                                if (stringRedisTemplate.hasKey(key)) {
                                    log.info("检查用户发送的通知消息Key：{} 未消费完成，开启重新投递", key);

                                    // Redis 中还存在该 Key，说明任务没被消费完，则可能是消费机器宕机了，重新投递消息
                                    CouponRemindDelayEvent couponTemplateRemindDelayEvent = JSONUtil.toBean(stringRedisTemplate.opsForValue().get(key), CouponRemindDelayEvent.class);
                                    couponRemindProducer.sendMessage(couponTemplateRemindDelayEvent);

                                    // 提醒用户后删除 Key
                                    stringRedisTemplate.delete(key);
                                }
                            } catch (Throwable ignored) {
                            }
                        }
                    });
        }
    }
}
