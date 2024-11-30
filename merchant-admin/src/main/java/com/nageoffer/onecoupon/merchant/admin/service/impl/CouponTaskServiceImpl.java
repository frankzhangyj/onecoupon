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
import cn.hutool.core.util.IdUtil;
import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import com.alibaba.excel.EasyExcel;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.nageoffer.onecoupon.framework.exception.ClientException;
import com.nageoffer.onecoupon.merchant.admin.common.context.UserContext;
import com.nageoffer.onecoupon.merchant.admin.common.enums.CouponTaskSendTypeEnum;
import com.nageoffer.onecoupon.merchant.admin.common.enums.CouponTaskStatusEnum;
import com.nageoffer.onecoupon.merchant.admin.dao.entity.CouponTaskDO;
import com.nageoffer.onecoupon.merchant.admin.dao.mapper.CouponTaskMapper;
import com.nageoffer.onecoupon.merchant.admin.dto.req.CouponTaskCreateReqDTO;
import com.nageoffer.onecoupon.merchant.admin.dto.resp.CouponTemplateQueryRespDTO;
import com.nageoffer.onecoupon.merchant.admin.mq.event.CouponTaskExecuteEvent;
import com.nageoffer.onecoupon.merchant.admin.mq.producer.CouponTaskActualExecuteProducer;
import com.nageoffer.onecoupon.merchant.admin.service.CouponTaskService;
import com.nageoffer.onecoupon.merchant.admin.service.CouponTemplateService;
import com.nageoffer.onecoupon.merchant.admin.service.handler.excel.RowCountListener;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;

/**
 *  难点 这一部分完全可以用rocketmq处理(将请求放到mq中 立即发送消息) 使用线程池处理具有不确定性，需要先执行再延迟确认。 注意bean的生命周期与注入时期
 *  如果是通过mq实现 则可以直接设置消息发送延迟时间 直接发送则不用设置延迟时间
 *
 * 优惠券推送业务逻辑实现层
 *  *      先是将推送任务存储到数据库 然后根据推送类型是立即发送(直接交给mq 在对应消费者中通过分发处理器处理)
 *  *      延迟发送(利用xxl-job定时检查是否存在任务已经到达延迟时间  通过xxl-job处理器再处理)
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class CouponTaskServiceImpl extends ServiceImpl<CouponTaskMapper, CouponTaskDO> implements CouponTaskService {

    private final CouponTemplateService couponTemplateService;
    private final CouponTaskMapper couponTaskMapper;
    private final RedissonClient redissonClient;
    private final CouponTaskActualExecuteProducer couponTaskActualExecuteProducer;
    // 难点 创建异步线程执行解析excel的长时间操作
    private final ExecutorService executorService = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors() << 1,
            60,
            TimeUnit.SECONDS,
            // 工作队列 直接将任务交给线程 不存储
            new SynchronousQueue<>(),
            // 拒绝策略 直接直接丢弃当前任务
            new ThreadPoolExecutor.DiscardPolicy()
    );

    /**
     * 先是将推送任务存储到数据库 然后根据推送类型是立即发送(直接交给mq 在对应消费者中通过分发处理器处理)
     * 延迟发送(利用xxl-job定时检查是否存在任务已经到达延迟时间  通过xxl-job处理器再处理)
     * @param requestParam 请求参数
     */
    @Transactional(rollbackFor = Exception.class) // 将数据库插入和线程池和延时队列的执行绑定 保证数据一致性
    @Override
    public void createCouponTask(CouponTaskCreateReqDTO requestParam) {
        // 验证非空参数
        // 验证参数是否正确，比如文件地址是否为我们期望的格式等
        // 验证参数依赖关系，比如选择定时发送，发送时间是否不为空等
        CouponTemplateQueryRespDTO couponTemplate = couponTemplateService.findCouponTemplateById(requestParam.getCouponTemplateId());
        if (couponTemplate == null) {
            throw new ClientException("优惠券模板不存在，请检查提交信息是否正确");
        }
        // ......

        // 构建优惠券推送任务数据库持久层实体
        CouponTaskDO couponTaskDO = BeanUtil.copyProperties(requestParam, CouponTaskDO.class);
        couponTaskDO.setBatchId(IdUtil.getSnowflakeNextId());
        couponTaskDO.setOperatorId(Long.parseLong(UserContext.getUserId()));
        couponTaskDO.setShopNumber(UserContext.getShopNumber());
        couponTaskDO.setStatus(
                Objects.equals(requestParam.getSendType(), CouponTaskSendTypeEnum.IMMEDIATE.getType())
                        ? CouponTaskStatusEnum.IN_PROGRESS.getStatus()
                        : CouponTaskStatusEnum.PENDING.getStatus()
        );

        //
        // 难点 hutool会直接将excel文件加载到内存中 getLastRowNum()来自POI库，POI 通常会缓存Excel文件中的数据以便于后续的操作和访问，所以不会被GC导致内存飙升。
        // 而EasyExcel逐行读取只将一行数据加载到内存 通过流式处理，将excel一行一行进行读取进行自增来读取总行记录数，所以内存有明显的降低。
        // 使用hutool读取excel总行数会导致严重内存占用
/*        // 读取 Excel 文件
        ExcelReader reader = ExcelUtil.getReader(requestParam.getFileAddress());

        // 获取总行数（包括标题行）
        int rowCount = reader.getRowCount();
        couponTaskDO.setSendNum(rowCount);*/

        // 通过 EasyExcel 监听器获取 Excel 中所有行数
//        RowCountListener listener = new RowCountListener();
//        EasyExcel.read(requestParam.getFileAddress(), listener).sheet().doRead();

//        int totalRows = listener.getRowCount();
//        couponTaskDO.setSendNum(totalRows);

//        // 保存优惠券推送任务记录到数据库 在异步线程中更新优惠券推送行数
        couponTaskMapper.insert(couponTaskDO);

        // 为什么需要统计行数？因为发送后需要比对所有优惠券是否都已发放到用户账号
        // 100 万数据大概需要 4 秒才能返回前端，如果加上验证将会时间更长，所以这里将最耗时的统计操作异步化
        JSONObject delayJsonObject = JSONObject
                .of("fileAddress", requestParam.getFileAddress(), "couponTaskId", couponTaskDO.getId());
        // submit不能抛出异常 调用实现runnable接口的参数
        // execute可以抛出异常 调用实现callable接口的参数 并且可以用future接收 获取执行结果
        executorService.execute(() -> refreshCouponTaskSendNum(delayJsonObject));

        // 假设刚把消息提交到线程池，突然应用宕机了，我们通过延迟队列进行兜底 Refresh
        // 使用阻塞双端队列作为延迟队列的基础结构 可以在请求空队列或插入满队列等待 避免频繁地轮询队列是否有元素，提高了资源利用率。
        RBlockingDeque<Object> blockingDeque = redissonClient.getBlockingDeque("COUPON_TASK_SEND_NUM_DELAY_QUEUE");
        // 延迟队列
        RDelayedQueue<Object> delayedQueue = redissonClient.getDelayedQueue(blockingDeque);
        // 这里延迟时间设置 20 秒，原因是我们笃定上面线程池 20 秒之内就能结束任务
        delayedQueue.offer(delayJsonObject, 20, TimeUnit.SECONDS);

        // 如果是立即发送任务，直接调用消息队列进行发送流程 延时任务通过xxl-job定时发送
        if (Objects.equals(requestParam.getSendType(), CouponTaskSendTypeEnum.IMMEDIATE.getType())) {
            // 执行优惠券推送业务，正式向用户发放优惠券
            CouponTaskExecuteEvent couponTaskExecuteEvent = CouponTaskExecuteEvent.builder()
                    .couponTaskId(couponTaskDO.getId())
                    .build();
            couponTaskActualExecuteProducer.sendMessage(couponTaskExecuteEvent);
        }
    }

    // 异步处理excel行数统计 并更新数据库
    private void refreshCouponTaskSendNum(JSONObject delayJsonObject) {
        // 通过 EasyExcel 监听器获取 Excel 中所有行数
        RowCountListener listener = new RowCountListener();
        EasyExcel.read(delayJsonObject.getString("fileAddress"), listener).sheet().doRead();
        int totalRows = listener.getRowCount();

        // 刷新优惠券推送记录中发送行数
        CouponTaskDO updateCouponTaskDO = CouponTaskDO.builder()
                .id(delayJsonObject.getLong("couponTaskId"))
                .sendNum(totalRows)
                .build();
        couponTaskMapper.updateById(updateCouponTaskDO);
    }

    /**
     * 因为静态内部类的 Bean 注入有问题，所以我们这里直接 new 对象运行即可
     * 如果按照上一版本的方式写，refreshCouponTaskSendNum 方法中 couponTaskMapper 为空
     * 在CouponTaskServiceImpl当前类构造函数和依赖注入完成后执行这个init方法 创建一个单独的线程执行延迟队列中的任务
     */
    @PostConstruct
    public void init() {
        new RefreshCouponTaskDelayQueueRunner(this, couponTaskMapper, redissonClient).run();
    }



    /**
     * 优惠券延迟刷新发送条数兜底消费者｜这是兜底策略，一般来说不会执行这段逻辑
     * 如果延迟消息没有持久化成功，或者 Redis 挂了怎么办？后续可以人工处理
     * 创建一个异步单线程单独一直循环处理延迟队列中的任务(消息提交到线程池，突然应用宕机了，我们通过延迟队列进行兜底)
     * CommandLineRunner类似责任链容器 都是在项目启动后运行后置任务run 但是此时的mapper没有创建ioc还没有完全初始化完成
     * 为什么不适用普通内部类 使用静态内部类
     * 使用普通类：在 Spring 中，普通内部类的实例化依赖于外部类的实例。当外部类（CouponTaskServiceImpl）在构造函数和依赖注入阶段完成后，Spring 容器还在初始化过程中，
     * 可能没有完全准备好所有的 Bean 可能会导致在外部类的依赖注入尚未完全完成时就尝试访问其成员
     * 使用静态类：静态内部类不依赖于外部类的实例，可以在外部类的实例化和依赖注入完成之前就被加载和初始化,当外部类依赖注入和构造函数完成 执行init方法进行执行单独线程
     */
    @Service
    @RequiredArgsConstructor
//    class RefreshCouponTaskDelayQueueRunner implements CommandLineRunner {
    static class RefreshCouponTaskDelayQueueRunner {

        private final CouponTaskServiceImpl couponTaskService;
        private final CouponTaskMapper couponTaskMapper;
        private final RedissonClient redissonClient;

//        @Override
//        public void run(String... args) throws Exception {
        public void run(String... args) {
            // 单线程线程在吃 一个单线程顺序执行延迟任务
            Executors.newSingleThreadExecutor(
                            runnable -> {
                                Thread thread = new Thread(runnable);
                                thread.setName("delay_coupon-task_send-num_consumer");
                                thread.setDaemon(Boolean.TRUE);
                                return thread;
                            })
                    .execute(() -> {
                        // 得到redis兜底的延迟队列
                        RBlockingDeque<JSONObject> blockingDeque = redissonClient.getBlockingDeque("COUPON_TASK_SEND_NUM_DELAY_QUEUE");
                        for (; ; ) {
                            try {
                                // 获取延迟队列已到达时间元素
                                JSONObject delayJsonObject = blockingDeque.take();
                                if (delayJsonObject != null) {
                                    // 获取优惠券推送记录，查看发送条数是否已经有值，有的话代表上面线程池已经处理完成，无需再处理
                                    CouponTaskDO couponTaskDO = couponTaskMapper.selectById(delayJsonObject.getLong("couponTaskId"));
                                    if (couponTaskDO.getSendNum() == null) {
//                                        refreshCouponTaskSendNum(delayJsonObject);
                                        couponTaskService.refreshCouponTaskSendNum(delayJsonObject);
                                    }
                                }
                            } catch (Throwable ignored) {
                            }
                        }
                    });
        }
    }
}
