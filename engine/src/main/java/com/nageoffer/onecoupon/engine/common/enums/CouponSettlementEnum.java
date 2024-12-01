package com.nageoffer.onecoupon.engine.common.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum CouponSettlementEnum {
    /**
     * 已锁定
     */
    LOCKING(0),

    /**
     * 已取消
     */
    CANCELED(1),

    /**
     * 已支付
     */
    PAYED(2),

    /**
     * 已退款
     */
    REFUND(3);

    @Getter
    private final int code;
}
