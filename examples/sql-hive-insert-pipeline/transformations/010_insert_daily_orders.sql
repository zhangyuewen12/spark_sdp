INSERT INTO test_order_dwd
PARTITION (dt)

SELECT
    o.order_id,
    o.user_id,

    -- 用户维度
    u.user_name,
    u.gender,
    u.age,
    u.user_level,

    -- 商品维度
    o.product_id,
    p.product_name,
    p.category,
    p.brand,

    -- 时间加工
    o.order_time,
    TO_DATE(o.order_time) AS order_date,

    -- 原始金额
    o.quantity,
    o.unit_price,
    o.discount,

    -- 原价
    CAST(
        o.quantity * o.unit_price
        AS DECIMAL(18,2)
    ) AS original_amount,

    -- 折扣金额
    CAST(
        CASE
            WHEN o.quantity * o.unit_price > 10000
                THEN o.discount + 100
            WHEN o.quantity * o.unit_price > 5000
                THEN o.discount + 50
            ELSE o.discount
        END
        AS DECIMAL(18,2)
    ) AS discount_amount,

    -- 实际金额
    CAST(
        o.quantity * o.unit_price
        -
        CASE
            WHEN o.quantity * o.unit_price > 10000
                THEN o.discount + 100
            WHEN o.quantity * o.unit_price > 5000
                THEN o.discount + 50
            ELSE o.discount
        END
        AS DECIMAL(18,2)
    ) AS actual_amount,

    o.pay_type,

    -- 状态转换
    CASE o.status
        WHEN 'SUCCESS' THEN '已支付'
        WHEN 'REFUND'  THEN '已退款'
        WHEN 'CANCEL'  THEN '已取消'
        ELSE '未知'
    END AS order_status,

    -- 地址加工
    o.province,
    u.city,

    -- 金额分层
    CASE
        WHEN o.quantity * o.unit_price >= 20000 THEN 'HIGH'
        WHEN o.quantity * o.unit_price >= 10000 THEN 'MEDIUM'
        WHEN o.quantity * o.unit_price >= 5000  THEN 'LOW'
        ELSE 'NORMAL'
    END AS amount_level,

    -- 用户分层
    CASE
        WHEN u.user_level = 'VIP' AND u.age >= 35
            THEN 'VIP_HIGH_AGE'
        WHEN u.user_level = 'VIP'
            THEN 'VIP'
        WHEN u.age >= 35
            THEN 'NORMAL_HIGH_AGE'
        ELSE 'NORMAL'
    END AS user_type,

    -- 分区字段
    DATE_FORMAT(o.order_time, 'yyyy-MM-dd') AS dt

FROM test_order_source o

LEFT JOIN test_user_dim u
    ON o.user_id = u.user_id

LEFT JOIN test_product_dim p
    ON o.product_id = p.product_id

WHERE o.status <> 'CANCEL';