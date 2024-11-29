-- 定义最大值和位数 用户领券次数最大9999
local SECOND_FIELD_BITS = 14

-- 将两个字段组合成一个int
-- 相当于将第一个字段左移13位 加上第二个字段
local function combineFields(firstField, secondField)
    local firstFieldValue = firstField and 1 or 0
    return (firstFieldValue * 2 ^ SECOND_FIELD_BITS) + secondField
end

-- Lua脚本开始
local key = KEYS[1] -- Redis Key
local userSetKey = KEYS[2] -- 用户领券 Set 的 Key
local userIdAndRowNum = ARGV[1] -- 用户 ID 和 Excel 所在行数

-- 判断用户领券次数是否超过限制
local userCouponCount = tonumber(redis.call('GET', KEYS[3]))
if userCouponCount >= tonumber(ARGV[2]) then
    return combineFields(2, userCouponCount) -- 用户已经达到领取上限
end

-- 获取库存
local stock = tonumber(redis.call('HGET', key, 'stock'))

-- 检查库存是否大于0
if stock == nil or stock <= 0 then
    return combineFields(1, redis.call('SCARD', userSetKey)) -- 没库存
end

-- 自减库存
redis.call('HINCRBY', key, 'stock', -1)

-- 添加用户到领券集合
redis.call('SADD', userSetKey, userIdAndRowNum)

-- 获取用户领券集合的长度
local userSetLength = redis.call('SCARD', userSetKey)

-- 返回结果
return combineFields(0, userSetLength)
