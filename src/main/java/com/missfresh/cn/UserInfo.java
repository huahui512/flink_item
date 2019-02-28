package com.missfresh.cn;

/**
 * @author wangzhihua
 * @date 2018-12-25 11:09
 */
public class UserInfo {
    public long userId;         // 用户ID
    public long itemId;         // 商品ID
    public long  categoryId;      // 商品类目ID
    public String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
    public long timestamp;      // 行为发生的时间戳，单位秒


    public UserInfo(long userId, long itemId, long categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }
}
