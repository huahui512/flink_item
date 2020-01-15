package com.huahui.item;

/**
 * @author wangzhihua
 * @date 2018-12-25 14:49
 */
public class ItemViewCount {
    public long itemId; // 商品ID
    public long windowEnd; // 窗口结束时间戳
    public long viewCount; // 商品的点击量

    public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
        ItemViewCount result = new ItemViewCount();
        result.itemId = itemId;
        result.windowEnd = windowEnd;
        result.viewCount = viewCount;
        return result;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId=" + itemId +
                ", windowEnd=" + windowEnd +
                ", viewCount=" + viewCount +
                '}';
    }
}
