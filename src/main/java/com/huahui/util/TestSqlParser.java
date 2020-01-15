package com.huahui.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wangzhihua
 * @date 2019-05-14 20:02
 */
public class TestSqlParser {

    public static void main(String[] args) {
        String sql = "select t1.id as order_id,t1.order_no as order_no,t1.trade_no as trade_no,t1.pay_time as pay_time,substr(t1.pay_time,1,10) as pay_date_id_10,t2.product_id as product_id,get_json_object(t1.new_receiver_message,'$.product_sku') as product_code,t2.product_name as product_name ,t1.quantity as quantity,get_json_object(t1.new_receiver_message,'$.supplier') as supplier,get_json_object(t1.new_receiver_message,'$.presale_type') as presale_type,get_json_object(t1.new_receiver_message,'$.can_use_voucher') as can_use_voucher,get_json_object(t1.new_receiver_message,'$.free_shipping') as free_shipping  ,get_json_object(t1.new_receiver_message,'$.price')/100 as price,get_json_object(t1.new_receiver_message,'$.vip_price')/100 as vip_price ,get_json_object(t1.new_receiver_message,'$.payment_price')/100 as payment_price,get_json_object(t1.new_receiver_message,'$.second_discount_amount_total')/100 as second_discount_amount_total,'' as second_discount_amount_total_new,get_json_object(t1.new_receiver_message,'$.special_price')/100 as special_price  ,get_json_object(t1.new_receiver_message,'$.special_quantity') as special_quantit,get_json_object(t1.new_receiver_message,'$.city_id') as city_id,get_json_object(t1.new_receiver_message,'$.day_limit') as day_limit,get_json_object(t1.new_receiver_message,'$.amount_limit') as amount_limit   ,get_json_object(t1.new_receiver_message,'$.promotion_id') as promotion_id ,get_json_object(t1.new_receiver_message,'$.limit') as limit,get_json_object(t1.new_receiver_message,'$.amount_limit_cache_key') as amount_limit_cache_key      ,t1.vip_exclusive as vip_exclusive ,t1.p_type as p_type ,t1.p_price/100 as p_price ,t1.p_vip_price/100 as p_vip_price ,t1.p_dis_price/100 as p_dis_price ,t1.p_dis_vip_price/100 as p_dis_vip_price ,get_json_object(t1.new_receiver_message,'$.total_pay_price')/100 as total_pay_price ,get_json_object(t1.new_receiver_message,'$.vip_price_save_amount')/100 as vip_price_save_amount,get_json_object(t1.new_receiver_message,'$.p_voucher_save_price')/100  as p_voucher_save_price,get_json_object(t1.new_receiver_message,'$.present_type') as present_type,get_json_object(t1.new_receiver_message,'$.gift_sku') as gift_sku ,get_json_object(t1.new_receiver_message,'$.sign_doc') as sign_doc ,get_json_object(t1.new_receiver_message,'$.tomorrow_send')as tomorrow_send,t1.vip_flag as vip_flag   ,get_json_object(t1.new_receiver_message,'$.is_send') as is_send,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as dt_etl_created from bi_process_mid.odi_mid_order_products_discount_his_tmp t1 left join bi_dw_mryx.dim_product_his t2 on t1.sku_code=t2.product_code".trim();
        System.out.println(sqlParse(sql,"bi_dw_mryx.dim_product_his"));

    }



    private static String sqlParse(String sql,String dimTable){
        List<Integer> selectPosList = getKeyWordsPos(sql, "select");
        List<Integer> fromPosList = getKeyWordsPos(sql, "from");
        List<Integer> joinPosList = getKeyWordsPos(sql, "join");
        List<Integer> onPosList = getKeyWordsPos(sql, "on");
        String result="";
        if (sql.indexOf(dimTable) > 0) {
            int dimPos = sql.indexOf(dimTable);
            Integer closeFrom = findClosePos(fromPosList, dimPos);
            //获取维表对应的from
            //获取维表对应的select
            Integer closeSelect = findClosePos(selectPosList, dimPos);
            //获取维表对应的select
            Integer closeOn = findKeyOn(onPosList, dimPos);
            //获取维表和流表select的字段
            String fieldsSubSql = sql.substring(selectPosList.get(closeSelect) + 6, fromPosList.get(closeFrom));
            String[] fieldsArr = fieldsSubSql.trim().split(",");
            //获取from之后的sql
            String fromSubSql = sql.substring(fromPosList.get(closeFrom) + 4);
            String subSql = sql.substring(0, fromPosList.get(closeFrom) + 4);

            String[] segArr = fromSubSql.trim().split("\\s+");
            int index = 0;
            String[] tables = {"", ""};
            String StreamT = "";
            String dimT = "";
            String onClause = "";

            //获取流表和维表名封装到数组中
            for (int i = 0; i < segArr.length - 1; i++) {
                if (i == 0) {
                    tables[0] = segArr[i];
                    StreamT = segArr[1];
                } else {
                    if ("join".equals(segArr[i])) {

                        tables[1] = segArr[i + 1];
                        dimT = segArr[i + 2];
                    }
                }
            }
            String joinCondition = fromSubSql.split("on")[1];
            result = joinCondition.split("=")[0].split("\\.")[1];
            tables[1]="LATERAL TABLE (" +  tables[1]+ "(" + StreamT + ".proctime" + "))";

           // fromSubSql = tables[0] + " " + StreamT + "," + " " + "LATERAL TABLE (" + dimT + "(" + StreamT + ".proctime" + "))" + " " + dimT + " where" + joinCondition;
            //result = subSql +" " + fromSubSql;
        }
        return  result;
    }

    //获取关键字所在的每一个位点
    private static List<Integer> getKeyWordsPos(final String sql, final String keyWords) {
        List<Integer> keyWordsPost = new ArrayList<Integer>();
        int spos = sql.indexOf(keyWords);
        while (spos > -1) {
            // on 这个关键字非常容易重复, 只需要真正的 on 表达式.
            if ("on".equalsIgnoreCase(keyWords)) {
                if (spos > 0 && String.valueOf(sql.charAt(spos - 1)).equals(" ")) {
                    keyWordsPost.add(spos);
                }
            } else {
                keyWordsPost.add(spos);
            }
            spos = sql.indexOf(keyWords, spos + 1);
        }
        return keyWordsPost;
    }
    //找到最进的位点
    private static int findClosePos(final List<Integer> posList, int pos) {
        for (int index = posList.size() - 1; index >= 0; index--) {
            if (posList.get(index) < pos) {
                return index;
            }
        }
        return -1;
    }
    //找到最进的位点
    private static int findKeyOn(final List<Integer> posList, int pos) {
        for (int index =0 ; index <=posList.size() - 1; index++) {
            if (posList.get(index) >pos) {
                return index;
            }
        }
        return -1;
    }
}
