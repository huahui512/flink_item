package com.huahui.sqldemo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author wangzhihua
 * @date 2019-05-05 11:30
 */
public class UdfDemo1 extends ScalarFunction {

    public String eval(String s) {
        return s+"udf";
    }


    public static void main(String[] args) {


        String str="{\"create_time\":\"-\",\"remote_user\":\"-\",\"time_local\":\"-\",\"time_local_date\":\"-\",\"time_local_hour\":\"-\",\"time_local_minute\":\"-\",\"http_referer\":\"-\",\"http_user_agent\":\"-\",\"param\":\"{\\\"abTestInfo\\\":[{\\\"code\\\":1,\\\"group_id\\\":\\\"g_0001\\\",\\\"message\\\":\\\"success\\\",\\\"key\\\":\\\"exp_0001\\\"},{\\\"code\\\":1,\\\"group_id\\\":\\\"gf7wndho\\\",\\\"message\\\":\\\"success\\\",\\\"key\\\":\\\"erz8b6kk\\\"}]}\",\"user_id\":\"100589951\",\"business\":\"mryt\",\"platform\":\"iOS\",\"version\":\"2.4.0\",\"device_id\":\"40651AA5-C057-4B77-AE14-B7F86186D800\",\"device_source_id\":\"40651AA5-C057-4B77-AE14-B7F86186D800\",\"device_model\":\"iPhone 7\",\"device_os\":\"11.4.1\",\"device_size\":\"0*0\",\"address_code\":\"0\",\"station_code\":\"-\",\"anchor_id\":\"155706451300040651AA5-C057-4B77-AE14-B7F86186D800\",\"from_source\":\"-\",\"abtest_id\":\"[{\\\"beginTime\\\":null,\\\"code\\\":1,\\\"endTime\\\":null,\\\"group_id\\\":\\\"g_0001\\\",\\\"key\\\":\\\"exp_0001\\\",\\\"message\\\":\\\"success\\\",\\\"status\\\":null},{\\\"beginTime\\\":null,\\\"code\\\":1,\\\"endTime\\\":null,\\\"group_id\\\":\\\"gf7wndho\\\",\\\"key\\\":\\\"erz8b6kk\\\",\\\"message\\\":\\\"success\\\",\\\"status\\\":null}]\",\"common_group_id\":\"-\",\"abtest_key\":\"erz8b6kk\",\"group_id\":\"gf7wndho\",\"label\":\"Redpacket_inviter\",\"event\":\"Redpacket_inviter_show\",\"action\":\"-\",\"module_standby\":\"-\",\"promotion_id\":\"-\",\"channel\":\"-\",\"second_channel\":\"-\",\"device_time\":\"1557154653000\",\"device_date\":\"2019-05-06\",\"device_time_hour\":\"2019-05-06 22\",\"device_time_minute\":\"2019-05-06 22:57\",\"type\":\"0\"}";


        JSONObject jsonObject = JSON.parseObject(str);
        String time_local = jsonObject.getString("time_local");

        String replace = str.replace("\"" + "time_local" + "\"" + ":" + "\"" + time_local + "\"", "\"" + "time_local" + "\"" + ":" + "\""+"ttt"+"\"");


        System.out.println(replace);
        }


}
