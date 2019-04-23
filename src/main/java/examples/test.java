package examples;

import java.util.HashMap;

/**
 * @author wangzhihua
 * @date 2019-04-03 20:52
 */
public class test {
    public static void main(String[] args) {
        String host = "10.2.40.15,10.2.40.1,10.2.40.10";
        int port = 9200;
        String schema = "http";
        EsBulkUtil esBulkUtil = new EsBulkUtil();
        //初始化client
        esBulkUtil.initHighLevelClient(host,port,schema);
        //初始化bulkProcessor
        esBulkUtil.initBulkProcessor();
        HashMap<String, String> map = new HashMap<>();
        map.put("name","wzh1");
        map.put("city","beijing1");
        map.put("sex","men1");
        //插入数据
        esBulkUtil.bulkAdd("es_flink2","user","1",map);
        esBulkUtil.flush();
        esBulkUtil.close();
    }
}
