package com.missfresh.util;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangzhihua
 * @date 2019-03-05 14:09
 */
public class Parameter {
    public  static Map getParm(String[] args){
        HashMap<String, String> map = new HashMap<>();
        if (args.length == 1) {
            //kafka的连接信息
            map.put("filePath",args[0]);


        } else {
            System.exit(1);
        }
        return map;

    }
}
