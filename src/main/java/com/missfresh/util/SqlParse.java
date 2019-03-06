package com.missfresh.util;

import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangzhihua
 * @date 2019-03-01 15:47
 */
public class SqlParse {

    public  static Map sqlParse(String sql , Row value){
        //去除所有空格，包括首尾、中间
        String sqlInfo = sql.replace(" ", "");
        String[] split1= sqlInfo.split("select");
        String str = split1[1];
        String[] split2 = str.split("from");
        String str2 = split2[0];
        String[] split3 = str2.split(",");

        Map<Object, Object> map = new HashMap<>();
        //生成MD5的字符串
        StringBuffer sb=new StringBuffer();
        //字段的长度
        int fielsSize= split3.length;

        for(int i=0;i<fielsSize;i++){
            map.put(getRealField(split3[i]),value.getField(i));
            sb.append(value.getField(i).toString());
        }
        String id = sb.toString();
        String md5Id = MD5Util.encryption(id);
        map.put("id",md5Id );


    return  map;

    }

    public  static String  getRealField(String str){
        String field;
        if (str.contains("as")){
            String[] split= str.split("as");
            field=split[1];
        }else {
            field=str;
        }

        return field;
    }


    public static void main(String[] args) {
        Row value=new Row(2);
        value.setField(0,"pv");
        value.setField(1,34);
        String str="select count(distinct userId) as uv ,"+"\n"+"behavior from userTable group by behavior";
        Map map = sqlParse(str, value);
        System.out.println(map.get("uv"));


    }




}
