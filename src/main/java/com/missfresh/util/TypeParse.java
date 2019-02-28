package com.missfresh.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

/**
 * @author wangzhihua
 * @date 2019-02-28 17:25
 */
public class TypeParse {
    public  static Object typeParse(String str, TypeInformation typeInfo){
        Object data;
       if (typeInfo==Types.INT){
           data = Integer.parseInt(str);
       }else if(typeInfo==Types.LONG){
           data = Long.parseLong(str);
       }else if(typeInfo==Types.DOUBLE){
           data = Double.parseDouble(str);
       }else if(typeInfo==Types.FLOAT){
           data = Float.parseFloat(str);
       }else {
           data=str;
       }
      return data;
    }
}
