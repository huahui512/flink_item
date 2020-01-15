package com.huahui.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

/**
 * @author wangzhihua
 * @date 2019-02-27 10:47
 */
public class MyRowInfo {

    //infoStr：behavior&String,userId&String,categoryId&String,itemId&String,timestamp&String
    public static RowTypeInfo getRowTypeInfo(String infoStr){
        //解析字符串，通过循环把字段名和字段类型存放到对应的数组中
        String[] split1 = infoStr.split(",");
        String[] info=new String[split1.length];
        TypeInformation[] types=new TypeInformation[split1.length];
        for (int i=0;i<split1.length;i++) {
            String strName = split1[i].split("&")[0];
            info[i]=strName;
            String typeInfo = split1[i].split("&")[1];
            types[i]=TypeInfo.getType(typeInfo);
        }
        //返回Row的字段名和字段类型
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, info);

      return  rowTypeInfo;
    }

}
