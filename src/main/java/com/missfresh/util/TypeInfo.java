package com.missfresh.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;

/**
 * @author wangzhihua
 * @date 2019-02-27 10:51
 */
public class TypeInfo {

    public static TypeInformation getType(String type) {
        switch (type)
        {
            case "int":
                return Types.INT();
            case "long":
                return Types.LONG();

            case "double":
                return Types.DOUBLE();

            case "float":
                return Types.FLOAT();

            default:
                return Types.STRING();
        }
    }
}