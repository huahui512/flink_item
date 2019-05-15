package sqldemo;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author wangzhihua
 * @date 2019-05-05 11:30
 */
public class UdfDemo1 extends ScalarFunction {

    public String eval(String s) {
        return s+"udf";
    }

}
