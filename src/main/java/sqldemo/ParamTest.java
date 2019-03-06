package sqldemo;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @author wangzhihua
 * @date 2019-03-06 14:59
 */
public class ParamTest {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println(params.get("topic"));
        System.out.println(params.get("topic2"));
    }
}
