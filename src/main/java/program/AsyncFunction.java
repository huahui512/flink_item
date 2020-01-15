package program;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;

/**
 * @author wangzhihua
 * @date 2019-04-02 11:17
 */
public class AsyncFunction  extends RichAsyncFunction<String,String> {
    private Connection connection = null;
    private PreparedStatement ps = null;
    private static final Logger logger = LoggerFactory.getLogger(AsyncFunction.class);
    //初始化连接
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
        connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/bifrost?useSSL=false", "root", "518189aA");//获取连接
        ps  = connection.prepareStatement("select job_name from rt_job");

    }
    //数据异步调用
    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        try {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String name = resultSet.getString("job_name");
                System.out.println(name);
               resultFuture.complete(Collections.singleton(name));//发送结果，结果是tuple2类型，2表示两个元素，可根据实际情况选择
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }

    @Override
    public void timeout(String input, ResultFuture resultFuture) throws Exception {

    }


    @Override
    public void close() throws Exception {
        super.close();
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }

}
