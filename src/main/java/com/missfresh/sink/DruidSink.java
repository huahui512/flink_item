package com.missfresh.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import com.metamx.tranquility.config.TranquilityConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.FutureEventListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.scala.log;
import scala.runtime.BoxedUnit;

import java.io.InputStream;
import java.util.Map;

/**
 * @author wangzhihua
 * @date 2019-08-04 17:28
 */
public class DruidSink  extends RichSinkFunction<String> {
     InputStream configStream=null;
     TranquilityConfig<PropertiesBasedConfig> config=null;
     DataSourceConfig<PropertiesBasedConfig> wikipediaConfig=null;
     Tranquilizer<Map<String, Object>> sender=null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        configStream = DruidSink.class.getClassLoader().getResourceAsStream("example.json");
        config = TranquilityConfig.read(configStream);
        wikipediaConfig = config.getDataSource("wikipedia");
        final Tranquilizer<Map<String, Object>> sender = DruidBeams.fromConfig(wikipediaConfig)
                .buildTranquilizer(wikipediaConfig.tranquilizerBuilder());
    }

    @Override
    public void invoke(String jsondata, Context context) throws Exception {
        Map maps = (Map)JSON.parse(jsondata);
        // Asynchronously send event to Druid:
        sender.send(maps).addEventListener(
                new FutureEventListener<BoxedUnit>()
                {
                    @Override
                    public void onSuccess(BoxedUnit value) {
                        System.out.println("Sent message: %s"+jsondata);
                    }

                    @Override
                    public void onFailure(Throwable e)
                    {
                        if (e instanceof MessageDroppedException) {
                            System.out.println(e+ "Dropped message: %s" + jsondata);
                        } else {
                            System.out.println(e+"Failed to send message: %s"+jsondata);
                        }
                    }
                }
        );

    }

    @Override
    public void close() throws Exception {
        sender.flush();
        sender.stop();
        super.close();
    }
}
