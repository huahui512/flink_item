package examples;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.math.BigDecimal;

import java.util.Date;
import java.util.Map;
import java.util.Set;

/**
 * @author wangzhihua
 * @date 2019-04-23 16:42
 */
public class Data2Parquet {
    public static final Gson gson = new Gson();

    public static void main(String[] args) throws Exception{
        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("/Users/apple/Downloads/code/code/flink_test_demo/src/resources/json.txt");
        PojoTypeInfo<CSModel> pojoTypeInfo = (PojoTypeInfo<CSModel>) TypeInformation.of(CSModel.class);


        SingleOutputStreamOperator<CSModel> rs = source.map(new MapFunction<String, CSModel>() {
            @Override
            public CSModel map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                CSModel csModel = jsonObject.toJavaObject(CSModel.class);

                return csModel;

            }
        }).returns(pojoTypeInfo);
        StreamingFileSink<CSModel> sink = StreamingFileSink.forBulkFormat(new Path("/Users/apple/Downloads/code/code/flink_test_demo/src/resources/output.txt"), new BulkWriter.Factory<CSModel>() {
            @Override
            public BulkWriter<CSModel> create(FSDataOutputStream outputStream) throws IOException {
                ParquetWriterFactory<CSModel> factory = ParquetAvroWriters.forReflectRecord(CSModel.class);
                BulkWriter<CSModel> writer = factory.create(outputStream);
                return writer;
            }
        })
                .build();
        rs.print();
        StreamingFileSink<CSModel> fileSink = StreamingFileSink.forBulkFormat(new Path("/Users/apple/Downloads/code/code/flink_test_demo/src/resources/output.txt"), ParquetAvroWriters.forReflectRecord(CSModel.class)).build();

        rs.addSink(sink);


        env.execute("ssss");

    }
    public static class CSModel{
        public String evcsId;
        public String chargeHeaderId;
        public Integer chargeHeaderStatus;
        public String orderId;

        public CSModel() {
        }

        public Double startElectricity;
        public Double endElectricity;

        public String getEvcsId() {
            return evcsId;
        }

        public void setEvcsId(String evcsId) {
            this.evcsId = evcsId;
        }

        public String getChargeHeaderId() {
            return chargeHeaderId;
        }

        public void setChargeHeaderId(String chargeHeaderId) {
            this.chargeHeaderId = chargeHeaderId;
        }

        public Integer getChargeHeaderStatus() {
            return chargeHeaderStatus;
        }

        public void setChargeHeaderStatus(Integer chargeHeaderStatus) {
            this.chargeHeaderStatus = chargeHeaderStatus;
        }

        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public Double getStartElectricity() {
            return startElectricity;
        }

        public void setStartElectricity(Double startElectricity) {
            this.startElectricity = startElectricity;
        }

        public Double getEndElectricity() {
            return endElectricity;
        }

        public void setEndElectricity(Double endElectricity) {
            this.endElectricity = endElectricity;
        }

        public Double getTotalCharged() {
            return totalCharged;
        }

        public void setTotalCharged(Double totalCharged) {
            this.totalCharged = totalCharged;
        }

        public Double getBatteryChargeVoltage() {
            return batteryChargeVoltage;
        }

        public void setBatteryChargeVoltage(Double batteryChargeVoltage) {
            this.batteryChargeVoltage = batteryChargeVoltage;
        }

        public Double getBatteryChargeCurrent() {
            return batteryChargeCurrent;
        }

        public void setBatteryChargeCurrent(Double batteryChargeCurrent) {
            this.batteryChargeCurrent = batteryChargeCurrent;
        }

        public Double getOutPutPower() {
            return outPutPower;
        }

        public void setOutPutPower(Double outPutPower) {
            this.outPutPower = outPutPower;
        }

        public String getTotalChargeFee() {
            return totalChargeFee;
        }

        public void setTotalChargeFee(String totalChargeFee) {
            this.totalChargeFee = totalChargeFee;
        }

        public String getTotalServiceFee() {
            return totalServiceFee;
        }

        public void setTotalServiceFee(String totalServiceFee) {
            this.totalServiceFee = totalServiceFee;
        }

        public Date getCsEventTime() {
            return csEventTime;
        }

        public void setCsEventTime(Date csEventTime) {
            this.csEventTime = csEventTime;
        }

        public Double totalCharged;
        public Double batteryChargeVoltage;
        public Double batteryChargeCurrent;
        public Double outPutPower;
        public String totalChargeFee;
        public String totalServiceFee;
        public Date csEventTime;
    }



}

