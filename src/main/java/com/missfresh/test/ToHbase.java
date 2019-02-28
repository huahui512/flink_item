package com.missfresh.test;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;

public class ToHbase {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> ds = env.fromElements("a", "b", "c");

        ds.map(value -> {
            System.out.println("333333333"+value);
            return "";
        }).output(new t());


        ds.print();
//        env.execute();


    }
    static class t implements OutputFormat<String>{

        @Override
        public void configure(Configuration parameters) {

        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {

        }

        @Override
        public void writeRecord(String record) throws IOException {
            System.out.println("writeRecord");
        }

        @Override
        public void close() throws IOException {

        }
    }
}
