package com.ywf.cal;

import com.ywf.sources.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName ReadDataFromMySqlSource
 * @Description 从自定义source读取数据
 * @Author YangWeiFeng
 * @Date 2019/12/8 16:43
 * @Version 1.0
 **/
public class ReadDataFromMySqlSource {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SourceFromMySQL()).print();
        try{
            env.execute("Flink add data sourc");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
