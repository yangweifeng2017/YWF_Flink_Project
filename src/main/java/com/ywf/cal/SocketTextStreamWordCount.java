package com.ywf.cal;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;

/**
 * Created by weifeng on 2019/11/18
 * flink run -c com.zhisheng.flink.SocketTextStreamWordCount original-word-count-1.0-SNAPSHOT.jar 127.0.0.1 9000
 */
public class SocketTextStreamWordCount {
    public static void main(String[] args) throws Exception {
        //参数检查
        if (args.length != 2) {
            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }
        String hostname = args[0];
        Integer port = Integer.parseInt(args[1]);
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据 nc -l 9000
        DataStreamSource<String> stream = env.socketTextStream(hostname, port);
        //计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplitter())
                .keyBy(0) //以数组的第一个元素作为分组的key group by
                .sum(1); //对数组的第二个元素进行求和

       // sum.print();
        sum.addSink(new PrintSinkFunction<>()); //添加输出sink

        try{
            env.execute("Java WordCount from SocketTextStream Example");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            System.out.println("实时流数据异常");
        }

    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            //匹配数字和字母下划线的多个字符
            String[] tokens = s.toLowerCase().split("\\W+");
            for (String token: tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
