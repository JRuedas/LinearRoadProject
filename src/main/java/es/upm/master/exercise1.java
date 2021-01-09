package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class exercise1 {
    public static void main(String[] args) throws Exception{

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataStream<String> text;

        // read the text file from given input path
        text = env.readTextFile(params.get("input"));
        System.out.println();


        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Integer>> mapStream = text.
                map(new MapFunction<String, Tuple4<Long, Integer, Integer, Integer>>() {
                    public Tuple4<Long, Integer, Integer, Integer> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple4<Long, Integer, Integer, Integer> out
                                = new Tuple4(Long.parseLong(fieldArray[0]), // Time
                                Integer.parseInt(fieldArray[3]), // Xway
                                Integer.parseInt(fieldArray[4]), // Lane
                                1); // Count
                        return out;
                    }
                })
                .filter(new FilterFunction<Tuple4<Long, Integer, Integer, Integer>>() {
            public boolean filter(Tuple4<Long, Integer, Integer, Integer> tuple) throws Exception {
                return tuple.f2 == 4;
            }
        });

        KeyedStream<Tuple4<Long, Integer, Integer, Integer>, Tuple> keyedStream = mapStream.
                assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple4<Long, Integer, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple4<Long, Integer, Integer, Integer> element) {
                                return element.f0*30*1000;
                            }
                        }
                ).keyBy(1);

        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Integer>> sumTumblingEventTimeWindows =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5))).apply(new CountVehicles());

        // emit result
        if (params.has("output")) {
            String file=params.get("output");
            sumTumblingEventTimeWindows.writeAsText(file);
        }

        // execute program
        env.execute("exercise1");
    }

    public static class CountVehicles implements WindowFunction<Tuple4<Long, Integer, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Long, Integer, Integer, Integer>> input, Collector<Tuple4<Long, Integer, Integer, Integer>> out) throws Exception {
            Iterator<Tuple4<Long, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple4<Long, Integer, Integer, Integer> first = iterator.next();
            Long time = 0L;
            Integer xway = 0;
            Integer lane = 0;
            Integer count = 0;
            if(first!=null){
                time = first.f0;
                xway = first.f1;
                lane = first.f2;
                count = first.f3;
            }
            while(iterator.hasNext()){
                Tuple4<Long, Integer, Integer, Integer> next = iterator.next();
                count += next.f3;
            }
            System.out.println("Hola");
            out.collect(new Tuple4<Long, Integer, Integer, Integer>(time, xway, lane, count));
        }
    }
}