package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.operators.SortedGrouping;

import java.util.*;

public class exercise2 {
    public static void main(String[] args) throws Exception{
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataStream<String> text;

        // read the text file from given input path
        text = env.readTextFile(params.get("input"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>> mapStream = text.
                map(new MapFunction<String, Tuple6<Long, Integer, Integer, Integer, Integer, Integer>>() {
                    public Tuple6<Long, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception{

                        String[] fieldArray = in.split(",");
                        Tuple6<Long, Integer, Integer, Integer, Integer, Integer> out
                                = new Tuple6(Long.parseLong(fieldArray[0]), // Time
                                Integer.parseInt(fieldArray[1]), // VID
                                Integer.parseInt(fieldArray[2]), // Spd
                                Integer.parseInt(fieldArray[3]), // Xway
                                Integer.parseInt(fieldArray[5]), // Dir
                                Integer.parseInt(fieldArray[6]) // Seg
                                );
                        return out;
                    }
                })
                .filter(new FilterFunction<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>>() {
                    public boolean filter(Tuple6<Long, Integer, Integer, Integer, Integer, Integer> tuple) throws Exception {
                        return tuple.f4 == 0 && tuple.f5 >= Integer.parseInt(params.get("startSegment")) &&
                                tuple.f5 <= Integer.parseInt(params.get("endSegment"));
                    }
                });

        KeyedStream<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStream = mapStream.
                assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple6<Long, Integer, Integer, Integer, Integer, Integer> element) {
                                return element.f0*1000;
                            }
                        }
                ).keyBy(3, 1);

        // Average speed
        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Double>> sumTumblingEventTimeWindows =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(Integer.parseInt(params.get("time"))))).apply(new exercise2.ComputeAvgSpd());

        // Filter by average speed
        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Double>> filterStream2 =
                sumTumblingEventTimeWindows.filter(new FilterFunction<Tuple4<Long, Integer, Integer, Double>>() {
                    public boolean filter(Tuple4<Long, Integer, Integer, Double> tuple) throws Exception {
                        return tuple.f3 > Integer.parseInt(params.get("speed"));
                    }
                });

        // Group by Xway
        KeyedStream<Tuple4<Long, Integer, Integer, Double>, Tuple> keyedStream2 = filterStream2.keyBy(2);

        // Result
        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, String>> sumTumblingEventTimeWindows2 =
                keyedStream2.window(TumblingEventTimeWindows.of(Time.seconds(Integer.parseInt(params.get("time"))))).apply(new exercise2.GetVIDs());

        // emit result
        if (params.has("output")) {
            String file=params.get("output");
            sumTumblingEventTimeWindows2.writeAsCsv(file, FileSystem.WriteMode.OVERWRITE);
        }

        // execute program
        env.execute("exercise2");
    }

    public static class ComputeAvgSpd implements WindowFunction<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>,
            Tuple4<Long, Integer, Integer, Double>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow,
                          Iterable<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>> input,
                          Collector<Tuple4<Long, Integer, Integer, Double>> out) {
            Iterator<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple6<Long, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
            Long time = 0L;
            Integer VID = 0;
            Integer xway = 0;
            Integer acc = 0;
            Integer count = 1;
            if(first!=null){
                time = first.f0;
                VID = first.f1;
                xway = first.f3;
                acc = first.f2;
            }
            while(iterator.hasNext()){
                Tuple6<Long, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                acc += next.f2;
                count++;
            }
            out.collect(new Tuple4<Long, Integer, Integer, Double>(time, VID, xway, ((double)acc)/count));
        }
    }

    public static class GetVIDs implements WindowFunction<Tuple4<Long, Integer, Integer, Double>,
            Tuple4<Long, Integer, Integer, String>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow,
                          Iterable<Tuple4<Long, Integer, Integer, Double>> input,
                          Collector<Tuple4<Long, Integer, Integer, String>> out) {
            Iterator<Tuple4<Long, Integer, Integer, Double>> iterator = input.iterator();
            Tuple4<Long, Integer, Integer, Double> first = iterator.next();
            Long time = 0L;
            Integer xway = 0;
            StringBuffer sb = new StringBuffer("[ ");
            Integer count = 1;
            if(first!=null){
                time = first.f0;
                xway = first.f2;
                sb.append(first.f1);
            }
            while(iterator.hasNext()){
                Tuple4<Long, Integer, Integer, Double> next = iterator.next();
                Long next_time = next.f0;
                if (next_time < time)
                    time = next_time;
                sb.append(" - ");
                sb.append(next.f1);
                count++;
            }
            sb.append(" ]");
            out.collect(new Tuple4<Long, Integer, Integer, String>(time, xway, count, sb.toString()));
        }
    }
}