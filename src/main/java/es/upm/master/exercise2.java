package es.upm.master;

import org.apache.commons.collections.map.HashedMap;
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

import java.util.*;

public class exercise2 {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // read the text file from given input path
        DataStream<String> text = env.readTextFile(params.get("input"));

        int time = Integer.parseInt(params.get("time"));
        final int speed = Integer.parseInt(params.get("time"));
        final int startSegment = Integer.parseInt(params.get("startSegment"));
        final int endSegment = Integer.parseInt(params.get("endSegment"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>> mapStream = text
                .map(new MapFunction<String, Tuple6<Long, Integer, Integer, Integer, Integer, Integer>>() {

                    public Tuple6<Long, Integer, Integer, Integer, Integer, Integer> map(String in) {

                        String[] fieldArray = in.split(",");
                        return new Tuple6<Long, Integer, Integer, Integer, Integer, Integer>(
                                Long.parseLong(fieldArray[0]), // Time
                                Integer.parseInt(fieldArray[1]), // VID
                                Integer.parseInt(fieldArray[2]), // Speed
                                Integer.parseInt(fieldArray[3]), // Xway
                                Integer.parseInt(fieldArray[5]), // Dir
                                Integer.parseInt(fieldArray[6]) // Seg
                        );
                    }
                });

        SingleOutputStreamOperator<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>> filterStream = mapStream
                .filter(new FilterFunction<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>>() {

                    public boolean filter(Tuple6<Long, Integer, Integer, Integer, Integer, Integer> tuple) {
                        return tuple.f4 == 0 && tuple.f5 >= startSegment && tuple.f5 <= endSegment;
                    }
                });

        KeyedStream<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStream = filterStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>>() {

                    @Override
                    public long extractAscendingTimestamp(Tuple6<Long, Integer, Integer, Integer, Integer, Integer> element) {
                        return element.f0*1000;
                    }
                })
                .keyBy(2); // Key by VID

        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Float>> avgSpeedTumblingEventTimeWindow = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(time)))
                .apply(new ComputeAverageSpeed())
                .filter(new FilterFunction<Tuple4<Long, Integer, Integer, Float>>() {

                    public boolean filter(Tuple4<Long, Integer, Integer, Float> tuple) {
                        return tuple.f3 > speed;
                    }
                });

        // TODO: Union streams to execute ComputeOutput function
        KeyedStream<Tuple4<Long, Integer, Integer, Float>, Tuple> keyedStream2 = avgSpeedTumblingEventTimeWindow.keyBy(1);

        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, String>> sumTumblingEventTimeWindow = keyedStream2
                .window(TumblingEventTimeWindows.of(Time.seconds(time)))
                .apply(new ComputeOutput());

        // emit result
        if (params.has("output")) {
            String file = params.get("output");
            sumTumblingEventTimeWindow.writeAsText(file, FileSystem.WriteMode.OVERWRITE);
        }

        // execute program
        env.execute("Exercise2");
    }

    public static class ComputeAverageSpeed implements WindowFunction<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>,
            Tuple4<Long, Integer, Integer, Float>, Tuple, TimeWindow> {

        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>> input,
                          Collector<Tuple4<Long, Integer, Integer, Float>> out) {

            Iterator<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple6<Long, Integer, Integer, Integer, Integer, Integer> first = iterator.next();

            long time = 0L;
            int xway = 0;
            int vid = 0;

            int sumSpeed = 0;
            int numberOfReports = 0;

            if(first != null) {

                time = first.f0;
                xway = first.f3;
                vid = first.f1;
                sumSpeed =+ first.f2;
                numberOfReports++;
            }

            while(iterator.hasNext()){

                Tuple6<Long, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                sumSpeed =+ next.f2;
                numberOfReports++;
            }

            float avgSpeed = sumSpeed/numberOfReports;

            out.collect(new Tuple4<Long, Integer, Integer, Float>(time, xway, vid, avgSpeed));
        }
    }

    public static class ComputeOutput implements WindowFunction<Tuple4<Long, Integer, Integer, Float>,
            Tuple4<Long, Integer, Integer, String>, Tuple, TimeWindow> {

        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Long, Integer, Integer, Float>> input,
                          Collector<Tuple4<Long, Integer, Integer, String>> out) {

            Iterator<Tuple4<Long, Integer, Integer, Float>> iterator = input.iterator();
            Tuple4<Long, Integer, Integer, Float> first = iterator.next();

            long time = 0L;
            int xway = 0;

            int numberOfVehicles = 0;
            StringBuilder sb = new StringBuilder("[");

            if(first != null) {
                time = first.f0;
                xway = first.f1;
                sb.append(first.f2);
            }

            while(iterator.hasNext()){

                Tuple4<Long, Integer, Integer, Float> next = iterator.next();
                sb.append("-");
                sb.append(next.f1);
            }

            sb.append("]");

            out.collect(new Tuple4<Long, Integer, Integer, String>(time, xway, numberOfVehicles, sb.toString()));
        }
    }
}