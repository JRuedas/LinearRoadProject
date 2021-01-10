package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
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

import java.util.Iterator;

public class exercise1 {

    public static void main(String[] args) throws Exception{

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // read the text file from given input path
        DataStream<String> text = env.readTextFile(params.get("input"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple3<Long, Integer, Integer>> mapStream = text
                .map(new MapFunction<String, Tuple3<Long, Integer, Integer>>() {

                    public Tuple3<Long, Integer, Integer> map(String in) {

                        String[] fieldArray = in.split(",");
                        return new Tuple3<Long, Integer, Integer> (
                                Long.parseLong(fieldArray[0]), // Time
                                Integer.parseInt(fieldArray[3]), // Xway
                                Integer.parseInt(fieldArray[4]) // Lane
                        );
                    }
                });

        SingleOutputStreamOperator<Tuple3<Long, Integer, Integer>> filterStream = mapStream
                .filter(new FilterFunction<Tuple3<Long, Integer, Integer>>() {

                    public boolean filter(Tuple3<Long, Integer, Integer> tuple) {
                        return tuple.f2 == 4;
                    }
                });

        KeyedStream<Tuple3<Long, Integer, Integer>, Tuple> keyedStream = filterStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Integer, Integer>>() {

                    @Override
                    public long extractAscendingTimestamp(Tuple3<Long, Integer, Integer> element) {
                        return element.f0*1000;
                    }
                })
                .keyBy(1); // Key by Xway

        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Integer>> sumTumblingEventTimeWindow = keyedStream
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new CountVehicles());

        // emit result
        if (params.has("output")) {
            String file = params.get("output");
            sumTumblingEventTimeWindow.writeAsText(file, FileSystem.WriteMode.OVERWRITE);
        }

        // execute program
        env.execute("Exercise1");
    }

    public static class CountVehicles implements WindowFunction<Tuple3<Long, Integer, Integer>,
            Tuple4<Long, Integer, Integer, Integer>, Tuple, TimeWindow> {

        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<Long, Integer, Integer>> input,
                          Collector<Tuple4<Long, Integer, Integer, Integer>> out) {

            Iterator<Tuple3<Long, Integer, Integer>> iterator = input.iterator();
            Tuple3<Long, Integer, Integer> first = iterator.next();

            long time = 0L;
            int xway = 0;
            int lane = 0;

            int numberOfVehicles = 0;

            if(first != null) {

                time = first.f0;
                xway = first.f1;
                lane = first.f2;
                numberOfVehicles++;
            }

            while(iterator.hasNext()){
                Tuple3<Long, Integer, Integer> next = iterator.next();
                numberOfVehicles++;
            }

            out.collect(new Tuple4<Long, Integer, Integer, Integer>(time, xway, lane, numberOfVehicles));
        }
    }
}