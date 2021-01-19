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

import java.util.Iterator;

public class exercise2 {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // read the text file from given input path
        DataStream<String> text = env.readTextFile(params.get("input"));

        int time = Integer.parseInt(params.get("time"));
        final int speed = Integer.parseInt(params.get("speed"));
        final int startSegment = Integer.parseInt(params.get("startSegment"));
        final int endSegment = Integer.parseInt(params.get("endSegment"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>> mappedStream = text
                .map(new MapFunction<String, Tuple6<Long, Integer, Integer, Integer, Integer, Integer>>() {

                    public Tuple6<Long, Integer, Integer, Integer, Integer, Integer> map(String in) {

                        String[] fieldArray = in.split(",");
                        return new Tuple6<Long, Integer, Integer, Integer, Integer, Integer>(
                                Long.parseLong(fieldArray[0]),   // Time
                                Integer.parseInt(fieldArray[1]), // VID
                                Integer.parseInt(fieldArray[2]), // Speed
                                Integer.parseInt(fieldArray[3]), // Xway
                                Integer.parseInt(fieldArray[5]), // Dir
                                Integer.parseInt(fieldArray[6])  // Seg
                        );
                    }
                });

        // Filter by Direction 4 = Eastbound and Segment
        SingleOutputStreamOperator<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>> filteredStream = mappedStream
                .filter(new FilterFunction<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>>() {

                    public boolean filter(Tuple6<Long, Integer, Integer, Integer, Integer, Integer> tuple) {
                        return tuple.f4 == 0 && tuple.f5 >= startSegment && tuple.f5 <= endSegment;
                    }
                });

        KeyedStream<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedByXwayVIDStream = filteredStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>>() {

                    @Override
                    public long extractAscendingTimestamp(Tuple6<Long, Integer, Integer, Integer, Integer, Integer> element) {
                        return element.f0*1000;
                    }
                })
                .keyBy(3, 1); // Key by Xway and VID

        // Compute Avg speed per car
        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Integer>> carAvgSpeedTumblingEventTimeWindow = keyedByXwayVIDStream
                .window(TumblingEventTimeWindows.of(Time.seconds(time)))
                .apply(new ComputeAverageSpeed());

        // Filter cars by Avg speed > speed
        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Integer>> filteredSpeedStream = carAvgSpeedTumblingEventTimeWindow
                .filter(new FilterFunction<Tuple4<Long, Integer, Integer, Integer>>() {

                    public boolean filter(Tuple4<Long, Integer, Integer, Integer> tuple) {
                        return tuple.f3 > speed;
                    }
                });

        KeyedStream<Tuple4<Long, Integer, Integer, Integer>, Tuple> keyedByXwayStream = filteredSpeedStream
                .keyBy(2);  // Key by Xway

        // Compute output
        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, String>> outputTumblingEventTimeWindow = keyedByXwayStream
                .window(TumblingEventTimeWindows.of(Time.seconds(time)))
                .apply(new ComputeOutput());

        // emit result
        if (params.has("output")) {
            String file = params.get("output");
            outputTumblingEventTimeWindow.writeAsCsv(file, FileSystem.WriteMode.OVERWRITE);
        }

        // execute program
        env.execute("Exercise2");
    }

    public static class ComputeAverageSpeed implements WindowFunction<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>,
            Tuple4<Long, Integer, Integer, Integer>, Tuple, TimeWindow> {

        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>> input,
                          Collector<Tuple4<Long, Integer, Integer, Integer>> out) {

            Iterator<Tuple6<Long, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple6<Long, Integer, Integer, Integer, Integer, Integer> first = iterator.next();

            long time = 0L;
            int vid = 0;
            int xway = 0;

            int sumSpeed = 0;
            int numberOfReports = 0;

            if(first != null) {

                time = first.f0;
                vid = first.f1;
                sumSpeed = first.f2;
                xway = first.f3;
                numberOfReports++;
            }

            while(iterator.hasNext()){

                Tuple6<Long, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                sumSpeed += next.f2;
                numberOfReports++;
            }

            int avgSpeed = sumSpeed/numberOfReports;

            out.collect(new Tuple4<Long, Integer, Integer, Integer>(time, vid, xway, avgSpeed));
        }
    }

    public static class ComputeOutput implements WindowFunction<Tuple4<Long, Integer, Integer, Integer>,
            Tuple4<Long, Integer, Integer, String>, Tuple, TimeWindow> {

        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Long, Integer, Integer, Integer>> input,
                          Collector<Tuple4<Long, Integer, Integer, String>> out) {

            Iterator<Tuple4<Long, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple4<Long, Integer, Integer, Integer> first = iterator.next();

            long time = 0L;
            int xway = 0;

            int numberOfVehicles = 0;
            StringBuilder sb = new StringBuilder("[ ");

            if(first != null) {
                time = first.f0;
                sb.append(first.f1);
                xway = first.f2;
                numberOfVehicles++;
            }

            while(iterator.hasNext()){

                Tuple4<Long, Integer, Integer, Integer> next = iterator.next();
                time = Math.min(time, next.f0);
                sb.append(" - ");
                sb.append(next.f1);
                numberOfVehicles++;
            }

            sb.append(" ]");

            out.collect(new Tuple4<Long, Integer, Integer, String>(time, xway, numberOfVehicles, sb.toString()));
        }
    }
}