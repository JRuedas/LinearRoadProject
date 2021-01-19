package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
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

public class exercise3 {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // read the text file from given input path
        DataStream<String> text = env.readTextFile(params.get("input"));

        final int segment = Integer.parseInt(params.get("segment"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple5<Long, Integer, Integer, Integer, Integer>> mappedStream = text
                .map(new MapFunction<String, Tuple5<Long, Integer, Integer, Integer, Integer>>() {

                    public Tuple5<Long, Integer, Integer, Integer, Integer> map(String in) {

                        String[] fieldArray = in.split(",");
                        return new Tuple5<Long, Integer, Integer, Integer, Integer>(
                                Long.parseLong(fieldArray[0]),   // Time
                                Integer.parseInt(fieldArray[1]), // VID
                                Integer.parseInt(fieldArray[2]), // Speed
                                Integer.parseInt(fieldArray[3]), // Xway
                                Integer.parseInt(fieldArray[6])  // Seg
                        );
                    }
                });

        // Filter by segment
        SingleOutputStreamOperator<Tuple5<Long, Integer, Integer, Integer, Integer>> filteredStream = mappedStream
                .filter(new FilterFunction<Tuple5<Long, Integer, Integer, Integer, Integer>>() {

                    public boolean filter(Tuple5<Long, Integer, Integer, Integer, Integer> tuple) {
                        return tuple.f4 == segment;
                    }
                });

        KeyedStream<Tuple5<Long, Integer, Integer, Integer, Integer>, Tuple> keyedByXwayVIDStream = filteredStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<Long, Integer, Integer, Integer, Integer>>() {

                    @Override
                    public long extractAscendingTimestamp(Tuple5<Long, Integer, Integer, Integer, Integer> element) {
                        return element.f0*1000;
                    }
                })
                .keyBy(3, 1); // Key by Xway and VID

        // Compute Avg speed per car output 1
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> carAvgSpeedTumblingEventTimeWindow = keyedByXwayVIDStream
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new ComputeAverageSpeed());

        // emit result 1
        if (params.has("output1")) {
            String file = params.get("output1");
            carAvgSpeedTumblingEventTimeWindow.writeAsCsv(file, FileSystem.WriteMode.OVERWRITE);
        }

        KeyedStream<Tuple3<Integer, Integer, Integer>, Tuple> keyedByXwayStream = carAvgSpeedTumblingEventTimeWindow
                .keyBy(1);  // Key by Xway

        // Compute output 2
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> outputTumblingEventTimeWindow = keyedByXwayStream
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new ComputeMaxAverageSpeed());

        // emit result 2
        if (params.has("output2")) {
            String file = params.get("output2");
            outputTumblingEventTimeWindow.writeAsCsv(file, FileSystem.WriteMode.OVERWRITE);
        }

        // execute program
        env.execute("Exercise3");
    }

    public static class ComputeAverageSpeed implements WindowFunction<Tuple5<Long, Integer, Integer, Integer, Integer>,
            Tuple3<Integer, Integer, Integer>, Tuple, TimeWindow> {

        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple5<Long, Integer, Integer, Integer, Integer>> input,
                          Collector<Tuple3<Integer, Integer, Integer>> out) {

            Iterator<Tuple5<Long, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple5<Long, Integer, Integer, Integer, Integer> first = iterator.next();

            int vid = 0;
            int xway = 0;

            int sumSpeed = 0;
            int numberOfReports = 0;

            if(first != null) {

                vid = first.f1;
                sumSpeed = first.f2;
                xway = first.f3;
                numberOfReports++;
            }

            while(iterator.hasNext()){

                Tuple5<Long, Integer, Integer, Integer, Integer> next = iterator.next();
                sumSpeed += next.f2;
                numberOfReports++;
            }

            int avgSpeed = sumSpeed/numberOfReports;

            out.collect(new Tuple3<Integer, Integer, Integer>(vid, xway, avgSpeed));
        }
    }

    public static class ComputeMaxAverageSpeed implements WindowFunction<Tuple3<Integer, Integer, Integer>,
            Tuple3<Integer, Integer, Integer>, Tuple, TimeWindow> {

        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<Integer, Integer, Integer>> input,
                          Collector<Tuple3<Integer, Integer, Integer>> out) {

            Iterator<Tuple3<Integer, Integer, Integer>> iterator = input.iterator();
            Tuple3<Integer, Integer, Integer> first = iterator.next();

            int vid = 0;
            int xway = 0;

            int avgSpeed = 0;

            if(first != null) {

                vid = first.f0;
                xway = first.f1;
                avgSpeed = first.f2;
            }

            while(iterator.hasNext()){

                Tuple3<Integer, Integer, Integer> next = iterator.next();

                if (next.f2 > avgSpeed) {
                    vid = next.f0;
                    avgSpeed = next.f2;
                }
            }

            out.collect(new Tuple3<Integer, Integer, Integer>(vid, xway, avgSpeed));
        }
    }
}
