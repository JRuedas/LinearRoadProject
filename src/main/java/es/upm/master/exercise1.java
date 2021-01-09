package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
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

        // get input data
        DataStream<String> text;

        // read the text file from given input path
        text = env.readTextFile(params.get("input"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> mapStream = text.
                map(new MapFunction<String, Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    public Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception {
                        String[] fieldArray = in.split(",");
                        Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out
                                = new Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> (
                                Long.parseLong(fieldArray[0]), // Time
                                Integer.parseInt(fieldArray[1]), // VID
                                Integer.parseInt(fieldArray[2]), // Spd
                                Integer.parseInt(fieldArray[3]), // Xway
                                Integer.parseInt(fieldArray[4]), // Lane
                                Integer.parseInt(fieldArray[5]), // Dig
                                Integer.parseInt(fieldArray[6]), // Seg
                                Integer.parseInt(fieldArray[7]) // Pos
                        );
                        return out;
                    }
                });

        SingleOutputStreamOperator<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> filterStream = mapStream
                .filter(new FilterFunction<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    public boolean filter(Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple) throws Exception {
                        return tuple.f4 == 4;
                    }
                });

        KeyedStream<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStream = mapStream.
                assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                                return element.f0*30*1000;
                            }
                        }
                ).keyBy(3);

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

    public static class CountVehicles implements WindowFunction<Tuple8<Long, Integer, Integer, Integer, Integer, Integer,
            Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>, Tuple, TimeWindow> {

        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input,
                          Collector<Tuple4<Long, Integer, Integer, Integer>> out) throws Exception {

            Iterator<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();

            Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();

            Long time = 0L;
            Integer xway = 0;
            Integer lane = 0;
            int count = 0;

            if(first!=null){

                time = first.f0;
                xway = first.f1;
                lane = first.f2;
                count++;
            }

            while(iterator.hasNext()){
                Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                count++;

            }

            Tuple4<Long, Integer, Integer, Integer> outputTuple = new Tuple4<Long, Integer, Integer, Integer>(time, xway, lane, count);
            out.collect(outputTuple);
        }
    }
}