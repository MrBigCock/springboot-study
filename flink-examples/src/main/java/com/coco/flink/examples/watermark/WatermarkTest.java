package com.coco.flink.examples.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.List;

public class WatermarkTest {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("USAGE:\nSocketWatermarkTest <hostname> <port>");
            return;
        }

        String hostName = args[0];
        Integer port = Integer.valueOf(args[1]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> input = env.socketTextStream(hostName, port);

        DataStream<Tuple2<String, Long>> inputMap = input.map(
                new MapFunction<String, Tuple2<String, Long>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split("\\W+");
                        return new Tuple2(arr[0], Long.valueOf(arr[1])*1000L);
                    }
                }
        );

        DataStream<Tuple2<String, Long>> watermarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            Long currentMaxTimestamp = 0L;
            Long maxOutOfOrderness = 10000L; //最大允许的乱序时间是10s

            Watermark watermark = null;

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Override
            public Watermark getCurrentWatermark() {
                watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                return watermark;
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                Long timestamp = element.f1;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                System.out.println("event: " + element.f0 + "," + element.f1 + " | currentTime: " + System.currentTimeMillis() + ", " + format.format(System.currentTimeMillis()) + " | eventTime: " + element.f1 + ", " + format.format(element.f1) + " | currentMaxTimestamp: " + currentMaxTimestamp + ", " + format.format(currentMaxTimestamp) + " | " + watermark);
                return timestamp;
            }
        });

        DataStream window = watermarkStream
                .keyBy(new FirstFieldKeyExtractor<Tuple2<String, Long>>())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunctionTest());

        window.print();

        env.execute("watermark test");

    }
}

class FirstFieldKeyExtractor<Type extends Tuple> implements KeySelector<Type, String> {

    @Override
    public String getKey(Type value) {
        return (String) value.getField(0);
    }
}

class WindowFunctionTest implements WindowFunction<Tuple2<String, Long>, Tuple6<String, Integer, String, String, String, String>, String, TimeWindow> {

    @Override
    public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple6<String, Integer, String, String, String, String>> out) throws Exception {
        List<Tuple2<String, Long>> list = (List<Tuple2<String, Long>>) input;
        list.sort((Tuple2<String, Long> t1, Tuple2<String, Long> t2) -> t1.f1.compareTo(t2.f1));
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        out.collect(new Tuple6(key, list.size(), list.get(0).f1 + "", list.get(list.size() - 1).f1 + "", fmt.format(window.getStart()), fmt.format(window.getEnd())));
    }

}
