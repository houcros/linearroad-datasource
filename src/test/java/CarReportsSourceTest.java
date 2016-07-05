import es.houcros.linearroad.datasource.CarReportsSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

/**
 * Created by houcros on 20/06/16.
 */
public class CarReportsSourceTest {

    @Test
    public void testStream() throws Exception {
        String inputFile = "datafile20seconds.dat";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple15<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer,
                Integer, Integer, Integer, Integer, Integer, Integer>> reports =
                env.addSource(new CarReportsSource<Tuple15<Integer, Integer, Integer, Integer, Integer, Integer,
                        Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>(inputFile))
                        .returns(new TypeHint<Tuple15<Integer, Integer, Integer, Integer, Integer, Integer, Integer,
                                Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {});

        AllWindowedStream<Tuple15<Integer, Integer, Integer, Integer, Integer, Integer, Integer,
                Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, TimeWindow> b =
                reports.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)));
        b.apply(new AllWindowFunction<Tuple15<Integer,Integer,Integer,Integer,Integer,Integer,
                Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>,
                java.lang.Object, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Tuple15<Integer, Integer, Integer, Integer, Integer,
                    Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterable,
                              Collector<java.lang.Object> collector) throws Exception {

                String out = "Window: " + String.valueOf(timeWindow.getStart()) + "\n";
                for (Tuple15 report : iterable){
                    for (int i = 0; i < 15; ++i) {
                        out += report.getField(i) + " ";
                    }
                    out += "\n";
                }
                collector.collect(out);
            }
        }).print();

        env.execute("CarReportsSourceTest");
    }
}
