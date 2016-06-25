import es.houcros.linearroad.datasource.CarReportsSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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

        DataStream<String> reports = env.addSource(new CarReportsSource<String>(inputFile)).returns(String.class);
        AllWindowedStream<String, TimeWindow> b = reports.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)));
        b.apply(new AllWindowFunction<String, Object, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<String> iterable, Collector<Object> collector) throws Exception {
                collector.collect("Notif");
            }
        }).print();

        env.execute("CarReportsSourceTest");
    }
}
