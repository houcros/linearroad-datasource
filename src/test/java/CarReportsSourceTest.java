import es.houcros.linearroad.datasource.CarReportsSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
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
        reports.timeWindowAll(Time.seconds(2));
        reports.print();

        env.execute("CarReportsSourceTest");
    }
}
