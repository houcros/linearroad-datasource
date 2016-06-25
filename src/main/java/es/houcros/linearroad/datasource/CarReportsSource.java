package es.houcros.linearroad.datasource;

import org.apache.flink.hadoop.shaded.org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

/**
 * Created by houcros on 19/06/16.
 */
public class CarReportsSource<T> implements SourceFunction<T> {
    private long count = 0L;
    private volatile boolean isRunning = true;

    private String inputPath;
    private long currentTimestamp = -1000L; // Timestamp in milis in Flink!

    public CarReportsSource(){

    }

    public CarReportsSource(String inputFile) throws FileNotFoundException, URISyntaxException {
        URL resource = CarReportsSource.class.getClassLoader().getResource(inputFile);
        inputPath = Paths.get(resource.toURI()).toString();
    }

    public void run(SourceContext<T> ctx) {

        de.twiechert.linroad.jdriver.DataDriver dataDriver = new de.twiechert.linroad.jdriver.DataDriver();
        dataDriver.getLibrary().startProgram(inputPath, new de.twiechert.linroad.jdriver.DataDriverLibrary.TupleReceivedCallback() {
            @Override
            public void invoke(String s) {
                  /*
                  report items are
                  report(0): type (0-> position report, etc.)
                  report(1): timestamp position report emitted (0...10799 (second))
                  report(2): vehicle identifier (0...MAXINT)
                  report(3): speed of the vehicle (0...100)
                  report(4): express way (0...L-1)
                  report(5): lane (0...4)
                  report(6): direction (0..1)
                  report(7): segment (0...99)
                  report(8): position of the vehicle (0...527999)
                  report(9): query identifier
                  report(10): start segment
                  report(11): end segment
                  report(12): day of week (1..7)
                  report(13): minute number in the day (1...1440)
                  report(14): 1->yesterday, 69->10 weeks ago (1..69)
                  */
                Long tmp = Long.valueOf(s.split(",")[1])*1000; // The second element is the timestamp of the report
                //System.out.println(tmp);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collectWithTimestamp((T)s, tmp);
                    if (tmp > currentTimestamp){
                        ctx.emitWatermark(new Watermark(tmp));
                        currentTimestamp = tmp;
                    }
                    count++;
                }
            }
        });
    }

    public void cancel() {
        isRunning = false;
    }

}