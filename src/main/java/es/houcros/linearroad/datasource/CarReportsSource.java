package es.houcros.linearroad.datasource;

import es.houcros.linearroad.driverwrapper.*;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
//import org.apache.flink.api.java.tuple.Tuple15;
import scala.Tuple15;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by houcros on 19/06/16.
 */
public class CarReportsSource<T> implements SourceFunction<T> {

    /* We don't use this yet, but it might come in handy later */
    private long count = 0L;
    private volatile boolean isRunning = true;

    /* The input file must be a resource (i.e., located in the *resources* directory */
    private String inputPath;
    /* We keep the timestamp of the last recieved report for watermarking */
    private long currentTimestamp = -1000L; // Timestamp in milis in Flink!

    public CarReportsSource(){

    }

    public CarReportsSource(String inputFile) throws IOException, URISyntaxException {
        final URI uri = CarReportsSource.class.getClassLoader().getResource(inputFile).toURI();
        Map<String, String> env = new HashMap<>();
        env.put("create", "true");
        FileSystem zipfs = FileSystems.newFileSystem(uri, env);
        inputPath = Paths.get(uri).toString();
    }

    public void run(SourceContext<T> ctx) throws ClassCastException{

        DataDriver dataDriver = null;
        try {
            dataDriver = new DataDriver();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("\n\n\n\n\n\n\n\n\n\n########################" + System.getProperty("jna.library.path"));
        }
        dataDriver.getLibrary().startProgram(inputPath, new DataDriverLibrary.TupleReceivedCallback() {
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

                // Cast the report to ints
                String[] report = s.split(",");
                Tuple15<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer,
                                        Integer, Integer, Integer, Integer, Integer, Integer> reportTuple = new Tuple15<>
                        (Integer.valueOf(report[0]), Integer.valueOf(report[1]), Integer.valueOf(report[2]),
                                Integer.valueOf(report[3]), Integer.valueOf(report[4]), Integer.valueOf(report[5]),
                                Integer.valueOf(report[6]), Integer.valueOf(report[7]), Integer.valueOf(report[8]),
                                Integer.valueOf(report[9]), Integer.valueOf(report[10]), Integer.valueOf(report[11]),
                                Integer.valueOf(report[12]), Integer.valueOf(report[13]), Integer.valueOf(report[14].trim()));

                // The second element is the timestamp of the report
                Long tmp = Long.valueOf(report[1])*1000;

                // Collect
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collectWithTimestamp((T)reportTuple, tmp);
                    // We know that the data comes sorted by timestamp, so the watermarking is simple
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