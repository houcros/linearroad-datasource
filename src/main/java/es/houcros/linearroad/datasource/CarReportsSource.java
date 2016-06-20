package es.houcros.linearroad.datasource;

import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;

/**
 * Created by houcros on 19/06/16.
 */
public class CarReportsSource<T> implements SourceFunction<T>, Checkpointed<Long> {
    private long count = 0L;
    private volatile boolean isRunning = true;
    private String inputFile;
    private int tolerance = 5; // Tolerance to emit the watermarks
    private Long timer = 0L; // Keep the last timestamp when a watermark was emitted

    public CarReportsSource(){

    }

    public CarReportsSource(String inputFilePath) throws FileNotFoundException {
        inputFile = inputFilePath;
    }

    public void run(SourceContext<T> ctx) {
        // Init buffer
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        InputStream in = classLoader.getResourceAsStream(inputFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        while (isRunning /*&& count < 1000*/) {
            String line;
            try {
                line = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
                isRunning = false;
                break;
            }
            if (line == null) {isRunning = false;break;} // No more lines in the file
            if (line.length() == 0) continue;

            // Collect report with the timestamp from the report
            Long tmp = Long.valueOf(line.split(",")[1]); // The second element is the timestamp of the report
            synchronized (ctx.getCheckpointLock()) {
                    ctx.collectWithTimestamp((T)line, tmp);
                    count++;
            }
            // Emit watermark for current timestamp when some tolerance time units have elapsed
            if (tmp > timer + tolerance){
                timer = tmp;
                ctx.emitWatermark(new Watermark(timer));
            }

        }
    }

    public void cancel() {
        isRunning = false;
    }

    public Long snapshotState(long checkpointId, long checkpointTimestamp) { return count; }

    public void restoreState(Long state) { this.count = state; }
}