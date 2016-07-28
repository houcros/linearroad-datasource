import es.houcros.linearroad.driverwrapper.DataDriver;
import org.junit.Test;

/**
 * Created by houcros on 28/07/16.
 */
public class RunSample {

    String inputFile = "datafile20seconds.dat";

    @Test
    public void runSampleTest(){
        DataDriver dataDriver = new DataDriver(inputFile);
        dataDriver.runSample();
    }
}
