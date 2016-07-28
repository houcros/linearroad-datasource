package es.houcros.linearroad.driverwrapper;

import com.sun.jna.Native;

/**
 * Created by houcros on 28/07/16.
 */
public class DataDriver {

    private DataDriverLibrary instance;
    private String path;


    public DataDriver(String path) {
        try {
            instance = (DataDriverLibrary) Native.loadLibrary("datadriver", DataDriverLibrary.class);
        } catch (Exception e) {
            System.out.println("jna.library.path searches in: " + System.getProperty("jna.library.path"));
            e.printStackTrace();
        }
        System.setProperty("jna.library.path", "datadriver");
        //System.setProperty("jna.library.path", "/Library/Java/JavaVirtualMachines/jdk1.8.0_51.jdk/Contents/Home/jre/lib/libdatadriver.dylib");
        this.path = path;

    }

    public DataDriver() {
        System.setProperty("jna.library.path", "datadriver");

    }

    public void runSample() {
        DataDriverLibrary.TupleReceivedCallback tupleReceivedCallback = new DataDriverLibrary.TupleReceivedCallback() {
            public void invoke(String tuple) {
                String[] array = tuple.split(",");
                System.out.println("Received: "+tuple);
                System.out.println("Current speed is: "+array[3]);

            }
        };

        instance.startProgram(path,tupleReceivedCallback);
    }

    public DataDriverLibrary getLibrary() {return instance;}

}

