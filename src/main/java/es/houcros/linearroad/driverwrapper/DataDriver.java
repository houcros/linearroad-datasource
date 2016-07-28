package es.houcros.linearroad.driverwrapper;

import com.sun.jna.Native;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Created by houcros on 28/07/16.
 */
public class DataDriver {

    private DataDriverLibrary instance;
    private String path;


    public DataDriver(String path) {
        instance = (DataDriverLibrary) Native.loadLibrary("datadriver", DataDriverLibrary.class);
        System.setProperty("jna.library.path", "ibdatadriver");
        this.path = path;

    }

    public DataDriver() {
        //System.setProperty("jna.debug_load", "true");
        System.setProperty("jna.library.path", "/usr/local/lib");
        instance = (DataDriverLibrary) Native.loadLibrary("datadriver", DataDriverLibrary.class);
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

