package es.houcros.linearroad.driverwrapper;

import com.sun.jna.Callback;
import com.sun.jna.Library;

/**
 * Created by houcros on 28/07/16.
 */
public interface DataDriverLibrary extends Library {


    interface TupleReceivedCallback extends Callback {
        void invoke(String tuple);
    }
    int startProgram(String argv,  TupleReceivedCallback tupleReceivedCallback);
    int  test();
}
