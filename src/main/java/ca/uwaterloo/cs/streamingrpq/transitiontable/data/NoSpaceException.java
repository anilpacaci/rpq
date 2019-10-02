package ca.uwaterloo.cs.streamingrpq.transitiontable.data;

/**
 * Created by anilpacaci on 2019-06-07.
 */
public class NoSpaceException extends Exception {

    private long capacity;
    public NoSpaceException(long x) {
        capacity = x;
    }

    public String toString() {
        return "NoSpaceException - cannot grow beyond " + capacity;
    }
}
