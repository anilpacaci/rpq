package ca.uwaterloo.cs.streamingrpq.input;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public interface TextStream {


    public boolean isOpen();

    public void open(String filename);

    public void open(String filename, int size);

    public void open(String filename, int size, long startTimestamp);

    public InputTuple next();

    public void close();

    public void reset();

}
