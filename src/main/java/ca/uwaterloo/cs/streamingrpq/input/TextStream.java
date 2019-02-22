package ca.uwaterloo.cs.streamingrpq.input;

import ca.uwaterloo.cs.streamingrpq.core.TupleType;

import java.io.*;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class TextStream {

    FileInputStream fileStream;
    BufferedReader bufferedReader;

    public boolean isOpen() {
        return false;
    }

    public void open(String filename) throws FileNotFoundException {
        fileStream = new FileInputStream(filename);
        bufferedReader = new BufferedReader(new InputStreamReader(fileStream));
    }

    public void close() throws IOException {
        bufferedReader.close();
        fileStream.close();
    }

    public InputTuple next() {
        String line = null;
        try {
            line = bufferedReader.readLine();
        } catch (IOException e) {
            return null;
        }
        if(line == null) {
            return null;
        }
        String[] splits = line.split("\\s");
        // assume first character is delete or insert
        return new InputTuple(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]),
                splits[3].charAt(0), splits[0].equals("+")? TupleType.INSERT:TupleType.DELETE);
    }

}
