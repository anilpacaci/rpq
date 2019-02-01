package ca.uwaterloo.cs.streamingrpq.input;

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
        return new InputTuple(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]), splits[2].charAt(0));
    }

}
