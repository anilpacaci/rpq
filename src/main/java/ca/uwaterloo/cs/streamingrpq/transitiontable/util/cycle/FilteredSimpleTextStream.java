package ca.uwaterloo.cs.streamingrpq.transitiontable.util.cycle;

import ca.uwaterloo.cs.streamingrpq.input.InputTuple;
import ca.uwaterloo.cs.streamingrpq.input.TextStream;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

public class FilteredSimpleTextStream implements TextStream{


    FileReader fileStream;
    BufferedReader bufferedReader;

    String filename;

    Integer localCounter = 0;
    Integer globalCounter = 0;

    HashSet<String> predicates;


    public FilteredSimpleTextStream(String[] predicates) {
        this.predicates = new HashSet<>();

        for(String p : predicates) {
            this.predicates.add(p);
        }
    }

    public boolean isOpen() {
        return false;
    }

    public void open(String filename, int maxSize) {
        open(filename);
    }

    public void open(String filename) {
        this.filename = filename;
        try {
            fileStream = new FileReader(filename);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        bufferedReader = new BufferedReader(fileStream, 1024*1024);

    }

    public void close() {
        try {
            bufferedReader.close();
            fileStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public InputTuple<Integer, Integer, String> next() {
        String line = null;
        InputTuple tuple = null;
        try {
            while((line = bufferedReader.readLine()) != null) {
                String[] splitResults = Iterables.toArray(Splitter.on('\t').split(line), String.class);
                if(splitResults.length == 3) {
                    if (this.predicates.contains(splitResults[1])) {
//                    tuple = new InputTuple(1,2,3);
                        tuple = new InputTuple(splitResults[0].hashCode(), splitResults[2].hashCode(), splitResults[1]);
                        break;
                    }
                }
            }
        } catch (IOException e) {
            return null;
        }
        if (line == null) {
            return null;
        }
        localCounter++;
        globalCounter++;
        return tuple;
    }

    public void reset() {
        close();

        open(this.filename);

        localCounter = 0;
        globalCounter = 0;
    }
}
