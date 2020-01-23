package ca.uwaterloo.cs.streamingrpq.input;

import com.google.common.base.Splitter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Yago2sTSVStream extends TextFileStream {


    public void open(String filename, int maxSize) {
        open(filename);
    }

       public void close() {
        try {
            bufferedReader.close();
            fileStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        executor.shutdown();
    }

    public InputTuple<Integer, Integer, String> next() {
        String line = null;

        //generate negative tuple if necessary
        if(!deletionBuffer.isEmpty() && ThreadLocalRandom.current().nextInt(100) < deletionPercentage) {
            line = (String) deletionBuffer.poll();
            Iterator<String> iterator = Splitter.on('\t').trimResults().split(line).iterator();
            int i = 0;
            for(i = 0; iterator.hasNext() && i < 3; i++) {
                splitResults[i] = iterator.next();
            }
            // only if we fully
            if(i == 3) {
                tuple.setSource(splitResults[0].hashCode());
                tuple.setTarget(splitResults[2].hashCode());
                tuple.setLabel(splitResults[1]);
                tuple.setTimestamp(globalCounter);
                tuple.setType(InputTuple.TupleType.DELETE);
                deleteCounter++;
                return tuple;
            }
        }

        try {
            while((line = bufferedReader.readLine()) != null) {
                Iterator<String> iterator = Splitter.on('\t').trimResults().split(line).iterator();
                int i = 0;
                for(i = 0; iterator.hasNext() && i < 3; i++) {
                    splitResults[i] = iterator.next();
                }
                // only if we fully
                if(i == 3) {
//                    tuple = new InputTuple(1,2,3);
                    lastTimestamp = globalCounter;

                    tuple.setSource(splitResults[0].hashCode());
                    tuple.setTarget(splitResults[2].hashCode());
                    tuple.setLabel(splitResults[1]);
                    tuple.setTimestamp(globalCounter);
                    tuple.setType(InputTuple.TupleType.INSERT);
//                    tuple = new InputTuple(Integer.parseInt(splitResults[0]), Integer.parseInt(splitResults[2]), splitResults[1]);
                    localCounter++;
                    globalCounter++;

                    // store this tuple for later deletion
                    if(ThreadLocalRandom.current().nextInt(100) < 2 * deletionPercentage) {
                        deletionBuffer.offer(line);
                    }

                    break;
                }
            }
        } catch (IOException e) {
            return null;
        }
        if (line == null) {
            return null;
        }

        return tuple;
    }

    @Override
    protected int getRequiredNumberOfFields() {
        return 3;
    }

    @Override
    protected void setSource() {
        tuple.setSource(splitResults[0]);
    }

    @Override
    protected void setTarget() {
        tuple.setTarget(splitResults[2]);
    }

    @Override
    protected void setLabel() {
        tuple.setLabel(splitResults[1]);
    }

    @Override
    protected void updateCurrentTimestamp() {
        lastTimestamp = globalCounter;
    }

    @Override
    protected void setTimestamp() {
        tuple.setTimestamp(lastTimestamp);
    }

    public void reset() {
        close();

        open(this.filename);

        localCounter = 0;
        globalCounter = 0;
    }
}
