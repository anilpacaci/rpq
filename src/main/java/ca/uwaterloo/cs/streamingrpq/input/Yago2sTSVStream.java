package ca.uwaterloo.cs.streamingrpq.input;

import java.io.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Yago2sTSVStream {


    FileInputStream fileStream;
    BufferedReader bufferedReader;

    ScheduledExecutorService executor;

    AtomicInteger counter = new AtomicInteger(0);

    public boolean isOpen() {
        return false;
    }

    public void open(String filename) throws FileNotFoundException {
        fileStream = new FileInputStream(filename);
        bufferedReader = new BufferedReader(new InputStreamReader(fileStream), 1024*1024*1024);

        Runnable counterRunnable = new Runnable() {
            @Override
            public void run() {
                System.out.println(counter.get());
            }
        };

        executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(counterRunnable, 0, 1, TimeUnit.SECONDS);

    }

    public void close() throws IOException {
        bufferedReader.close();
        fileStream.close();
        executor.shutdown();
    }

    public InputTuple next() {
        String line = null;
        InputTuple tuple = null;
        try {
            while((line = bufferedReader.readLine()) != null) {
                String[] splits = line.split("\\t");
                if(splits.length == 3) {
                    tuple = new InputTuple(splits[0].hashCode(), splits[2].hashCode(), splits[1]);
                    break;
                }
            }
        } catch (IOException e) {
            return null;
        }
        if (line == null) {
            return null;
        }
        String[] splits = line.split("\\t");
        counter.incrementAndGet();
        return tuple;
    }
}
