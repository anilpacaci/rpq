package ca.uwaterloo.cs.streamingrpq.runtime;

import ca.uwaterloo.cs.streamingrpq.stree.data.ManualQueryAutomata;
import ca.uwaterloo.cs.streamingrpq.stree.query.BricsAutomata;
import ca.uwaterloo.cs.streamingrpq.stree.query.BricsAutomataBuilder;
import com.google.common.io.Files;
import org.apache.jena.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

public class gMarkQueries {

    private static Logger logger = LoggerFactory.getLogger(gMarkQueries.class);


    public static BricsAutomata getQuery(String datasetLocation, String queryName) {
        // make sure that query name have the correct extension
        if(!queryName.endsWith("sparql")) {
            queryName = queryName + ".sparql";
        }

        //first read the SPARQL string
        String queryString;
        try {
            queryString = FileUtils.readWholeFileAsUTF8(Paths.get(datasetLocation, queryName).toString());
            logger.info(String.format("Creating automata for query %s", queryString));
        } catch (IOException e) {
            logger.error("Query cannot be parsed from input file {}", queryName );
            throw new IllegalArgumentException("Query does not exists",e);
        }

        BricsAutomataBuilder builder = new BricsAutomataBuilder();
        BricsAutomata queryAutomata = builder.fromSPARQL(queryString);

        return queryAutomata;
    }
}
