package ca.uwaterloo.cs.streamingrpq.query;

import ca.uwaterloo.cs.streamingrpq.stree.query.*;
import ca.uwaterloo.cs.streamingrpq.stree.query.sparql.LinearARQOpVisitor;
import dk.brics.automaton.Automaton;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.algebra.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.syntax.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ARQQueryParser {

    private final Logger logger = LoggerFactory.getLogger(ARQQueryParser.class);

    private static final String QUERY_STRING =  "SELECT ?y\n" +
            "FROM <window>\n" +
            "WHERE {\n" +
            "   <-1169556728> (<http://yago-knowledge.org/resource/isLocatedIn> / <http://yago-knowledge.org/resource/dealsWith>  / <http://yago-knowledge.org/resource/hasCapital>)+ ?y .\n" +
            "}";

    public static void main(String[] args) {

        Query query = QueryFactory.create(QUERY_STRING, Syntax.syntaxARQ);

        List<String> resultVariables = query.getResultVars();
        Element pattern = query.getQueryPattern();
        List<Var> projectVariables = query.getProjectVars();

        Op algebra = Algebra.compile(query);
        BricsAutomataBuilder automataBuilder = new BricsAutomataBuilder();
        BricsAutomata automata = automataBuilder.fromSPARQL(QUERY_STRING);

        String[] alphabet = StringUtils.substringsBetween(QUERY_STRING, "<", ">");



        automata.generateTransitiongraph("asd");

        return;
    }






}



