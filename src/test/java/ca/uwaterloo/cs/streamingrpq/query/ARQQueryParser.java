package ca.uwaterloo.cs.streamingrpq.query;

import ca.uwaterloo.cs.streamingrpq.stree.query.NFA;
import ca.uwaterloo.cs.streamingrpq.stree.query.sparql.LinearARQOpVisitor;
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


    private static final String QUERY_STRING =  "PREFIX : <http://example.org/gmark/>\n" +
            "ASK { { ?x0 (((:pname/^:pname)|(:pname/^:pname/:pname/^:pname)|(:pname/^:pname/:pname/^:pname))){,3} ?x1 . ?x1 ((^:pstudyAt/^:phasCreator/:pcreationDate)|(^:pstudyAt/:pbirthday)|(^:pstudyAt/:pemail/^:pgender/:pbirthday)) ?x2 . } }";
    public static void main(String[] args) {

        Query query = QueryFactory.create(QUERY_STRING, Syntax.syntaxARQ);

        List<String> resultVariables = query.getResultVars();
        Element pattern = query.getQueryPattern();
        List<Var> projectVariables = query.getProjectVars();

        Op algebra = Algebra.compile(query);
        LinearARQOpVisitor visitor = new LinearARQOpVisitor();
        OpWalker.walk(algebra, visitor);

        NFA<String> nfa = visitor.getAutomaton();
        
        

        return;


    }






}



