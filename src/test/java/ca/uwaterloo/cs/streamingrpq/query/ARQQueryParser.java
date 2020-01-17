package ca.uwaterloo.cs.streamingrpq.query;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.syntax.Element;

import java.util.List;

public class ARQQueryParser {

    private static final String QUERY_STRING = "PREFIX : <http://example.org/gmark/>\n" +
            "SELECT DISTINCT ?x0 ?x3 WHERE { { ?x0 (((:pcontactPoint/^:pemployee))){,3}|((:pcontactPoint*/:pfriendOf/:plike)) ?x2 . ?x2 (((:phomepage/^:phomepage/^:pauthor)))* ?x3 . } }";
    public static void main(String[] args) {

        Query query = QueryFactory.create(QUERY_STRING, Syntax.syntaxARQ);

        List<String> resultVariables = query.getResultVars();
        Element pattern = query.getQueryPattern();
        List<Var> projectVariables = query.getProjectVars();



    }

}
