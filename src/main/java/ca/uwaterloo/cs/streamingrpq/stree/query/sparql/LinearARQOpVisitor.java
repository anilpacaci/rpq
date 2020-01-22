package ca.uwaterloo.cs.streamingrpq.stree.query.sparql;

import ca.uwaterloo.cs.streamingrpq.stree.query.AutomataBuilder;
import ca.uwaterloo.cs.streamingrpq.stree.query.NFA;
import ca.uwaterloo.cs.streamingrpq.stree.query.NFAAutomataBuilder;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpPath;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.path.Path;

import java.util.Stack;

public class LinearARQOpVisitor<A> extends OpVisitorBase {

    Stack<A> nfaStack;
    private AutomataBuilder<A, String> automataBuilder;

    public LinearARQOpVisitor(AutomataBuilder<A, String> automataBuilder) {
        nfaStack = new Stack<>();
        this.automataBuilder = automataBuilder;
    }

    public A getAutomaton() {
        return nfaStack.pop();
    }

    @Override
    public void visit(OpGroup opGroup) {
        return;
    }

    @Override
    public void visit(OpPath opPath) {
        ARQPathVisitor<A> visitor = new ARQPathVisitor(this.automataBuilder);
        Path path = opPath.getTriplePath().getPath();
        path.visit(visitor);

        A nfa = visitor.getAutomaton();

        nfaStack.push(nfa);

        return;
    }

    @Override
    public void visit(OpSequence opSequence)  {
        // this is a linear chain processor so assumption is that all conjuncts form a linear chain.
        // thus we can simply concataneta all conjuncts
        while(nfaStack.size() > 1) {
            A rightNFA = nfaStack.pop();
            A leftNFA = nfaStack.pop();

            A resultNFA = automataBuilder.concenetation(leftNFA, rightNFA);
            nfaStack.push(resultNFA);
        }

        return;
    }
}
