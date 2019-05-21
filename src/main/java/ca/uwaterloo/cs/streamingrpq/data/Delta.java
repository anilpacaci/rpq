package ca.uwaterloo.cs.streamingrpq.data;

import com.google.common.collect.Multimap;
import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.compound.CompoundIndex;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.resultset.ResultSet;
import com.googlecode.cqengine.resultset.common.NoSuchObjectException;
import com.googlecode.cqengine.resultset.common.NonUniqueObjectException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.googlecode.cqengine.query.QueryFactory.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class Delta {

    IndexedCollection<Tuple> cache;

    private final Logger logger = LoggerFactory.getLogger(Delta.class.getName());

    public Delta() {
        cache = new ConcurrentIndexedCollection();
        cache.addIndex(CompoundIndex.onAttributes(Tuple.TUPLE_TARGET, Tuple.TUPLE_TARGETSTATE));
        cache.addIndex(CompoundIndex.onAttributes(Tuple.TUPLE_SOURCE, Tuple.TUPLE_TARGET, Tuple.TUPLE_TARGETSTATE));

    }


    public boolean contains(Tuple tuple) {
        Query<Tuple> query = and(equal(Tuple.TUPLE_SOURCE, tuple.getSource()), equal(Tuple.TUPLE_TARGETSTATE, tuple.getTargetState()), equal(Tuple.TUPLE_TARGET, tuple.getTarget()));
        return cache.retrieve(query).isNotEmpty();
    }

    public List<Tuple> retrieveByTargetAndTargetState(Integer target, Integer targetState) {
        Query<Tuple> query = and(equal(Tuple.TUPLE_TARGETSTATE, targetState), equal(Tuple.TUPLE_TARGET, target));
        ResultSet<Tuple> resultSet = cache.retrieve(query);
        List<Tuple> results = new ArrayList<Tuple>();
        resultSet.iterator().forEachRemaining(results::add);
        return results;
    }

    public List<Tuple> retrieveAll() {
        List<Tuple> results = cache.stream().collect(Collectors.toList());
        return results;
    }

    public void removeAll() {
        cache.clear();
    }

    public void add(Tuple candidateTuple) {
        cache.add(candidateTuple);
    }

    public void addPredecessor(Tuple tuple, ProductNode predecessor) {
        Query<Tuple> query = and(equal(Tuple.TUPLE_SOURCE, tuple.getSource()), equal(Tuple.TUPLE_TARGETSTATE, tuple.getTargetState()), equal(Tuple.TUPLE_TARGET, tuple.getTarget()));
        cache.retrieve(query).uniqueResult().addPredecessor(predecessor);
    }

    public void remove(Tuple candidateTuple) {
        cache.remove(candidateTuple);
    }

    public Set<ProductNode> getPredecessor(Tuple tuple) {
        Query<Tuple> query = and(equal(Tuple.TUPLE_SOURCE, tuple.getSource()), equal(Tuple.TUPLE_TARGETSTATE, tuple.getTargetState()), equal(Tuple.TUPLE_TARGET, tuple.getTarget()));
        Set<ProductNode> predecessors = new HashSet<>();
        try {
            Tuple result = cache.retrieve(query).uniqueResult();
            predecessors = result.predecessors;
        } catch (NoSuchObjectException | NonUniqueObjectException e) {
            logger.warn("Tuple does not exist in the Cache: ", e);
        } finally {
            return predecessors;
        }

    }

    public void removePredecessor(Tuple tuple, ProductNode predecessor) {
        Query<Tuple> query = and(equal(Tuple.TUPLE_SOURCE, tuple.getSource()), equal(Tuple.TUPLE_TARGETSTATE, tuple.getTargetState()), equal(Tuple.TUPLE_TARGET, tuple.getTarget()));
        try {
            Tuple result = cache.retrieve(query).uniqueResult();
            result.predecessors.remove(predecessor);
        } catch (NoSuchObjectException | NonUniqueObjectException e) {
            logger.warn("Tuple does not exist in the Cache: ", e);
        }
    }
}
