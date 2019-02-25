package ca.uwaterloo.cs.streamingrpq.util;

import ca.uwaterloo.cs.streamingrpq.core.SubPath;
import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.compound.CompoundIndex;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.resultset.ResultSet;
import com.googlecode.cqengine.resultset.common.NoSuchObjectException;
import com.googlecode.cqengine.resultset.common.NonUniqueObjectException;

import java.util.ArrayList;
import java.util.List;

import static com.googlecode.cqengine.query.QueryFactory.*;

/**
 * Created by anilpacaci on 2019-01-31.
 */
public class Cache<S> {

    IndexedCollection<SubPath> cache;

    public Cache() {
        cache = new ConcurrentIndexedCollection<SubPath>();
        cache.addIndex(HashIndex.onAttribute(SubPath.TUPLE_TARGET));
        cache.addIndex(CompoundIndex.onAttributes(SubPath.TUPLE_SOURCE, SubPath.TUPLE_SOURCESTATE));
    }

    /**
     * Insert elements in to the cache if it does not exists, increases the counter if it does exists
     * @param subPath
     * @return false if element already exists in the cache
     */
    public boolean insertOrIncrement(SubPath subPath) {
        Query<SubPath> query = and(equal(SubPath.TUPLE_SOURCE, subPath.getSource()), equal(SubPath.TUPLE_SOURCESTATE, subPath.getSourceState()), equal(SubPath.TUPLE_TARGET, subPath.getTarget()));
        try {
            SubPath existing = cache.retrieve(query).uniqueResult();
            // subpath exists, simply increment
            existing.increment();
            return false;
        } catch (NoSuchObjectException e) {
            // object does not exists, simply processEdge
            cache.add(subPath);
            return true;
        }
    }

    /**
     * decrements element's counter in the cache, and removes it if reaches 0
     * @param subPath
     * @return true if element is removed from the cache
     */
    public boolean removeOrDecrement(SubPath subPath) {
        Query<SubPath> query = and(equal(SubPath.TUPLE_SOURCE, subPath.getSource()), equal(SubPath.TUPLE_SOURCESTATE, subPath.getSourceState()), equal(SubPath.TUPLE_TARGET, subPath.getTarget()));
        // note that subpath has to exists
        try {
            SubPath existing = cache.retrieve(query).uniqueResult();
            existing.decrement();
            if(existing.getCounter() == 0) {
                // remove the entry from the cache
                cache.remove(subPath);
                return true;
            }
        } catch (NoSuchObjectException e) {
            throw e;
        }
        return false;
    }

    public boolean contains(SubPath subPath) {
        Query<SubPath> query = and(equal(SubPath.TUPLE_SOURCE, subPath.getSource()), equal(SubPath.TUPLE_SOURCESTATE, subPath.getSourceState()), equal(SubPath.TUPLE_TARGET, subPath.getTarget()));
        return cache.retrieve(query).isNotEmpty();
    }

    public List<SubPath> retrieveByTarget(Integer target) {
        Query<SubPath> query = equal(SubPath.TUPLE_TARGET, target);
        ResultSet<SubPath> resultSet = cache.retrieve(query);
        List<SubPath> results = new ArrayList<SubPath>();
        resultSet.iterator().forEachRemaining(results::add);
        return results;
    }

    public List<SubPath> retrieveBySourceStateAndTarget(Integer sourceState, Integer target) {
        Query<SubPath> query = and(equal(SubPath.TUPLE_SOURCESTATE, sourceState), equal(SubPath.TUPLE_TARGET, target));
        ResultSet<SubPath> resultSet = cache.retrieve(query);
        List<SubPath> results = new ArrayList<SubPath>();
        resultSet.iterator().forEachRemaining(results::add);
        return results;
    }

    public List<SubPath> retrieveBySource(Integer source, Integer sourceState) {
        Query<SubPath> query = and(equal(SubPath.TUPLE_SOURCE, source), equal(SubPath.TUPLE_SOURCESTATE, sourceState));
        ResultSet<SubPath> resultSet = cache.retrieve(query);
        List<SubPath> results = new ArrayList<SubPath>();
        resultSet.iterator().forEachRemaining(results::add);
        return results;
    }

    public List<SubPath> retrieveBySource(List<Integer> source, Integer sourceState) {
        Query<SubPath> query = and(in(SubPath.TUPLE_SOURCE, source), equal(SubPath.TUPLE_SOURCESTATE, sourceState));
        ResultSet<SubPath> resultSet = cache.retrieve(query);
        List<SubPath> results = new ArrayList<SubPath>();
        resultSet.iterator().forEachRemaining(results::add);
        return results;
    }

    public List<SubPath> retrieveBySourceState(Integer sourceState) {
        Query<SubPath> query = equal(SubPath.TUPLE_SOURCESTATE, sourceState);
        ResultSet<SubPath> resultSet = cache.retrieve(query);
        List<SubPath> results = new ArrayList<SubPath>();
        resultSet.iterator().forEachRemaining(results::add);
        return results;
    }
}
