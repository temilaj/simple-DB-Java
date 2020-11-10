package simpledb;

import java.util.*;
import java.util.function.DoubleBinaryOperator;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate pred;
    private DbIterator child1;
    private DbIterator child2;
    private TupleDesc comboTD;
    private HashMap<Object, ArrayList<Tuple>> map;
    static int MAP_SIZE = 20000;
    private Tuple t1;
    private Tuple t2;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.pred = p;
        this.child1 = child1;
        this.child2 = child2;

        TupleDesc tupleDesc1 = this.child1.getTupleDesc();
        TupleDesc tupleDesc2 = this.child2.getTupleDesc();

        this.comboTD = TupleDesc.merge(tupleDesc1, tupleDesc2);
        map = new HashMap<Object, ArrayList<Tuple>>();
    }

    public JoinPredicate getJoinPredicate() {
       return  this.pred;
    }

    public TupleDesc getTupleDesc() {
        return this.comboTD;
    }
    
    public String getJoinField1Name()
    {
        TupleDesc tupleDesc = this.child1.getTupleDesc();
        int fieldIndex = this.pred.getField1();
        return tupleDesc.getFieldName(fieldIndex);
    }

    public String getJoinField2Name()
    {
        TupleDesc tupleDesc = this.child2.getTupleDesc();
        int fieldIndex = this.pred.getField2();
        return tupleDesc.getFieldName(fieldIndex);
    }
    
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        this.child1.open();
        this.child2.open();

    }

    public void close() {
        this.child1.close();
        this.child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child1.rewind();
        this.child2.rewind();
    }

    transient Iterator<Tuple> listIt = null;

    private boolean loadMap() throws DbException, TransactionAbortedException {
        int count = 0;
        map.clear();

        // iterate though the first iterator (child1) while there's still at least one tuple left
        while(this.child1.hasNext())
        {
            this.t1 = this.child1.next();
            Field fieldValue = this.t1.getField(pred.getField1());

            // check if key (field) exisits in the map
            ArrayList<Tuple> tupleArrayList = map.get(fieldValue);
            // if it doesn't create a new list and add it to the map with the same key
            if (tupleArrayList == null)
            {
                tupleArrayList = new ArrayList<>();
                map.put(fieldValue, tupleArrayList);
            }
            // otherwise just add the current tuple to the existing list
            tupleArrayList.add(this.t1);
            count += 1;
            if (count == this.MAP_SIZE)
            {
                return true;
            }
        }
        return count > 0;
    }

    /**
     * Returns the next Tuple in the iterator, or null if the iteration is finished.
     * Operator uses this method to implement both next and hasNext.
     * 
     * @return The next Tuple in the iterator, or null if the iteration is finished.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.listIt != null && this.listIt.hasNext())
        {
            return this.processList();
        }

        // iterate through child2
        while (child2.hasNext())
        {
            this.t2 = child2.next();
            ArrayList<Tuple> tupleArrayList = map.get(this.t2.getField(pred.getField2()));
            // if there's no key match in the map, continue'
            if (tupleArrayList == null)
            {
                continue;
            }
            // if there's a match create a new tuple that concatenates the values of both tuples
            this.listIt = tupleArrayList.iterator();
            return  this.processList();
        }

        child2.rewind();
        // check if the map has been loaded and recurse if it has
        if (loadMap())
        {
            return fetchNext();
        }
        return null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no more tuples.
     * Logically, this is the next tuple in r1 cross r2 that satisfies the join predicate.
     * There are many possible implementations; the simplest is a nested loops join.
     *
     * Note that the tuples returned from this particular implementation of Join are simply the
     * concatenation of joining tuples from the left and right relation. Therefore, there will be
     * two copies of the join attribute in the results. (Removing such duplicate columns can be
     * done with an additional projection operator if needed.)
     *
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * @return The next matching tuple.
     */
    private Tuple processList() throws TransactionAbortedException, DbException {
        this.t1 = listIt.next();

        int fieldCount1 =  this.t1.getTupleDesc().numFields();
        int fieldCount2 =  this.t2.getTupleDesc().numFields();

        // create a new Tuple by concatenating the joining tuples from the left and right relation
        Tuple concatenatedTuple = new Tuple(comboTD);
        int index = 0;
        for (int i  = 0; i < fieldCount1; i++)
        {
            concatenatedTuple.setField(index, this.t1.getField(index));
            index += 1;
        }
        for (int i = 0; i < fieldCount2; i++)
        {
            concatenatedTuple.setField(index, this.t2.getField(i));
            index += 1;
        }
        return concatenatedTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {this.child1, this.child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child1 = children[0];
        this.child2 = children[1];

    }
    
}
