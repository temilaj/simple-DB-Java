package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate predicate;
    private DbIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        super();
        this.predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        this.child.open();
    }

    public void close() {
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // Iterate over tuples from the child operator
        while (this.child.hasNext())
        {
            Tuple currentTuple = this.child.next();
            // attempt to filter with the predicate
            if (this.predicate.filter(currentTuple))
            {
                // return the tuple if successful
                return currentTuple;
            }
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // return the children DbIterators of this operator.
        // If there is only one child, return an array of only one element.
        DbIterator[] children = new DbIterator[] { this.child };
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length == 0)
        {
            throw new IllegalArgumentException("Incorrect number of elements supplied");
        }
        // set the current child to the first element of the children array
        this.child = children[0];
    }

}
