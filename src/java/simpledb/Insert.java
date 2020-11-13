package simpledb;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private int tableId;
    private TupleDesc tup_desc;
    private boolean already_fetched;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
    	this.t=t;
    	TupleDesc tab_desc= Database.getCatalog().getTupleDesc(tableId);
    	if (!(tab_desc.equals(child.getTupleDesc()))) 
    	{
    		throw new DbException("The table and child TupleDesc do not match");
    	}
    	this.child = child;
    	this.tableId=tableId;
    	this.already_fetched = false;
    	
    	this.tup_desc = new TupleDesc(new Type[] {Type.INT_TYPE});
//    	this.tup_desc = tab_desc;
    	
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tup_desc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	already_fetched = false; // to enable reset
    	
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (already_fetched) 
        	return null;
    	
        int count_insert = 0;
    	try 
    	{
    		child.open();
    		while(child.hasNext()) {
    			Database.getBufferPool().insertTuple(this.t, this.tableId, child.next());
    			count_insert++;
    		}
    		child.close();
    	}
    	catch(Exception e) 
    	{
    		throw new DbException("Insertion error" + e.toString());
    	}
    	already_fetched = true;
    	
    	Tuple tup = new Tuple(new TupleDesc(new Type[] {Type.INT_TYPE}));
    	tup.setField(0, new IntField(count_insert));
        return tup;        
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	if(children.length==0) 
    		throw new IllegalArgumentException("Incorrect number of elements");
    	child = children[0];
    }
}
