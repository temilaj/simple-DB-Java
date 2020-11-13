package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private TupleDesc tup_desc;
    private boolean already_fetched;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.t=t;
    	this.child=child;
    	this.tup_desc =  new TupleDesc(new Type[] {Type.INT_TYPE});
    	this.already_fetched=false;
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
    	already_fetched = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (already_fetched) 
        	return null;
    	
    	int count_deleted = 0;
    	try 
    	{
    		child.open();
    		while(child.hasNext()) {
    			Database.getBufferPool().deleteTuple(this.t, child.next());
    			count_deleted++;
    		}
    		child.close();
    	}
    	catch(IOException e) 
    	{
//    		throw new DbException("Insertion error");
    		e.printStackTrace();
    	}
    	already_fetched = true;
    	
    	Tuple tup = new Tuple(new TupleDesc(new Type[] {Type.INT_TYPE}));
    	tup.setField(0, new IntField(count_deleted));
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
    	if(children.length < 1) 
    		throw new IllegalArgumentException("Incorrect number of elements");
    	child = children[0];
    }

}
