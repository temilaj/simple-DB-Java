package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private File f; // file which store data
	private TupleDesc td; // the schema
	private int tableID; // the table id corresponding to this file
	private int num_pages; // number of pages in file

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f = f;
    	this.td = td;
    	this.tableID = f.getAbsoluteFile().hashCode();
    	this.num_pages = (int)Math.ceil(f.length()/BufferPool.getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
    	return tableID;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
    	return this.td;
    }

    // see DbFile.java for javadocs
    /**
     * Read the specified page from disk.
     *
     * @throws IllegalArgumentException if the page does not exist in this file.
     */
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // To read a page from disk, you will first need to calculate the correct
    	// offset in the file. Hint: you will need random access to the file in order 
    	// to read and write pages at arbitrary offsets. You should not call BufferPool
    	// methods when reading a page from disk.
    	
    	// get page number, then read from it 
    	try {
    		RandomAccessFile file = new RandomAccessFile(f,"r");
    		int offset = BufferPool.getPageSize() * pid.pageNumber();
    		byte [] data = new byte[BufferPool.getPageSize()];
    		int read_len = Math.min(BufferPool.getPageSize(), (int)(f.length() - offset));
    		file.seek(offset);	// needed to ensure out of bounds access does not happen
    		// seek automatically raise exception
    		file.read(data,0,read_len);
    		file.close();
    		// now construct a page from data 
    		HeapPageId id = new HeapPageId(tableID,pid.pageNumber());
    		HeapPage pg = new HeapPage(id, data);
    		return pg;
    		}
    	catch(Exception e)
    	{
    		throw new IllegalArgumentException();
    	}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
		byte[] pageData = page.getPageData();
		long offsetPosition = BufferPool.getPageSize() * page.getId().pageNumber();
		try
		{
			RandomAccessFile file = new RandomAccessFile(this.f, "rw");
            // Set the file-pointer offset,
			file.seek(offsetPosition);
			file.write(pageData);
            // Close file stream
			file.close();
		}
		catch(Exception e)
		{
			throw new IOException("error writing page to file");
		}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	return num_pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	ArrayList<Page> pageList = new ArrayList<>();
    	BufferPool bufferPool = Database.getBufferPool();
    	return pageList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        try
        {
            PageId pageId = t.getRecordId().getPageId();
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
			heapPage.deleteTuple(t);
            ArrayList<Page> pageList = new ArrayList<>();
            Collections.addAll(pageList, heapPage);

            return pageList;
        }
        catch(DbException e)
        {
            throw new DbException("Error deleting tuple" + e.getMessage());
        }
    }

    // see DbFile.java for javadocs 
    public DbFileIterator iterator(TransactionId tid) {
    	
    	DbFileIterator iter = new DbFileIterator() {
    		
    		private HeapPage cur_page = null;
            private boolean is_open = false;
            private Iterator<Tuple> cursor = null;
            
            @Override
            public void open() throws DbException, TransactionAbortedException {
            	
            	try {
                is_open = true;
                HeapPageId hpid = new HeapPageId(getId(), 0);
                cur_page = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
                cursor = cur_page.iterator();
            	}
            	catch(Exception e)
            	{
            		throw new DbException("Opening file failed" + e.toString());
            	}
            }
    		
            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
            	
            	try {

	                if ((!is_open) || (cur_page == null)) 
	                	return false;
	
	                if (cursor.hasNext()) // If more tuples in page
	                	return true;
	                else {
	                    // Check if next page exits
	                	
	                	int cur_page_num = cur_page.getId().pageNumber();
	                    int next_page_num = (cur_page_num + 1);
	                    if ((next_page_num<0) || (next_page_num == num_pages))
	                        return false;
	
	                    // If yes load pages
	                    HeapPageId hpid = new HeapPageId(getId(), next_page_num);
	                    cur_page = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
	                    cursor = cur_page.iterator();
	                    
	                    // Then next tuple
	                    return cursor.hasNext();
	                }
            	}
            	catch(Exception e)
            	{
            		throw new DbException("Error checking for next page");
            	}
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                
            	if(!hasNext())
            	{
            		throw new NoSuchElementException();
            	}
            	
            	return cursor.next();
//				return null;
            }
            
            @Override
            public void rewind() throws DbException, TransactionAbortedException {
            	try {
	            	HeapPageId hpid = new HeapPageId(getId(), 0);
	                cur_page = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
	                cursor = cur_page.iterator();
            	}
            	catch(Exception e)
            	{
            		throw new DbException("Error while resetting");
            	}
            }
            
            @Override
            public void close() {
            	is_open = false;
            	cur_page = null;
                cursor = null;
            }

    	};
        return iter;

        
    }

}

