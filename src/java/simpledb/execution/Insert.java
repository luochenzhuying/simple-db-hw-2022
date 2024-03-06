package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator[] children;
    private int tableId;
    private TupleDesc td;
    // store the result of the insert
    private Tuple result;
    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // TODO: some code goes here
        this.tid = t;
        this.children = new OpIterator[]{child};
        this.tableId = tableId;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"insertNums"});
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        super.open();
        children[0].open();
        this.result = null;
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        children[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        this.close();
        this.open();
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
        // TODO: some code goes here
        if (this.result != null) {
            return null;
        }
        int insertNums = 0;
        while (children[0].hasNext()) {
            try {
                Database.getBufferPool().insertTuple(this.tid, this.tableId, children[0].next());
                insertNums++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.result = new Tuple(this.td);
        result.setField(0, new IntField(insertNums));
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return this.children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
        this.children = children;
    }
}
