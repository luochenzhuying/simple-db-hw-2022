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

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator[] children;
    private TupleDesc td;
    private Tuple result;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // TODO: some code goes here
        this.tid = t;
        this.children = new OpIterator[]{child};
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"deleteNums"});
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
        this.result = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        close();
        open();
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
        // TODO: some code goes here
        if (this.result != null) {
            return null;
        }
        int deleteNums = 0;
        while (children[0].hasNext()) {
            Tuple tuple = children[0].next();
            try {
                Database.getBufferPool().deleteTuple(tid, tuple);
                deleteNums++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        this.result = new Tuple(this.td);
        this.result.setField(0, new IntField(deleteNums));
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
