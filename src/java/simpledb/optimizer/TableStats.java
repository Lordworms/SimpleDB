package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.swing.text.html.parser.Entity;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private HeapFile tableFile;
    private HashMap<String,Integer[]>attrToStats;//name to min/max
    private HashMap<String,Object>nameToHis;
    private DbFileIterator iter;
    private TupleDesc desc;
    private int ioCostPerPage;
    private int number;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // TODO: some code goes here
        this.ioCostPerPage=ioCostPerPage;
        this.tableFile=(HeapFile)Database.getCatalog().getDatabaseFile(tableid);
        this.desc=tableFile.getTupleDesc();
        this.attrToStats=new HashMap<>();
        this.nameToHis=new HashMap<>();
        Transaction t=new Transaction();
        this.iter=tableFile.iterator(t.getId());
        this.number=0;//total tuple number
        initHistorgam();
    }
    /*
     * Initialize all historgam
     * 
     */
    public void initHistorgam(){
        try {
            this.iter.open();
            while(iter.hasNext()){
                this.number++;
                Tuple t=iter.next();
                for(int i=0;i<desc.numFields();++i){
                    Type type=desc.getFieldType(i);
                    String name=desc.getFieldName(i);
                    if(type==Type.INT_TYPE){//only int could insert into
                        Integer value=((IntField)t.getField(i)).getValue();
                        if(this.attrToStats.containsKey(name)){
                            Integer[] stats=this.attrToStats.get(name);
                            stats[0]=Integer.min(value,stats[0]);
                            stats[1]=Integer.max(value,stats[1]);
                        }else{
                            attrToStats.put(name, new Integer[]{value,value});
                        }
                    }
                }
            }
            for (Map.Entry<String, Integer[]> entry : attrToStats.entrySet()) {
                Integer[] stats=entry.getValue();
                this.nameToHis.put(entry.getKey(),new IntHistogram(NUM_HIST_BINS, stats[0], stats[1]));
            }
            iter.rewind();
            while(iter.hasNext()){
                Tuple t=iter.next();
                for(int i=0;i<desc.numFields();++i){
                    String name=desc.getFieldName(i);
                    Type type=desc.getFieldType(i);
                    if(type==Type.STRING_TYPE){
                        String value=((StringField)t.getField(i)).getValue();
                        if(nameToHis.containsKey(name)){
                            StringHistogram graph=(StringHistogram)this.nameToHis.get(name);
                            graph.addValue(value);
                            this.nameToHis.put(name, graph);
                        }else{
                            StringHistogram graph=new StringHistogram(NUM_HIST_BINS);
                            graph.addValue(value);
                            nameToHis.put(name, graph);
                        }
                    }else{
                        int value=((IntField)t.getField(i)).getValue();
                        IntHistogram graph=(IntHistogram)this.nameToHis.get(name);
                        graph.addValue(value);
                        this.nameToHis.put(name, graph);
                    }
                }
            }
        } catch (DbException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
    }
    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // TODO: some code goes here
        return tableFile.numPages()*this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // TODO: some code goes here
        return (int)Math.ceil(1.0*this.number*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // TODO: some code goes here
        String name=desc.getFieldName(field);
        if(constant.getType()==Type.INT_TYPE){
            int value=((IntField)constant).getValue();
            IntHistogram graph=(IntHistogram)this.nameToHis.get(name);
            return graph.estimateSelectivity(op, value);
        }else{
            String value=((StringField)constant).getValue();
            StringHistogram graph=(StringHistogram)this.nameToHis.get(name);
            return graph.estimateSelectivity(op, value);
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return this.number;
    }

}
