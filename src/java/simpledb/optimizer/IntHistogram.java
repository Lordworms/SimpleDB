package simpledb.optimizer;

import simpledb.execution.Predicate;
import simpledb.execution.Predicate.Op;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int min_value;
    private int max_value;
    private int[] graphs;
    private int width;
    private int buckets;
    private int number;//total number
    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // TODO: some code goes here
        this.max_value=max;
        this.min_value=min;
        this.buckets=buckets;
        this.width=(int)Math.ceil(1.0*(max-min+1)/buckets);
        this.graphs=new int[buckets];
        for(int i=0;i<buckets;++i){
            this.graphs[i]=0;
        }
        this.number=0;
    }
    public int getIndex(int v){
        return (v==this.max_value?this.buckets-1:(v-this.min_value)/width);
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int index=getIndex(v);
        this.graphs[index]++;
        this.number++;
    }
    private boolean valid(int v){
        return v>=this.min_value && v<=this.max_value;
    }
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int index=getIndex(v);
        int left_range=index*width+this.min_value;
        int right_range=index*width+this.min_value+width-1;
        switch(op){
            case EQUALS:{
                if(!valid(v)){
                    return 0.0;
                }
                return 1.0*this.graphs[index]/(this.width*this.number);
            }
            case LESS_THAN:{
                if(v<this.min_value){
                    return 0.0;
                }
                if(v>this.max_value){
                    return 1.0;
                }
                int outer_value=0;
                for(int i=index-1;i>=0;--i){
                    outer_value+=this.graphs[i];
                }
                double outer_prob=1.0*outer_value/this.number;
                double inner_prob=1.0*(v-left_range)/(this.width*this.number);
                return inner_prob+outer_prob;
            }
            case GREATER_THAN:{
                if(v<this.min_value){
                    return 1.0;
                }
                if(v>this.max_value){
                    return 0.0;
                }
                int outer_value=0;
                for(int i=index+1;i<this.buckets;++i){
                    outer_value+=this.graphs[i];
                }
                double outer_prob=1.0*outer_value/this.number;
                double inner_prob=1.0*(right_range-v)/(this.width*this.number);
                return inner_prob+outer_prob;
            }
            case GREATER_THAN_OR_EQ:{
                return 1.0-this.estimateSelectivity(Op.LESS_THAN, v);
            }
            case LESS_THAN_OR_EQ:{
                return 1.0-this.estimateSelectivity(Op.GREATER_THAN, v);
            }
            case LIKE:{
                return 1.0;
            }
            case NOT_EQUALS:{
                return 1.0-this.estimateSelectivity(Op.EQUALS, v);
            }
        }
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // TODO: some code goes here
        return null;
    }
}
