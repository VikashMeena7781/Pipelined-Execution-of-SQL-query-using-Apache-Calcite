package rel;

import javafx.beans.binding.ObjectExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;

import convention.PConvention;

import java.util.*;
import java.util.Map;

// Count, Min, Max, Sum, Avg
public class PAggregate extends Aggregate implements PRel {

    public PAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof PConvention;
    }
    //    private final HashMap<String, AggregationData> resultMap = new HashMap<>();
    private final LinkedHashMap<List<Object>, AggregationData> resultMap = new LinkedHashMap<>();

//    private int num_rows_returned = 0;
//    private List<Object[]> rowsreq;
//    private List<Object[]> answer;

    private Iterator<Map.Entry<List<Object>, AggregationData>> resultIterator;
    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new PAggregate(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public String toString() {
        return "PAggregate";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PAggregate");
        /* Write your code here */
        if (input instanceof PRel){
            ((PRel) input).open();
            while(((PRel)input).hasNext()) {
                Object[] row = ((PRel) input).next();
                List<Object> key = groupby(row);
//                System.out.println("row: " + key);

//                System.out.println("key: " + key);
                resultMap.putIfAbsent(key, new AggregationData());
//                System.out.println("updated into the aggregation function");
                resultMap.get(key).accumulate(row, aggCalls);
//                System.out.println("size of result map: " + resultMap.size());
            }
            ((PRel)input).close();
//            System.out.println("closed the input successfully");
            resultIterator = resultMap.entrySet().iterator();
            return true;
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PAggregate");
        /* Write your code here */
//        System.out.println("size before clearing" + resultMap.size());
        resultMap.clear();
//        System.out.println("size after clearing" + resultMap.size());
//        System.out.println("cleared the result map");
//        num_rows_returned = 0;
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PAggregate has next");
        /* Write your code here */
        boolean val = resultIterator.hasNext();
//        System.out.println("has next: " + val);
        return val;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PAggregate");
//        System.out.println("in the next function");
        boolean val = hasNext();

        if (!val) {
            return null;
        }
        Map.Entry<List<Object>, AggregationData> entry = resultIterator.next();
        return formatResult(entry.getKey(), entry.getValue());
    }

    private List<Object> groupby(Object[] row) {
        // List to hold group-by values
        List<Object> keyValues = new ArrayList<>();
//        System.out.println("group set empty: " + groupSet.isEmpty());
        // Iterate through all bits set in groupSet (the group-by indices)
        for (int i = groupSet.nextSetBit(0); i >= 0; i = groupSet.nextSetBit(i + 1)){

            keyValues.add(row[i]);  // Add the value at index i
        }
//        if (keyValues.isEmpty()) {
//            return new Object[0];  // Correct syntax to return an empty array
//        }
        // Convert list to array and return
        return keyValues;
    }
    //    private Object[] decodeGroupKey(String key) {
//        // Split the key by the comma delimiter to get the original values
//        String[] parts = key.split("$");
////        System.out.println("decoded the group string");
//        return Arrays.copyOf(parts, parts.length, Object[].class);  // Convert String array to Object array
//    }
    private Object[] formatResult(List<Object> groupKeys, AggregationData data) {
        // Determine the size needed for the result list including the aggregated values
        List<Object> resultList = new ArrayList<>(groupKeys); // Initializes resultList with groupKeys

        // Iterate over each AggregateCall to handle multiple aggregations in a single query
        for (AggregateCall call : aggCalls) {
            String aggregationType = call.getAggregation().toString();
            String type = call.getType().toString();

            switch (aggregationType) {
                case "COUNT":
                    resultList.add(data.getCount());
                    break;

                case "SUM":
                    switch (type) {
                        case "INTEGER":
                            resultList.add(Math.round(data.getSum())); // Round to nearest integer
                            break;
                        case "DOUBLE":
                            resultList.add((double) Math.round(data.getSum())); // Round to nearest integer but store as double
                            break;
                        case "FLOAT":
                            resultList.add(data.getSum()); // Convert from double to float without rounding
                            break;
                    }
                    break;

                case "MIN":
                    switch (type) {
                        case "INTEGER":
                            resultList.add((int) Math.round(data.getMin())); // Cast to integer after rounding
                            break;
                        case "DOUBLE":
                            resultList.add(data.getMin()); // Keep as double
                            break;
                        case "FLOAT":
                            resultList.add(data.getMin()); // Convert from double to float
                            break;
                    }
                    break;

                case "MAX":
                    switch (type) {
                        case "INTEGER":
                            resultList.add((int) Math.round(data.getMax())); // Cast to integer after rounding
                            break;
                        case "DOUBLE":
                            resultList.add(data.getMax()); // Keep as double
                            break;
                        case "FLOAT":
                            resultList.add(data.getMax()); // Convert from double to float
                            break;
                    }
                    break;

                case "AVG":
                    resultList.add(data.getAverage());
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported aggregation function: " + aggregationType);
            }
        }

        // Convert resultList to array for compatibility with existing interfaces
        return resultList.toArray(new Object[0]);
    }





    public class AggregationData {
        private int count = 0;  // For COUNT
        private double sum = 0.0;  // For SUM
        private double min = Double.MAX_VALUE;  // For MIN
        private double max = Double.MIN_VALUE;  // For MAX
        private double average = 0.0;  // For AVG

        // Method to update aggregation results based on a new row and the list of aggregation calls
        public void accumulate(Object[] row, List<AggregateCall> aggCalls) {
//            System.out.println("in accumulate function");
            for (AggregateCall call : aggCalls) {
//                System.out.println(call.getAggregation());
                List<Integer> argList = call.getArgList();
                double value = 0;
                int functionIndex;
                if (!argList.isEmpty()) {
//                    System.out.println("arguement list");
//                    for(Integer i : argList){
//                        System.out.println(i + " ");
//                    }
//                    System.out.println();
//                    System.out.println("Argument list for " + call.getAggregation() + " is not empty. Skipping this aggregation.");
                    functionIndex = call.getArgList().get(0);  // Assuming single argument functions for simplicity
//                    System.out.println("index of value: " + call.getArgList().get(0));
                    Object rawValue = row[functionIndex];
                    if (rawValue instanceof Double) {
                        value = (double) row[functionIndex];  // Cast or convert row value as needed
                    }
                    else if (rawValue instanceof Integer) {
                        value = (Integer) row[functionIndex];  // Cast or convert row value as needed
                    }
                    else if (rawValue instanceof Float){
                        value = (float) row[functionIndex];
                    }
                    else{
                        throw new IllegalArgumentException("Expected numeric type but found: " + rawValue.getClass().getSimpleName());
                    }
//                    System.out.println("value: " + value);
//                    continue;  // Skip this iteration if there are no arguments for the aggregation function
                }



                switch (call.getAggregation().toString()) {
                    case "COUNT":
                        count++;
                        break;
                    case "SUM":
                        sum += value;
                        break;
                    case "MIN":
                        min = Math.min(min, value);
                        break;
                    case "MAX":
                        max = Math.max(max, value);
                        break;
                    case "AVG":
                        average = ((average * count) + value) / (count + 1);  // Update average dynamically
                        count++;
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported aggregation function");
                }
            }
        }

        // Getter methods for each aggregation result
        public int getCount() {
            return count;
        }

        public double getSum() {
            return sum;
        }

        public double getMin() {
            return min;
        }

        public double getMax() {
            return max;
        }

        public double getAverage() {
            return average;
        }
    }
}