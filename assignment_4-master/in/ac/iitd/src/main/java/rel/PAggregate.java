package rel;

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
    private final LinkedHashMap<String, AggregationData> resultMap = new LinkedHashMap<>();


    private Iterator<Map.Entry<String, AggregationData>> resultIterator;
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
                String key = groupby(row);
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
        boolean val = hasNext();
        if (!val) {
            return null;
        }
//        System.out.println("in the next function");
        Map.Entry<String, AggregationData> entry = resultIterator.next();
        return formatResult(entry.getKey(), entry.getValue());
    }

    private String groupby(Object[] row) {
        // Use StringBuilder for efficient string concatenation
        StringBuilder keyBuilder = new StringBuilder();
        // Iterate through all bits set in groupSet (the group-by indices)
        for (int i = groupSet.nextSetBit(0); i >= 0; i = groupSet.nextSetBit(i + 1)) {
            if (keyBuilder.length() > 0) keyBuilder.append("$");
            keyBuilder.append(row[i].toString());  // Append the value at index i, converting it to string
        }
//        System.out.println("grouped value: " + keyBuilder.toString());
        return keyBuilder.toString();  // Return the concatenated key
    }
    private Object[] decodeGroupKey(String key) {
        // Split the key by the comma delimiter to get the original values
        String[] parts = key.split("$");
//        System.out.println("decoded the group string");
        return Arrays.copyOf(parts, parts.length, Object[].class);  // Convert String array to Object array
    }
    private Object[] formatResult(String groupby, AggregationData data) {
        // Decode the groupby key to get initial data array
        Object[] groupKeys = decodeGroupKey(groupby);

        // Determine the size needed for the result array
        // Assuming AggregationData contains 5 aggregated values: count, sum, min, max, avg
        Object[] result = new Object[groupKeys.length + 5];

        // Copy group keys into result array
        System.arraycopy(groupKeys, 0, result, 0, groupKeys.length);

        // Append aggregation data to the result array
        result[groupKeys.length] = data.getCount();
//        System.out.println("cout: " + data.getCount());
        result[groupKeys.length + 1] = data.getSum();
//        System.out.println("sum: " + data.getSum());
        result[groupKeys.length + 2] = data.getMin();
//        System.out.println("min: " + data.getMin());
        result[groupKeys.length + 3] = data.getMax();
//        System.out.println("max: " + data.getMax());
        result[groupKeys.length + 4] = data.getAverage();
//        System.out.println("average: " + data.getAverage());

        return result;
    }



    public class AggregationData {
        private long count = 0;  // For COUNT
        private double sum = 0.0;  // For SUM
        private double min = Double.MAX_VALUE;  // For MIN
        private double max = Double.MIN_VALUE;  // For MAX
        private double average = 0.0;  // For AVG

        // Method to update aggregation results based on a new row and the list of aggregation calls
        public void accumulate(Object[] row, List<AggregateCall> aggCalls) {
//            System.out.println("in accumulate function");
            for (AggregateCall call : aggCalls) {
                List<Integer> argList = call.getArgList();
                double value = 0;
                int functionIndex;
                if (!argList.isEmpty()) {
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
        public long getCount() {
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