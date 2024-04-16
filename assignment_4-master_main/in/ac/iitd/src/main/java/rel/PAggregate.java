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
    private final LinkedHashMap<List<Object>, Data_agg> Map_req = new LinkedHashMap<>();


    private Iterator<Map.Entry<List<Object>, Data_agg>> req_iterator;
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
                List<Object> key_req = hasher(row);
                Map_req.putIfAbsent(key_req, new Data_agg());
                Map_req.get(key_req).aggregate_res(row, aggCalls);
            }
            ((PRel)input).close();
            req_iterator = Map_req.entrySet().iterator();
            return true;
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PAggregate");
        Map_req.clear();
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PAggregate has next");
        return req_iterator.hasNext();
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PAggregate");
        boolean val = hasNext();

        if (!val) {
            return null;
        }
        Map.Entry<List<Object>, Data_agg> entry = req_iterator.next();
        return final_output(entry.getKey(), entry.getValue());
    }

    private List<Object> hasher(Object[] row) {
        List<Object> keylist = new ArrayList<>();
        for (int i = groupSet.nextSetBit(0); i >= 0; i = groupSet.nextSetBit(i + 1)){

            keylist.add(row[i]);
        }
        return keylist;
    }
    private Object[] final_output(List<Object> grouping_keys, Data_agg data) {
        List<Object> resultList = new ArrayList<>(grouping_keys); // Keeps the initial grouping keys

        for (AggregateCall call : aggCalls) {
            String aggregationType = call.getAggregation().toString();
            String type = call.getType().toString();

            switch (aggregationType) {
                case "COUNT":
                    resultList.add(data.getCount());
                    break;

                case "SUM":
                    resultList.add(formatResultByType(data.getSum(), type));
                    break;

                case "MIN":
                    resultList.add(formatResultByType(data.getMin(), type));
                    break;

                case "MAX":
                    resultList.add(formatResultByType(data.getMax(), type));
                    break;

                case "AVG":
                    resultList.add(data.getAverage());
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported aggregation function: " + aggregationType);
            }
        }
        return resultList.toArray(new Object[0]);
    }

    private Object formatResultByType(double value, String dataType) {
        switch (dataType) {
            case "INTEGER":
                return (int) Math.round(value);
            case "DOUBLE":
                return value;
            case "FLOAT":
                return (float) value;
            default:
                throw new IllegalArgumentException("Unsupported data type for aggregation: " + dataType);
        }
    }

    public class Data_agg {
        private int count = 0;
        private double sum = 0.0;
        private double min = Double.MAX_VALUE;
        private double max = Double.MIN_VALUE;
        private double average = 0.0;

        public void aggregate_res(Object[] row, List<AggregateCall> aggCalls) {
            for (AggregateCall call : aggCalls) {
                List<Integer> argList = call.getArgList();
                double value = 0;

                if (!argList.isEmpty()) {
                    int functionIndex = argList.get(0);
                    Object rawValue = row[functionIndex];
                    value = extractNumericValue(rawValue);
                }

                switch (call.getAggregation().toString()) {
                    case "COUNT":
                        count++;
                        break;
                    case "SUM":
                    case "MIN":
                    case "MAX":
                        applyAggregation(call.getAggregation().toString(), value);
                        break;
                    case "AVG":
                        updateAverage(value);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported aggregation function: " + call.getAggregation().toString());
                }
            }
        }

        private double extractNumericValue(Object value) {
            if (value instanceof Double) {
                return (double) value;
            }
            else if (value instanceof Integer){
                return (Integer) value;
            }
            else if (value instanceof Float){
                return (float) value;
            }
            else {
                throw new IllegalArgumentException("Expected numeric type but found: " + value.getClass().getSimpleName());
            }
        }

        private void applyAggregation(String type, double value) {
            switch (type) {
                case "SUM":
                    sum += value;
                    break;
                case "MIN":
                    min = Math.min(min, value);
                    break;
                case "MAX":
                    max = Math.max(max, value);
                    break;
            }
        }

        private void updateAverage(double value) {
            average = ((average * count) + value) / (count + 1);
            count++;
        }

        public int getCount() { return count; }
        public double getSum() { return sum; }
        public double getMin() { return min; }
        public double getMax() { return max; }
        public double getAverage() { return average; }
    }

}

