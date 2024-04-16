package rel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

public class PSort extends Sort implements PRel {

    private List<Object[]> Data_sort;
    private List<Object[]> sorted_result;
    private int idx_to_send = 0;

    public PSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode child,
            RelCollation collation,
            RexNode offset,
            RexNode fetch
    ) {
        super(cluster, traits, hints, child, collation, offset, fetch);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new PSort(getCluster(), traitSet, hints, input, collation, offset, fetch);
    }

    @Override
    public String toString() {
        return "PSort";
    }

    // Initializes sorting process
    @Override
    public boolean open(){
        logger.trace("Opening PSort");
        if (input instanceof PRel) {
            ((PRel) input).open();
            Data_sort = new ArrayList<>();
            while (((PRel) input).hasNext()) {
                Object[] row = ((PRel) input).next();
                Data_sort.add(row);
            }
            ((PRel) input).close();
            sorted_result = sort_func(Data_sort);
            idx_to_send = 0;
            return true;
        }
        return false;
    }

    private List<Object[]> sort_func(List<Object[]> data){
        Collections.sort(data, new Comparator<Object[]>() {
            @Override
            public int compare(Object[] first, Object[] second) {
                for (RelFieldCollation field : collation.getFieldCollations()) {
                    int fieldIndex = field.getFieldIndex();
                    Comparable firstValue = (Comparable) first[fieldIndex];
                    Comparable secondValue = (Comparable) second[fieldIndex];

                    if (firstValue == null || secondValue == null) {
                        if (firstValue == null && secondValue == null) {
                            continue;
                        }
                        return firstValue == null ? 1 : -1;
                    }

                    int comparisonResult = firstValue.compareTo(secondValue);
                    if (comparisonResult != 0) {
                        if (field.getDirection() == RelFieldCollation.Direction.ASCENDING) {
                            return comparisonResult;
                        } else {
                            return -comparisonResult;
                        }
                    }
                }
                return 0;
            }
        });
        return data;
    }
    // Clean up resources
    @Override
    public void close(){
        logger.trace("Closing PSort");
        if (Data_sort != null) {
            Data_sort.clear();
            sorted_result.clear();
        }
        idx_to_send = 0;
        return;
    }

    @Override
    public boolean hasNext(){
        logger.trace("Checking if PSort has next");
        return sorted_result != null && idx_to_send < sorted_result.size();
    }

    // Return the next sorted row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PSort");
        if (!hasNext()) {
            return null;
        }
        Object[] req = sorted_result.get(idx_to_send);
        idx_to_send++;
        return req;
    }
}
