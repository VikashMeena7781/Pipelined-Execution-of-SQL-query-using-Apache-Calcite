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

    private List<Object[]> sortedData;
    private int currentIndex = 0;

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
//        System.out.println("in open of sort");
        if (input instanceof PRel) {
            ((PRel) input).open();
            sortedData = new ArrayList<>();
            while (((PRel) input).hasNext()) {
                Object[] row = ((PRel) input).next();
//                System.out.println("actor id: " + row[0]);
                sortedData.add(row);
            }
            ((PRel) input).close();

            // Sort the data based on the collation fields
            Collections.sort(sortedData, new Comparator<Object[]>() {
                @Override
                public int compare(Object[] o1, Object[] o2) {
                    for (RelFieldCollation field : collation.getFieldCollations()) {
                        int index = field.getFieldIndex();
                        Comparable val1 = (Comparable) o1[index];
                        Comparable val2 = (Comparable) o2[index];

                        // Handle nulls: Assume nulls last
                        if (val1 == null && val2 == null) continue;
                        if (val1 == null) return 1;
                        if (val2 == null) return -1;

                        int result = val1.compareTo(val2);
                        if (result != 0) {
                            return field.getDirection() == RelFieldCollation.Direction.ASCENDING ? result : -result;
                        }
                    }
                    return 0;
                }
            });

//            System.out.println("done with sorting");
            currentIndex = 0; // Reset index after sorting
            return true;
        }
        return false;
    }

    // Clean up resources
    @Override
    public void close(){
        logger.trace("Closing PSort");
        if (sortedData != null) {
            sortedData.clear();
        }
        currentIndex = 0;
//        System.out.println("clear sorted data");
        return;
    }

    // Check if more rows are available
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PSort has next");
//        System.out.println("has Next: " + (sortedData != null && currentIndex < sortedData.size()));
        return sortedData != null && currentIndex < sortedData.size();
    }

    // Return the next sorted row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PSort");
//        System.out.println("in next");
        if (!hasNext()) {
            return null;
        }
        Object[] req = sortedData.get(currentIndex++);
//        System.out.println("actor id : " + req[0]);
        return req;
    }
}
