package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;

import convention.PConvention;

import java.util.*;

/*
 * Implements a hash join where:
 * - Right side (build side) is blocking: All rows are read into a hash table before the join begins.
 * - Left side (probe side) is streaming: Rows are read and joined as they come.
 * Supports multi-attribute join keys.
 */
public class PJoin extends Join implements PRel {

    private final Map<List<Object>, List<Object[]>> hashMap = new HashMap<>();
    private Object[] currentProbeRow;
    private Iterator<Object[]> currentMatchIterator;
    private boolean moreLeftRows = true;

    private int left_size;

    public PJoin(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
        super(cluster, traits, ImmutableList.of(), left, right, condition, variablesSet, joinType);
        assert traits.getConvention() instanceof PConvention;
    }

    @Override
    public PJoin copy(RelTraitSet traits, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new PJoin(getCluster(), traits, left, right, condition, variablesSet, joinType);
    }

    @Override
    public boolean open() {
//        System.out.println(joinType);
//        System.out.println(variablesSet.size());
        System.out.println("Condition "+condition.toString());
//        for (CorrelationId id : variablesSet) {
//            System.out.println(id);
//        }
        PRel leftCopy = null;
        // Make a copy of the left pointer
        if (left instanceof PRel) {
            leftCopy = (PRel) left;
            leftCopy.open();
        }
        // Find the length of the left table row
        if (leftCopy != null && leftCopy.hasNext()) {
            currentProbeRow = leftCopy.next();
            left_size = currentProbeRow.length;
            System.out.println("Size of left "+left_size);
        } else {
            // Left table is empty
            left_size = 0;
        }
        leftCopy.close();

        if (right instanceof PRel) {
            ((PRel) right).open();
            while (((PRel) right).hasNext()) {
                Object[] row = ((PRel) right).next();
//                left_size=4;
                List<Object> key = extractJoinKey(row, true);
                hashMap.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
            ((PRel) right).close();

            if (left instanceof PRel) {
                ((PRel) left).open();
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean hasNext() {
        if (currentMatchIterator != null && currentMatchIterator.hasNext()) {
            return true;
        }
        while (moreLeftRows && left instanceof PRel && ((PRel) left).hasNext()) {
            currentProbeRow = ((PRel) left).next();
//            left_size=currentProbeRow.length;
            List<Object> probeKey = extractJoinKey(currentProbeRow, false);
            List<Object[]> matches = hashMap.get(probeKey);
            if (matches != null && !matches.isEmpty()) {
                currentMatchIterator = matches.iterator();
                return true;
            }
        }

        moreLeftRows = false;  // No more rows to probe
        return false;
    }

    @Override
    public Object[] next() {
        if (currentMatchIterator == null || !currentMatchIterator.hasNext()) {
            throw new NoSuchElementException("No more rows available in PJoin.");
        }

        Object[] buildRow = currentMatchIterator.next();

        // Create an array to hold the combined row data
        Object[] combinedRow = new Object[currentProbeRow.length + buildRow.length];

        // Copy all elements from the left row (probe side)
        System.arraycopy(currentProbeRow, 0, combinedRow, 0, currentProbeRow.length);

        // Copy all elements from the right row (build side)
        System.arraycopy(buildRow, 0, combinedRow, currentProbeRow.length, buildRow.length);

        return combinedRow;
    }

    @Override
    public void close() {
        if (left instanceof PRel) {
            ((PRel) left).close();
        }
        hashMap.clear();
    }

    private List<Object> extractJoinKey(Object[] row, boolean isRightSide) {
        // Placeholder: You need to customize this method based on actual join conditions
        // Example for a join on the first two columns
//        isBuildSide true --> right --> 1
        List<Integer> answer= new ArrayList<>();
//        System.out.println("left size "+left_size);
        if (isRightSide){
//            System.out.println(row[0]);
            find_index(condition,1,answer);
//            return Arrays.asList(row[0]);
//            System.out.println("Size2 "+answer.size());

            List<Object> final_ans = new ArrayList<>();
            for (int i = 0; i < answer.size(); i++) {
                final_ans.add(row[answer.get(i)-left_size]);
            }
            return final_ans;
        }else{
            find_index(condition,0,answer);
//            System.out.println("Size1 "+answer.size());
            List<Object> final_ans = new ArrayList<>();
            for (int i = 0; i < answer.size(); i++) {
                final_ans.add(row[answer.get(i)]);
            }
            return final_ans;

        }
//        return Arrays.asList(row[0]);

    }

    private void find_index(RexNode condition,int is_right,List<Integer> answer){
        RexCall call = (RexCall) condition;
        SqlKind kind = call.getKind();
        List<RexNode> operands = call.getOperands();
        switch (kind){
            case EQUALS:
                if(operands.get(is_right) instanceof RexInputRef){
//                    System.out.println("temp1 "+operands.get(is_right));
                    RexInputRef ref = (RexInputRef) operands.get(is_right);
                    answer.add(ref.getIndex());
                    break;
                }
            case AND:
//                System.out.println("Size "+operands.size());
                for (int i = 0; i < operands.size(); i++) {
//                    System.out.println("temp2 "+operands.get(i));
                    find_index(operands.get(i),is_right,answer);
                }
                break;
            case OR:
                for (int i = 0; i < operands.size(); i++) {
                    find_index(operands.get(i),is_right,answer);
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation: " + kind);
        }
    }
}
