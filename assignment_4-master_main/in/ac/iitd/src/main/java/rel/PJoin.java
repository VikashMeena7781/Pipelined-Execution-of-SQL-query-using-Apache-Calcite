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

import convention.PConvention;

import java.util.*;

/*
 * Implements a hash join where:
 * - Right side (build side) is blocking: All rows are read into a hash table before the join begins.
 * - Left side (probe side) is streaming: Rows are read and joined as they come.
 * Supports multi-attribute join keys.
 */
public class PJoin extends Join implements PRel {


    private boolean has_left_rows = true;

    private int leftSize;

    private final Map<List<Object>, List<Object[]>> listMap = new HashMap<>();
    private Object[] current_row;
    private Iterator<Object[]> iterator;

    private boolean left_unmatched = false;

    private boolean is_right_join = false;

    private final LinkedHashMap<List<Object>, Boolean> mapReq = new LinkedHashMap<>();

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
        PRel left_copy = null;
        if (left instanceof PRel) {
            left_copy = (PRel) left;
            left_copy.open();
        }
        if (left_copy != null && left_copy.hasNext()) {
            current_row = left_copy.next();
            leftSize = current_row.length;
        } else {
            leftSize = 0;
        }
        left_copy.close();

        if (right instanceof PRel) {
            ((PRel) right).open();
            while (((PRel) right).hasNext()) {
                Object[] tmp = ((PRel) right).next();
                List<Integer> answer =new ArrayList<>();
                get_index(condition,1,answer);
                List<Object> key = new ArrayList<>();
                for (Integer integer : answer) {
                    key.add(tmp[integer - leftSize]);
                }
                List<Object> key1 = Arrays.asList(tmp);

                mapReq.compute(key1,(k, v) -> v == null ? false : v);

                listMap.computeIfAbsent(key, k -> new ArrayList<>()).add(tmp);
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
        if (iterator != null && iterator.hasNext()) {
            return true;
        }
        while (has_left_rows && left instanceof PRel && ((PRel) left).hasNext()) {
            current_row = ((PRel) left).next();
            List<Integer> answer = new ArrayList<>();
            get_index(condition,0,answer);
            List<Object> probeKey = new ArrayList<>();
            for (Integer integer : answer) {
                probeKey.add(current_row[integer]);
            }

            List<Object[]> tmp = listMap.get(probeKey);
            if (tmp != null && !tmp.isEmpty()) {
                iterator = tmp.iterator();
                iterator = tmp.iterator();
                for(Object[] obj : tmp){
                    List<Object> req_key = Arrays.asList(obj);
                    if (mapReq.get(req_key)!= null) {
                        mapReq.remove(req_key);
                    }
                }
                return true;
            }else{
                if (joinType.equals(JoinRelType.LEFT) || joinType.equals((JoinRelType.FULL))){
                    List<Object[]> left_match = new ArrayList<>();
                    left_match.add(current_row);
                    iterator = left_match.iterator();
                    left_unmatched = true;
                    return true;
                }
            }
        }
        has_left_rows = false;
        if (joinType.equals(JoinRelType.RIGHT) || joinType.equals(JoinRelType.FULL)){
            if (mapReq.isEmpty()){return false;}
            List<Object[]> rightMatch = new ArrayList<>();
            for (Map.Entry<List<Object>, Boolean> entry : mapReq.entrySet()) {
                List<Object> list = entry.getKey();
                Object[] array = list.toArray(new Object[0]);
                rightMatch.add(array);
            }
            iterator = rightMatch.iterator();
            mapReq.clear();
            is_right_join = true;
            return true;
        }
        return false;
    }

    @Override
    public Object[] next() {
        if (iterator == null || !iterator.hasNext()) {
            throw new NoSuchElementException("No more rows available in PJoin.");
        }
        Object[] buildRow = iterator.next();
        Object[] result = new Object[current_row.length + buildRow.length];

        System.arraycopy(current_row, 0, result, 0, current_row.length);
        if (!left_unmatched){
            System.arraycopy(buildRow, 0, result, current_row.length, buildRow.length);}
        else{
            Arrays.fill(result, current_row.length, result.length, null);
            left_unmatched = false;
        }
        if (is_right_join){
            Object[] final2 = new Object[current_row.length + buildRow.length];
            Arrays.fill(final2, 0, current_row.length, null);
            System.arraycopy(buildRow, 0, final2, current_row.length, buildRow.length);
            return final2;
        }
        return result;

    }

    @Override
    public void close() {
        if (left instanceof PRel) {
            ((PRel) left).close();
        }
        listMap.clear();
    }


    private void get_index(RexNode condition, int right, List<Integer> answer){
        RexCall call_value = (RexCall) condition;
        SqlKind call_valueKind = call_value.getKind();
        List<RexNode> operands = call_value.getOperands();
        if(call_valueKind==SqlKind.EQUALS){
            if(operands.get(right) instanceof RexInputRef){
                RexInputRef ref = (RexInputRef) operands.get(right);
                answer.add(ref.getIndex());
            }
        }else if(call_valueKind==SqlKind.AND){
            for (RexNode rexNode : operands) {
                get_index(rexNode, right, answer);
            }
        }else if(call_valueKind==SqlKind.OR){
            for (RexNode operand : operands) {
                get_index(operand, right, answer);
            }
        }else{
            throw new UnsupportedOperationException("Unsupported operation: " + call_valueKind);
        }

    }
}
