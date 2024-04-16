package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import rel.PRel;

import java.util.List;

// Hint: Think about alias and arithmetic operations
public class PProject extends Project implements PRel {

    RelNode input;
    public PProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        this.input=input;
        assert getConvention() instanceof PConvention;
    }

    @Override
    public PProject copy(RelTraitSet traitSet, RelNode input,
                               List<RexNode> projects, RelDataType rowType) {
        return new PProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public String toString() {
        return "PProject";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProject");
        if (input instanceof PRel) {
            PRel pInput = (PRel) input;
            return pInput.open();
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProject");
        if (input instanceof PRel) {
            PRel pInput = (PRel) input;
            pInput.close();
        }
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProject has next");
        if (input instanceof PRel) {
            PRel pInput = (PRel) input;
            return pInput.hasNext();
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProject");
        if (input instanceof PRel && ((PRel) input).hasNext()) {
            Object[] input1 = ((PRel) input).next();
            Object[] output = new Object[getProjects().size()];
            for (int i = 0; i < getProjects().size(); i++) {
                output[i] = get_row(getProjects().get(i), input1);
            }
            return output;
        }
        return null;
    }

    private Object get_row(RexNode expression, Object[] inputRow) {
        if (expression instanceof RexInputRef) {
            RexInputRef ref = (RexInputRef) expression;
            return inputRow[ref.getIndex()];
        } else if (expression instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) expression;
            return literal.getValue3();
        } else if (expression instanceof RexCall) {
            RexCall call = (RexCall) expression;
            Object result ;
            List<RexNode> operands = call.getOperands();
            Object a,b;
            switch (call.getKind()) {
                case PLUS:
                    a = get_row(operands.get(0), inputRow);
                    b = get_row(operands.get(1), inputRow);
                    if (a instanceof Number && b instanceof Number) {
                        if (a instanceof Double || b instanceof Double) {
                            result =  ((Number) a).doubleValue() + ((Number) b).doubleValue();
                        } else if (a instanceof Float || b instanceof Float) {
                            result = ((Number) a).floatValue() + ((Number) b).floatValue();
                        } else{
                            result = ((Number) a).intValue() + ((Number) b).intValue();
                        }
                    }else{
                        throw new IllegalArgumentException("Invalid arguments for add: " + a + ", " + b);
                    }
                    break;
                case MINUS:
                    a = get_row(operands.get(0), inputRow);
                    b = get_row(operands.get(1), inputRow);
                    if (a instanceof Number && b instanceof Number) {
                        if (a instanceof Double || b instanceof Double) {
                            result =  ((Number) a).doubleValue() - ((Number) b).doubleValue();
                        } else if (a instanceof Float || b instanceof Float) {
                            result = ((Number) a).floatValue() - ((Number) b).floatValue();
                        } else{
                            result = ((Number) a).intValue() - ((Number) b).intValue();
                        }
                    }else{
                        throw new IllegalArgumentException("Invalid arguments for sub: " + a + ", " + b);
                    }
                    break;
                case TIMES:
                    a = get_row(operands.get(0), inputRow);
                    b = get_row(operands.get(1), inputRow);
                    if (a instanceof Number && b instanceof Number) {
                        if (a instanceof Double || b instanceof Double) {
                            result =  ((Number) a).doubleValue() * ((Number) b).doubleValue();
                        } else if (a instanceof Float || b instanceof Float) {
                            result = ((Number) a).floatValue() * ((Number) b).floatValue();
                        } else{
                            result = ((Number) a).intValue() * ((Number) b).intValue();
                        }
                    }else{
                        throw new IllegalArgumentException("Invalid arguments for mul: " + a + ", " + b);
                    }
                    break;
                case DIVIDE:
                    a = get_row(operands.get(0), inputRow);
                    b = get_row(operands.get(1), inputRow);
                    if (a instanceof Number && b instanceof Number) {
                        double divisor = ((Number) b).doubleValue();
                        if (divisor == 0) throw new ArithmeticException("Division by zero");
                        if (a instanceof Double || b instanceof Double) {
                            result =  ((Number) a).doubleValue() / divisor;
                        } else if (a instanceof Float || b instanceof Float) {
                            result = ((Number) a).floatValue() / (float) divisor;
                        } else{
                            result = ((Number) a).intValue() / (int) divisor;
                        }
                    }else{
                        throw new IllegalArgumentException("Invalid arguments for add: " + a + ", " + b);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported operation: " + call.getKind());
            }
            return result;
        } else {
            throw new IllegalArgumentException("Unsupported RexNode type: " + expression.getClass());
        }
    }


}