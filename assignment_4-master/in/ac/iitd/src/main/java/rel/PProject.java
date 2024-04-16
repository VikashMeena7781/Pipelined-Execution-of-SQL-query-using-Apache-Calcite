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

import java.util.Arrays;
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
        /* Write your code here */
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
        /* Write your code here */
        if (input instanceof PRel) {
            PRel pInput = (PRel) input;
            pInput.close();
        }
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProject has next");
        /* Write your code here */
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
        /* Write your code here */
        if (input instanceof PRel && ((PRel) input).hasNext()) {
            Object[] inputRow = ((PRel) input).next();
//            System.out.println("Printing the object in Project: " + Arrays.toString(inputRow));
            Object[] outputRow = new Object[getProjects().size()];

            for (int i = 0; i < getProjects().size(); i++) {
                outputRow[i] = evaluateExpression(getProjects().get(i), inputRow);
            }
//            System.out.println("Printing the object after Projection: " + Arrays.toString(outputRow));
            return outputRow;
        }
        return null;
    }

    private Object evaluateExpression(RexNode expression, Object[] inputRow) {
        if (expression instanceof RexInputRef) {
            // Handle direct field references
            RexInputRef ref = (RexInputRef) expression;
            return inputRow[ref.getIndex()];
        } else if (expression instanceof RexLiteral) {
            // Handle literals
            RexLiteral literal = (RexLiteral) expression;
            return literal.getValue3(); // getValue3() is used to get the Java comparable object
        } else if (expression instanceof RexCall) {
            // Handle function calls (arithmetic operations)
            RexCall call = (RexCall) expression;
            Object result ;
            List<RexNode> operands = call.getOperands();
            switch (call.getKind()) {
                case PLUS:
                    result = add(evaluateExpression(operands.get(0), inputRow),
                            evaluateExpression(operands.get(1), inputRow));
                    break;
                case MINUS:
                    result = subtract(evaluateExpression(operands.get(0), inputRow),
                            evaluateExpression(operands.get(1), inputRow));
                    break;
                case TIMES:
                    result = multiply(evaluateExpression(operands.get(0), inputRow),
                            evaluateExpression(operands.get(1), inputRow));
                    break;
                case DIVIDE:
                    result = divide(evaluateExpression(operands.get(0), inputRow),
                            evaluateExpression(operands.get(1), inputRow));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported operation: " + call.getKind());
            }
            return result;
        } else {
            throw new IllegalArgumentException("Unsupported RexNode type: " + expression.getClass());
        }
    }

    private Object add(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            if (a instanceof Double || b instanceof Double) {
                return ((Number) a).doubleValue() + ((Number) b).doubleValue();
            } else if (a instanceof Float || b instanceof Float) {
                return ((Number) a).floatValue() + ((Number) b).floatValue();
            } else{
                return ((Number) a).intValue() + ((Number) b).intValue();
            }
        }
        throw new IllegalArgumentException("Invalid arguments for add: " + a + ", " + b);
    }

    private Object subtract(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            if (a instanceof Double || b instanceof Double) {
                return ((Number) a).doubleValue() - ((Number) b).doubleValue();
            } else if (a instanceof Float || b instanceof Float) {
                return ((Number) a).floatValue() - ((Number) b).floatValue();
            } else{
                return ((Number) a).intValue() - ((Number) b).intValue();
            }
        }
        throw new IllegalArgumentException("Invalid arguments for subtract: " + a + ", " + b);
    }

    private Object multiply(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            if (a instanceof Double || b instanceof Double) {
                return ((Number) a).doubleValue() * ((Number) b).doubleValue();
            } else if (a instanceof Float || b instanceof Float) {
                return ((Number) a).floatValue() * ((Number) b).floatValue();
            } else{
                return ((Number) a).intValue() * ((Number) b).intValue();
            }
        }
        throw new IllegalArgumentException("Invalid arguments for multiply: " + a + ", " + b);
    }

    private Object divide(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            double divisor = ((Number) b).doubleValue();
            if (divisor == 0) throw new ArithmeticException("Division by zero");
            if (a instanceof Double || b instanceof Double) {
                return ((Number) a).doubleValue() / divisor;
            } else if (a instanceof Float || b instanceof Float) {
                return ((Number) a).floatValue() / (float) divisor;
            } else{
                return ((Number) a).intValue() / (int) divisor;
            }
        }
        throw new IllegalArgumentException("Invalid arguments for divide: " + a + ", " + b);
    }

}
