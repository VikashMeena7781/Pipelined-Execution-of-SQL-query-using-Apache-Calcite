package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.*;
import convention.PConvention;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class PFilter extends Filter implements PRel {

//    private final RelNode input;
    private Object[] currentRow;

    private List<Object[]> data;
    int counter;

//    private boolean temp=false;

    public PFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RexNode condition) {
        super(cluster, traits, child, condition);
//        this.input=child.getInputs();
//        this.input=getInput();
//        if (child instanceof PRel) {
//            this.inputRel = (PRel) child;
//        } else {
//            throw new IllegalArgumentException("Child must be of type PRel");
//        }

        System.out.println("condition "+condition.toString());
//        System.out.println("cluster "+ cluster.toString());
//        System.out.println("traits "+traits.toString());
//        System.out.println("child "+child.toString());

//        this.input=child;
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public String toString() {
        return "PFilter";
    }

    @Override
    public boolean open() {
        logger.trace("Opening PFilter");
//        if (input instanceof PRel) {
//            PRel pInput = (PRel) input;
//            return pInput.open();
//        }
//        return false;
//        System.out.println("Storing input1");
        if(input instanceof PRel){
            ((PRel) input).open();
//            System.out.println("Storing input2");
            data=new ArrayList<>();
            counter=0;
            while (((PRel) input).hasNext()) {
                data.add(((PRel) input).next());
            }
//            System.out.println("size "+counter);
            ((PRel)input).close();
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        logger.trace("Closing PFilter");
//        if (input instanceof PRel) {
//            PRel pInput = (PRel) input;
//            pInput.close();
//        }
        data.clear();
    }

    @Override
    public boolean hasNext() {
        logger.trace("Checking if PFilter has next");
//        if(input instanceof PRel){
//            while (((PRel)input).hasNext()) {
//                Object[] row = ((PRel) input).next();
//                if (evaluateCondition(condition,row)) {
//                    System.out.println("Printing the Filtered obj " + Arrays.toString(row));
//                    currentRow = row;
//                    return true;
//                }
//            }
//        }
        int temp=counter;
        while(temp<data.size()){
            Object[] row = data.get(temp);
            temp++;
            if(evaluateCondition(condition,row)){
//                currentRow=row;
                return true;
            }

        }
        return false;
    }

    @Override
    public Object[] next() {
        logger.trace("Getting next row from PFilter");
//        if (currentRow!=null) {
//            Object[] row = currentRow;
//            currentRow = null;
////            System.out.println("Printing the Filtered obj " + Arrays.toString(row));
//            return row;
//        }
        if(hasNext()) {
            while (counter < data.size()) {
                Object[] row = data.get(counter);
                counter++;
                if (evaluateCondition(condition, row)) {
//                    currentRow=row;
                    return row;
                }
            }
        }
        return null;
    }



    private boolean evaluateCondition(RexNode condition, Object[] row) {
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            SqlKind kind = call.getKind();
            List<RexNode> operands = call.getOperands();
            switch (kind) {
                case EQUALS:
                    return evaluateEquals(operands,row);
                case GREATER_THAN:
                    return evaluateGreaterThan(operands, row);
                case GREATER_THAN_OR_EQUAL:
                    return evaluateGreaterThanEqual(operands, row);
                case LESS_THAN:
                    return evaluateLessThan(operands, row);
                case LESS_THAN_OR_EQUAL:
                    return evaluateLessThanEquals(operands, row);
                case AND:
                    int operand_size=operands.size();
                    boolean temp = true;
                    for (int i = 0; i < operand_size; i++) {
                        Comparable value = evaluateCondition(operands.get(i),row);
                        temp = (temp && (boolean)value);
                    }
                    return temp;
                case OR:
                    boolean temp1 = false;
                    for (int i = 0; i < operands.size(); i++) {
                        Comparable value1 = evaluateCondition(operands.get(i),row);
                        temp1 = (temp1 || (boolean)value1);
                    }
                    return temp1;

                default:
                    throw new UnsupportedOperationException("Unsupported operation: " + kind);
            }
        }
        return false;
    }

    private boolean evaluateLessThanEquals(List<RexNode> operands, Object[] row) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException("GREATER_THAN requires two operands");
        }
        Comparable value1 = evaluateExpression(operands.get(0), row);
        Comparable value2 = evaluateExpression(operands.get(1), row);
        if(value1 instanceof BigDecimal){
            value1 = convertBigDecimal(value1);
        }
        if(value2 instanceof BigDecimal){
            value2=convertBigDecimal(value2);
        }
        return value1.compareTo(value2)<=0;
    }

    private boolean evaluateGreaterThanEqual(List<RexNode> operands, Object[] row) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException("GREATER_THAN requires two operands");
        }
        Comparable value1 = evaluateExpression(operands.get(0), row);
        Comparable value2 = evaluateExpression(operands.get(1), row);
        if(value1 instanceof BigDecimal){
            value1 = convertBigDecimal(value1);
        }
        if(value2 instanceof BigDecimal){
            value2=convertBigDecimal(value2);
        }
        return value1.compareTo(value2)>=0;

    }

    private boolean evaluateEquals(List<RexNode> operands, Object[] row) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException("GREATER_THAN requires two operands");
        }
        Comparable value1 = evaluateExpression(operands.get(0), row);
        Comparable value2 = evaluateExpression(operands.get(1), row);
        if(value1 instanceof BigDecimal){
            value1 = convertBigDecimal(value1);
        }
        if(value2 instanceof BigDecimal){
            value2=convertBigDecimal(value2);
        }
        return value1.compareTo(value2)==0;
    }


    private boolean evaluateGreaterThan(List<RexNode> operands, Object[] row) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException("GREATER_THAN requires two operands");
        }
        Comparable value1 = evaluateExpression(operands.get(0), row);
        Comparable value2 = evaluateExpression(operands.get(1), row);
        if(value1 instanceof BigDecimal){
            value1 = convertBigDecimal(value1);
        }
        if(value2 instanceof BigDecimal){
            value2=convertBigDecimal(value2);
        }
        return value1.compareTo(value2)>0;


    }

    private boolean evaluateLessThan(List<RexNode> operands, Object[] row) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException("LESS_THAN requires two operands");
        }
        Comparable value1 = evaluateExpression(operands.get(0), row);
        Comparable value2 = evaluateExpression(operands.get(1), row);
        if(value1 instanceof BigDecimal){
            value1 = convertBigDecimal(value1);
        }
        if(value2 instanceof BigDecimal){
            value2=convertBigDecimal(value2);
        }
        return value1.compareTo(value2)<0;
    }




    private Comparable evaluateExpression(RexNode node, Object[] row) {
        if (node instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) node;
            Object value = literal.getValue();
//            System.out.println("value type "+value.getClass());
//            System.out.println("value "+value);
            if (value instanceof BigDecimal) {
                return (BigDecimal) value;
            } else if (value instanceof NlsString) {
                String temp = ((NlsString) value).getValue();
                return temp;
            } else if(value instanceof Boolean) {
                return (Boolean)value;
            }else{
                throw new IllegalArgumentException("Unsupported literal type");
            }
        } else if (node instanceof RexInputRef) {
            RexInputRef ref = (RexInputRef) node;
//            System.out.println("value type1 "+row[ref.getIndex()].getClass());
//            System.out.println("value1 "+row[ref.getIndex()]);
            return (Comparable) row[ref.getIndex()];
        } else if (node instanceof RexCall) {
            RexCall call = (RexCall) node;
            if (call.getKind() == SqlKind.PLUS || call.getKind() == SqlKind.MINUS ||
                    call.getKind() == SqlKind.TIMES || call.getKind() == SqlKind.DIVIDE) {
                Comparable left = evaluateExpression(call.getOperands().get(0), row);
                Comparable right = evaluateExpression(call.getOperands().get(1), row);

                if (left instanceof BigDecimal) {
                    left = convertBigDecimal(left);
                }

                if (right instanceof BigDecimal) {
                    right = convertBigDecimal(right);
                }
                // Handle arithmetic operations based on the types of left and right
                if (left instanceof Double && right instanceof Double) {
                    if (call.getKind() == SqlKind.PLUS) {
                        return (Double) left + (Double) right;
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return (Double) left - (Double) right;
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return (Double) left * (Double) right;
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return (Double) left / (Double) right;
                    }
                } else if (left instanceof Float && right instanceof Float) {
                    if (call.getKind() == SqlKind.PLUS) {
                        return (Float) left + (Float) right;
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return (Float) left - (Float) right;
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return (Float) left * (Float) right;
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return (Float) left / (Float) right;
                    }
                } else if (left instanceof Integer && right instanceof Integer) {
                    if (call.getKind() == SqlKind.PLUS) {
                        return (Integer) left + (Integer) right;
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return (Integer) left - (Integer) right;
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return (Integer) left * (Integer) right;
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return (Integer) left / (Integer) right;
                    }
                } else if (left instanceof Double && right instanceof Integer) {
                    if (call.getKind() == SqlKind.PLUS) {
                        return (Double) left + ((Integer) right).doubleValue();
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return (Double) left - ((Integer) right).doubleValue();
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return (Double) left * ((Integer) right).doubleValue();
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return (Double) left / ((Integer) right).doubleValue();
                    }
                } else if (left instanceof Integer && right instanceof Double) {
                    // Convert left to Double
                    if (call.getKind() == SqlKind.PLUS) {
                        return ((Integer) left).doubleValue() +  (Double) right;
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return ((Integer) left).doubleValue()- (Double) right;
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return ((Integer) left).doubleValue() * (Double) right;
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return ((Integer) left).doubleValue() / (Double) right;
                    }
                } else if (left instanceof Float && right instanceof Integer) {
                    // Convert right to Float
                    if (call.getKind() == SqlKind.PLUS) {
                        return (Float) left + ((Integer) right).floatValue();
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return (Float) left - ((Integer) right).floatValue();
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return (Float) left * ((Integer) right).floatValue();
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return (Float) left / ((Integer) right).floatValue();
                    }
//                    return (Float) left + ((Integer) right).floatValue();
                } else if (left instanceof Integer && right instanceof Float) {
                    // Convert left to Float
                    if (call.getKind() == SqlKind.PLUS) {
                        return ((Integer) left).floatValue() + (float)right;
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return ((Integer) left).floatValue() - (float)right;
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return ((Integer) left).floatValue() * (float)right;
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return ((Integer) left).floatValue() / (float)right;
                    }
//                    return ((Integer) left).floatValue() + (Float) right;
                }else if (left instanceof Float && right instanceof Double) {
                    // Convert left to Double
                    if (call.getKind() == SqlKind.PLUS) {
                        return  ((Float) left).doubleValue() + (Double) right;
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return ((Float) left).doubleValue() - (Double) right;
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return ((Float) left).doubleValue() * (Double) right;
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return ((Float) left).doubleValue() / (Double) right;
                    }
//                    return ((Float) left).doubleValue() + (Double) right;
                } else if (left instanceof Double && right instanceof Float) {
                    // Convert right to Double
                    if (call.getKind() == SqlKind.PLUS) {
                        return (Double) left + ((Float) right).doubleValue();
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return (Double) left - ((Float) right).doubleValue();
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return (Double) left * ((Float) right).doubleValue();
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return (Double) left / ((Float) right).doubleValue();
                    }
                }
                else {
                    throw new IllegalArgumentException("Unsupported types for arithmetic operation");
                }

            }
        }
        throw new UnsupportedOperationException("Unsupported type of expression");
    }


    private Comparable convertBigDecimal(Comparable value) {
        BigDecimal bigDecimalValue = (BigDecimal) value;
        if (bigDecimalValue.scale() <= 0) {
            // No fractional part, treat as Integer
            return bigDecimalValue.intValue();
        } else if (bigDecimalValue.scale() == 1) {
            // One digit fractional part, treat as Float
            return bigDecimalValue.floatValue();
        } else {
            // More than one digit fractional part, treat as Double
            return bigDecimalValue.doubleValue();
        }

    }
}