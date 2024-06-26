import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.util.Sources;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.log4j.Logger;

import manager.StorageManager;
import executor.QueryExecutor;
import convention.PConvention;
import org.checkerframework.checker.nullness.qual.Nullable;
import rel.PProject;

import java.util.Properties;
import java.util.Collections;
import java.util.HashMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.util.List;
import java.util.ArrayList;

public class MyCalciteConnection {

    private String jsonPath(String model) {
        return resourcePath(model + ".json");
    }

    private String resourcePath(String path) {
        // print the absolute path of this class
        return Sources.of(MyCalciteConnection.class.getResource("/" + path)).file().getAbsolutePath();
    }

    private Connection connection;
    private SqlValidator validator;
    private SqlToRelConverter converter;
    private VolcanoPlanner planner;
    private StorageManager storage_manager;
    private QueryExecutor query_executor;

    private static final Logger logger = Logger.getLogger(MyCalciteConnection.class);
    
    public MyCalciteConnection() throws Exception {

        storage_manager = StorageManager.getInstance();
        query_executor = new QueryExecutor();
        Properties info = new Properties();
        info.put("model", jsonPath("model"));
        info.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.FALSE.toString());

        try {
            connection = DriverManager.getConnection("jdbc:calcite:", info);
        } catch (Exception e) {
            logger.error("Error in creating connection", e);
            logger.error("Cause: ", e.getCause());
            throw e;
        }

        CalciteConnection calciteConnection = (CalciteConnection) connection;
        Schema schema = calciteConnection.getRootSchema();

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs = metaData.getSchemas(null, null);

        rs.next();
        schema = schema.getSubSchema(rs.getString("TABLE_SCHEM"));

        CalciteSchema calciteSchema = CalciteSchema.from((SchemaPlus) schema);
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(info);
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        ResultSet columns = metaData.getColumns(null, null, null, null);

        // hashmap from string table name to list of RelDataType
        HashMap<String, List<RelDataType>> tableSchema = new HashMap<>();

        while (columns.next()) {
            if (columns.getString("TABLE_NAME").equals("COLUMNS") || columns.getString("TABLE_NAME").equals("TABLES")) {
                continue;
            }
            String type = columns.getString("TYPE_NAME");
            if (type.endsWith(" NOT NULL")) {
                type = type.substring(0, type.length() - 9);
            }
            if (!tableSchema.containsKey(columns.getString("TABLE_NAME"))) {
                tableSchema.put(columns.getString("TABLE_NAME"), new ArrayList<>());
            }
            tableSchema.get(columns.getString("TABLE_NAME")).add((new RelDataTypeFieldImpl(
                columns.getString("COLUMN_NAME"),
                0,
                typeFactory.createSqlType(SqlTypeName.get(type))
            )).getType());
        }

        // load the tables
        for (String table : tableSchema.keySet()) {
            List<RelDataType> fields = tableSchema.get(table);
            storage_manager.loadFile(table + ".csv", fields);
        }

        logger.trace("Done loading files");

        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
            calciteSchema,
            Collections.singletonList(rs.getString("TABLE_SCHEM")),
            typeFactory,
            config
        );

        SqlOperatorTable operatorTable = new ChainedSqlOperatorTable(ImmutableList.of(SqlStdOperatorTable.instance()));

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withSqlConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);

        validator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory, validatorConfig);

        planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);

        converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );
    }

    public void close() throws Exception{
        connection.close();
    }

    public SqlNode parseSql(String sql) throws Exception{
        SqlParser parser = SqlParser.create(sql);
        return parser.parseStmt();
    }

    public SqlNode validateSql(SqlNode sqlNode) throws Exception{
        return validator.validate(sqlNode);
    }

    public RelNode convertSql(SqlNode sqlNode) throws Exception{
        return converter.convertQuery(sqlNode, false, true).rel;
    }

    public RelNode logicalToPhysical(RelNode node, RelTraitSet requiredTraitSet, RuleSet rules) {
        Program program = Programs.of(RuleSets.ofList(rules));
        return program.run(
                planner,
                node,
                requiredTraitSet,
                Collections.emptyList(),
                Collections.emptyList()
        );
    }

    public List<Object[]> executeQuery(RelNode relNode) {
        return query_executor.execute(relNode);
    }

    // Bonus Part for A4
    public List<Object[]> executeQueryBonus(String query, RuleSet rules) {
        try {
            SqlNode sqlNode = parseSql(query);
            SqlNode validatedSqlNode = validateSql(sqlNode);
            RelNode logicalPlan = convertSql(validatedSqlNode);
            RelNode physicalPlan = logicalToPhysical(logicalPlan, logicalPlan.getTraitSet().plus(PConvention.INSTANCE), rules);
            List<Object[]> result = executeQuery(physicalPlan);

            /*
                Write your code here
                You can post-process the result here, if needed
            */
//            System.out.println("sql node "+sqlNode.toString());
//            System.out.println("logical plan "+logicalPlan.toString());
//            System.out.println("logical plan "+sqlNode);

//            System.out.println("Type "+validatedSqlNode.getClass());

            List<String> projectedFieldNames = new ArrayList<>();
            if (validatedSqlNode instanceof SqlSelect) {
                SqlSelect selectNode = (SqlSelect) validatedSqlNode;
                SqlNodeList selectList = selectNode.getSelectList();
                for (SqlNode node : selectList) {
                    // Check if the node is a basic call
                    if (node instanceof SqlBasicCall) {
                        SqlBasicCall basicCall = (SqlBasicCall) node;
                        String fieldName = basicCall.getOperandList().get(0).toString();
                        projectedFieldNames.add(fieldName);
                        System.out.println(fieldName);

                    } else if (node instanceof SqlIdentifier) {
                        SqlIdentifier identifier = (SqlIdentifier) node;
                        String fieldName = identifier.names.get(identifier.names.size() - 1);
                        projectedFieldNames.add(fieldName);
                        System.out.println(fieldName);
                    } else {
                        System.out.println("Non-column reference found: " + node.toString());
                    }
                }
            }
            StorageManager db;



            return result;

        }
        catch (Exception e) {
            logger.error("Error in executing query", e);
            logger.error("Cause: ", e.getCause());
            return null;
        }
    }
}
//    public List<Object[]> executeQueryBonus(String query, RuleSet rules) {
//        try {
//            SqlNode sqlNode = parseSql(query);
//            SqlNode validatedSqlNode = validateSql(sqlNode);
//            RelNode logicalPlan = convertSql(validatedSqlNode);
//
//            // Retrieve the project fields from the logical plan if it's a project type
//            List<Integer> projectFieldIndices = null;
//            System.out.println("Hii1 "+logicalPlan.getClass());
//            if (logicalPlan instanceof Project) {
//                System.out.println("Hii2");
//                Project project = (Project) logicalPlan;
//                projectFieldIndices = project.getProjects().stream()
//                        .filter(expr -> expr instanceof RexInputRef)
//                        .map(expr -> ((RexInputRef) expr).getIndex())
//                        .collect(Collectors.toList());
//            }
//
//            if (projectFieldIndices == null) {
//                throw new RuntimeException("No projection found or projection fields are not direct references");
//            }
//
//            RelNode physicalPlan = logicalToPhysical(logicalPlan, logicalPlan.getTraitSet().plus(PConvention.INSTANCE), rules);
//            List<Object[]> result = executeQuery(physicalPlan);
//
//            System.out.println("Size "+result.size());
//
//            // Post-process the result to include only the projected fields
//            List<Object[]> filteredResult = new ArrayList<>();
//            for (Object[] row : result) {
//                Object[] newRow = new Object[projectFieldIndices.size()];
//                for (int i = 0; i < projectFieldIndices.size(); i++) {
//                    newRow[i] = row[projectFieldIndices.get(i)];
//                }
//                filteredResult.add(newRow);
//            }
//
//            return filteredResult;
//        }
//        catch (Exception e) {
//            logger.error("Error in executing query", e);
//            logger.error("Cause: ", e.getCause());
//            return null;
//        }
//    }


//    private Object evaluateExpression(RexNode expression, Object[] inputRow) {
//        if (expression instanceof RexInputRef) {
//            // Handle direct field references
//            RexInputRef ref = (RexInputRef) expression;
//            return inputRow[ref.getIndex()];
//        } else if (expression instanceof RexLiteral) {
//            // Handle literals
//            RexLiteral literal = (RexLiteral) expression;
//            return literal.getValue3(); // getValue3() is used to get the Java comparable object
//        } else if (expression instanceof RexCall) {
//            // Handle function calls (arithmetic operations)
//            RexCall call = (RexCall) expression;
//            Object result = null;
//            List<RexNode> operands = call.getOperands();
//            switch (call.getKind()) {
//                case PLUS:
//                    result = add(evaluateExpression(operands.get(0), inputRow),
//                            evaluateExpression(operands.get(1), inputRow));
//                    break;
//                case MINUS:
//                    result = subtract(evaluateExpression(operands.get(0), inputRow),
//                            evaluateExpression(operands.get(1), inputRow));
//                    break;
//                case TIMES:
//                    result = multiply(evaluateExpression(operands.get(0), inputRow),
//                            evaluateExpression(operands.get(1), inputRow));
//                    break;
//                case DIVIDE:
//                    result = divide(evaluateExpression(operands.get(0), inputRow),
//                            evaluateExpression(operands.get(1), inputRow));
//                    break;
//                default:
//                    throw new UnsupportedOperationException("Unsupported operation: " + call.getKind());
//            }
//            return result;
//        } else {
//            throw new IllegalArgumentException("Unsupported RexNode type: " + expression.getClass());
//        }
//    }
//
//    private Object add(Object a, Object b) {
//        if (a instanceof Number && b instanceof Number) {
//            return ((Number) a).doubleValue() + ((Number) b).doubleValue();
//        }
//        throw new IllegalArgumentException("Invalid arguments for add: " + a + ", " + b);
//    }
//
//    private Object subtract(Object a, Object b) {
//        if (a instanceof Number && b instanceof Number) {
//            return ((Number) a).doubleValue() - ((Number) b).doubleValue();
//        }
//        throw new IllegalArgumentException("Invalid arguments for subtract: " + a + ", " + b);
//    }
//
//    private Object multiply(Object a, Object b) {
//        if (a instanceof Number && b instanceof Number) {
//            return ((Number) a).doubleValue() * ((Number) b).doubleValue();
//        }
//        throw new IllegalArgumentException("Invalid arguments for multiply: " + a + ", " + b);
//    }
//
//    private Object divide(Object a, Object b) {
//        if (a instanceof Number && b instanceof Number) {
//            double divisor = ((Number) b).doubleValue();
//            if (divisor == 0) throw new ArithmeticException("Division by zero");
//            return ((Number) a).doubleValue() / divisor;
//        }
//        throw new IllegalArgumentException("Invalid arguments for divide: " + a + ", " + b);
//    }
//
//}