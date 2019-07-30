package com.liefb.calcite;

import java.util.Date;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

public class BeamSqlExplainer {
    public static final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    public static final SchemaPlus defaultSchema = Frameworks.createRootSchema(true);

    private FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(defaultSchema).build();
    private Planner planner = Frameworks.getPlanner(config);

    public BeamSqlExplainer(){
        addTableSchema();
    }

    public void addTableSchema(){
        defaultSchema.add("ORDER_DETAILS", new StreamableTable() {

            @Override
            public boolean isRolledUp(String column) {
                return false;
            }

            @Override
            public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
                return false;
            }

            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.FieldInfoBuilder b = typeFactory.builder();
                b.add("CK_TIME", typeFactory.createJavaType(Date.class));
                b.add("ITEM_ID", typeFactory.createJavaType(Long.class));
                b.add("ITEM_PRICE", typeFactory.createJavaType(Double.class));
                b.add("BUYER_NAME", typeFactory.createJavaType(String.class));
                b.add("QUANTITY", typeFactory.createJavaType(Integer.class));

                return b.build();
            }

            public Statistic getStatistic() {
//        return Statistics.of(100, ImmutableList.<ImmutableBitSet>of());
                Direction dir = Direction.ASCENDING;
                RelFieldCollation collation = new RelFieldCollation(0, dir, NullDirection.UNSPECIFIED);
                return Statistics.of(5, ImmutableList.of(ImmutableBitSet.of(0)),
                        ImmutableList.of(RelCollations.of(collation)));
            }

            public TableType getJdbcTableType() {
                return TableType.STREAM;
            }

            public Table stream() {
                return null;
            }

        });
        defaultSchema.add("LISTING_DETAILS", new ScannableTable() {

            @Override
            public boolean isRolledUp(String column) {
                return false;
            }

            @Override
            public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
                return false;
            }

            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.FieldInfoBuilder b = typeFactory.builder();
                b.add("LIST_ID", typeFactory.createJavaType(Long.class));
                b.add("ITEM_PRICE", typeFactory.createJavaType(Double.class));
                b.add("SELLER_NAME", typeFactory.createJavaType(String.class));
                b.add("AVAIL_QUANTITY", typeFactory.createJavaType(Integer.class));

                return b.build();
            }

            public Statistic getStatistic() {
                return Statistics.of(100, ImmutableList.<ImmutableBitSet>of());
            }

            public TableType getJdbcTableType() {
                return TableType.TABLE;
            }

            @Override
            public Enumerable scan(DataContext root) {
                return null;
            }

        });
    }

    public void explain(String sql) throws SqlParseException, ValidationException, RelConversionException{
        // parse and validate
        SqlNode parse = planner.parse(sql);
        SqlNode validate = planner.validate(parse);
        System.out.println("SQL Parser and Validator:");
        System.out.println(validate.toString());
        System.out.println();

        // convert to RelNode
        RelNode tree = planner.convert(validate);
        //Convert a relational expression to a String
        String plan = RelOptUtil.toString(tree);
        System.out.println("Logical Plan:");
        System.out.println(plan);
        System.out.println("------------------------------------------------------------------------------------");
        System.out.println("");

    }

    public static void main(String[] args) throws ValidationException, RelConversionException, SqlParseException {

        BeamSqlExplainer bse1 = new BeamSqlExplainer();
        BeamSqlExplainer bse2 = new BeamSqlExplainer();
        /*bse.explain("SELECT STREAM ITEM_ID, ITEM_PRICE, BUYER_NAME " +
                        "FROM ORDER_DETAILS " +
                        "WHERE ITEM_PRICE > 0 AND QUANTITY > 0");*/

        bse1.explain("SELECT STREAM A.ITEM_ID, A.ITEM_PRICE, A.BUYER_NAME, B.SELLER_NAME " +
                "FROM ORDER_DETAILS AS A " +
                "INNER JOIN LISTING_DETAILS AS B ON A.ITEM_ID = B.LIST_ID AND A.ITEM_PRICE > 0 AND B.AVAIL_QUANTITY > 0");

        bse2.explain("SELECT STREAM A.ITEM_ID, A.ITEM_PRICE, A.BUYER_NAME, B.SELLER_NAME " +
                "FROM ORDER_DETAILS AS A " +
                "INNER JOIN LISTING_DETAILS AS B ON A.ITEM_ID = B.LIST_ID " +
                "WHERE A.ITEM_PRICE > 0 AND B.AVAIL_QUANTITY > 0");
    }


}
