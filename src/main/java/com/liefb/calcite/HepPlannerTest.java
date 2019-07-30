package com.liefb.calcite;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class HepPlannerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HepPlannerTest.class);

    public static void main(String[] args) {
        SchemaPlus rootSchema = CalciteUtils.registerRootSchema();

        final FrameworkConfig fromworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(rootSchema)
                .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
                .build();

        String sql = "select 10+20, u.id as user_id, u.name as user_name, j.company as user_company, u.age as user_age "
                + "from users u join jobs j on u.id = j.id "
                + "where u.age > 30+1 and j.id > 10 "
                + "order by user_id";

        // use HepPlanner
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN);
        builder.addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE);
        builder.addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE);
        builder.addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE);
        HepPlanner planner = new HepPlanner(builder.build());

        try {
            SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            // sql parser
            SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
            SqlNode parsed = parser.parseStmt();
            LOGGER.info("The SqlNode after parsed is:\n{}\n", parsed.toString());
            //System.out.println("The SqlNode after parsed is:\n" + parsed.toString() + "\n");

            // 通过CalciteCatalogReader获取table schema
            CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
                    CalciteSchema.from(rootSchema),
                    CalciteSchema.from(rootSchema).path(null),
                    factory,
                    new CalciteConnectionConfigImpl(new Properties()));

            // sql validate
            SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), calciteCatalogReader,
                    factory, CalciteUtils.conformance(fromworkConfig));
            SqlNode validated = validator.validate(parsed);
            LOGGER.info("The SqlNode after validated is:\n{}\n", validated.toString());
            //System.out.println("The SqlNode after validated is:\n" + validated.toString() + "\n");

            final RexBuilder rexBuilder = CalciteUtils.createRexBuilder(factory);
            final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

            // init SqlToRelConverter config
            final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
                    .withConfig(fromworkConfig.getSqlToRelConverterConfig())
                    .withTrimUnusedFields(false)
                    .withConvertTableAccess(false)
                    .build();
            // SqlNode toRelNode
            final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(new CalciteUtils.ViewExpanderImpl(),
                    validator, calciteCatalogReader, cluster, fromworkConfig.getConvertletTable(), config);
            RelRoot root = sqlToRelConverter.convertQuery(validated, false, true);

            root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
            final RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
            root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
            RelNode relNode = root.rel;
            LOGGER.info("The relational expression string before optimized is:\n{}\n", RelOptUtil.toString(relNode));
            //System.out.println("The relational expression string before optimized is:\n" + RelOptUtil.toString(relNode) + "\n");

            // 将 RelNode 树转换成 HepPlanner 内部使用的 Graph
            planner.setRoot(relNode);
            // 通过 findBestExp() 找到最优的 plan，规则的匹配都是在这里进行
            relNode = planner.findBestExp();
            System.out.println("The Best relational expression string:\n" + RelOptUtil.toString(relNode));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
