package com.bocom.rdss.spark.sdp3x.dolphinscheduler;

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedWith;
import org.apache.spark.sql.execution.SparkSqlParser;
import scala.Tuple2;
import scala.collection.Iterator;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * 使用 Spark 未解析语法树提取 SQL 输入表，识别 CTE 和子查询作用域。
 * 只分析语法，不创建 SparkContext、不访问 catalog、不执行 SQL。
 */
final class SqlLineage {
  /** @param sql 当前作业中的单条 SQL；返回用于工作流依赖匹配的真实输入表名集合 */
  Set<String> inputs(String sql) {
    try {
      Set<String> result = new LinkedHashSet<>();
      visit(new SparkSqlParser().parsePlan(sql), Collections.emptySet(), result);
      return result;
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot parse Spark SQL for dependency analysis: " + e.getMessage(), e);
    }
  }

  private void visit(LogicalPlan plan, Set<String> ctes, Set<String> result) {
    if (plan instanceof UnresolvedWith) {
      UnresolvedWith with = (UnresolvedWith) plan;
      Set<String> available = new LinkedHashSet<>(ctes);
      Iterator<Tuple2<String, SubqueryAlias>> iterator = with.cteRelations().iterator();
      while (iterator.hasNext()) {
        Tuple2<String, SubqueryAlias> cte = iterator.next();
        // Nonrecursive CTEs can refer to prior CTEs, but not themselves or later siblings.
        visit(cte._2().child(), available, result);
        available.add(cte._1().toLowerCase(Locale.ROOT));
      }
      visit(with.child(), available, result);
      return;
    }
    if (plan instanceof InsertIntoStatement) {
      visit(((InsertIntoStatement) plan).query(), ctes, result);
      return;
    }
    if (plan instanceof UnresolvedRelation) {
      UnresolvedRelation relation = (UnresolvedRelation) plan;
      String name = String.join(".", scala.collection.JavaConverters.seqAsJavaList(relation.multipartIdentifier()))
        .toLowerCase(Locale.ROOT);
      if (relation.multipartIdentifier().size() != 1 || !ctes.contains(name)) {
        result.add(name);
      }
    }
    Iterator<LogicalPlan> children = plan.children().iterator();
    while (children.hasNext()) {
      visit(children.next(), ctes, result);
    }
    Iterator<Expression> expressions = plan.expressions().iterator();
    while (expressions.hasNext()) {
      expression(expressions.next(), ctes, result);
    }
  }

  private void expression(Expression expression, Set<String> ctes, Set<String> result) {
    if (expression instanceof SubqueryExpression) {
      visit(((SubqueryExpression) expression).plan(), ctes, result);
    }
    Iterator<Expression> children = expression.children().iterator();
    while (children.hasNext()) {
      expression(children.next(), ctes, result);
    }
  }
}
