package com.missfresh.util;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

import static com.alibaba.druid.sql.SQLUtils.*;

/**
 * @author wangzhihua
 * @date 2019-06-21 14:41
 */
public class SqlParseDruid {
    public static  void enhanceSql(String sql) {
        /**
         * SQLUtils的parseStatements方法会把你传入的SQL语句给解析成SQLStatement对象集合，
         * 每一个SQLStatement代表一条完整的SQL语句,一般我们值处理单条
         */
        List<SQLStatement> statements = parseStatements(sql, JdbcConstants.MYSQL);
        // 只考虑一条语句
        SQLStatement statement = statements.get(0);
        // 只考虑查询语句
        /**
         * 查询：SQLSelectStatement
         * 更新：SQLUpdateStatement
         * 删除：SQLDeleteStatement
         * 插入: SQLInsertStatement
         */
        SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) statement;
        SQLSelectQuery sqlSelectQuery   = sqlSelectStatement.getSelect().getQuery();
        // 非union的查询语句
        if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
            // 获取字段列表
            List<SQLSelectItem> selectItems = sqlSelectQueryBlock.getSelectList();
            selectItems.forEach(x -> {
                System.out.println("字段值:"+x);
            });
            // 获取表
            SQLTableSource table = sqlSelectQueryBlock.getFrom();
            // 普通单表
            if (table instanceof SQLExprTableSource) {
                System.out.println("普通表"+table);
                // join多表
            } else if (table instanceof SQLJoinTableSource) {
                System.out.println("join表"+table);
                // 子查询作为表
            } else if (table instanceof SQLSubqueryTableSource) {
                System.out.println("子查询表"+table);
            }
            // 获取where条件
            SQLExpr where = sqlSelectQueryBlock.getWhere();
            // 如果是二元表达式
            if (where instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr   sqlBinaryOpExpr = (SQLBinaryOpExpr) where;
                SQLExpr           left            = sqlBinaryOpExpr.getLeft();
                SQLBinaryOperator operator        = sqlBinaryOpExpr.getOperator();
                SQLExpr           right           = sqlBinaryOpExpr.getRight();
                // 处理---------------------
                // 如果是子查询
            } else if (where instanceof SQLInSubQueryExpr) {
                SQLInSubQueryExpr sqlInSubQueryExpr = (SQLInSubQueryExpr) where;
                // 处理---------------------
            }
            // 获取分组
            SQLSelectGroupByClause groupBy = sqlSelectQueryBlock.getGroupBy();
            // 处理---------------------
            // 获取排序
            SQLOrderBy orderBy = sqlSelectQueryBlock.getOrderBy();
            System.out.println("orderBy的字段"+orderBy);
            // 获取分页
            SQLLimit limit = sqlSelectQueryBlock.getLimit();
            System.out.println("limit值："+limit);
            // union的查询语句
        } else if (sqlSelectQuery instanceof SQLUnionQuery) {
            // 处理---------------------
        }
    }

    public static void main(String[] args) {
        String sqlinfo="select \n" +
                "     trace_no,\n" +
                "     sku as product_code,\n" +
                "     sum(amount) as vip_subsidy_fee\n" +
                "from bi_odi_mryx.recent_as_promotion_record_order_promotion_discount \n" +
                "where create_time>='${lasthourday}'  and discount_type='10'  and event_class<>'DELETE' --取近两天的数据\n" +
                "group by trace_no,sku";
        enhanceSql(sqlinfo);
    }
}
