"""
Market Basket Association Analysis Module
==========================================

A comprehensive, elegant, and efficient module for analyzing product associations
in market basket data. Supports analysis of itemsets of any size (pairs, triplets,
quadruplets, and beyond).

Features:
- Generic n-item association analysis
- FP-Growth algorithm integration with size filtering
- Efficient SQL query generation
- Product recommendations based on associations
- Comprehensive metrics calculation (support, confidence, lift)
- Department-level analysis
- Optimized for PySpark and Databricks

Author: Refactored for elegance and efficiency
Date: 2025-11-04
"""

from typing import Optional, List, Dict, Tuple, Union
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, collect_list, count, lit, size, array_intersect,
    expr, concat_ws, round as spark_round, desc, asc
)
from pyspark.ml.fpm import FPGrowth, FPGrowthModel


@dataclass
class AnalysisConfig:
    """Configuration for association analysis"""
    min_support: float = 0.001
    min_confidence: float = 0.1
    min_lift: float = 1.0
    min_co_occurrence: int = 50
    max_transactions: Optional[int] = 100000
    database_name: str = "workspace.instacart"

    def __post_init__(self):
        """Validate configuration parameters"""
        if not 0 < self.min_support < 1:
            raise ValueError("min_support must be between 0 and 1")
        if not 0 < self.min_confidence <= 1:
            raise ValueError("min_confidence must be between 0 and 1")
        if self.min_lift < 0:
            raise ValueError("min_lift must be non-negative")


class SQLGenerator:
    """Generates optimized SQL queries for itemset analysis"""

    def __init__(self, database: str):
        self.database = database

    def _get_table(self, table_name: str) -> str:
        """Get fully qualified table name"""
        return f"{self.database}.{table_name}"

    def generate_itemset_query(
        self,
        itemset_size: int,
        min_co_occurrence: int,
        limit: Optional[int] = None
    ) -> str:
        """
        Generate SQL query for finding n-item associations

        Args:
            itemset_size: Size of itemsets to analyze (2, 3, 4, etc.)
            min_co_occurrence: Minimum number of times items must appear together
            limit: Optional limit on number of results

        Returns:
            SQL query string
        """
        if itemset_size < 2:
            raise ValueError("itemset_size must be at least 2")

        # Generate table aliases and joins
        aliases = [f"op{i}" for i in range(1, itemset_size + 1)]

        # Build JOIN clauses with ordering constraint to avoid duplicates
        joins = [
            f"JOIN {self._get_table('order_products_prior')} {aliases[i]} "
            f"ON {aliases[0]}.order_id = {aliases[i]}.order_id "
            f"AND {aliases[i-1]}.product_id < {aliases[i]}.product_id"
            for i in range(1, itemset_size)
        ]
        join_clause = "\n        ".join(joins)

        # Build product_id selections
        product_ids = ", ".join([f"{alias}.product_id as product{i}_id"
                                 for i, alias in enumerate(aliases, 1)])

        # Build product name joins
        product_joins = "\n    ".join([
            f"JOIN {self._get_table('products')} p{i} "
            f"ON itemsets.product{i}_id = p{i}.product_id"
            for i in range(1, itemset_size + 1)
        ])

        # Build product name selections
        product_names = ", ".join([f"p{i}.product_name as product_{i}"
                                   for i in range(1, itemset_size + 1)])

        # Build support subqueries for each product
        support_ctes = ",\n    ".join([
            f"product{i}_support AS (\n"
            f"        SELECT product_id, COUNT(DISTINCT order_id) as support_count\n"
            f"        FROM {self._get_table('order_products_prior')}\n"
            f"        GROUP BY product_id\n"
            f"    )"
            for i in range(1, itemset_size + 1)
        ])

        # Build support joins
        support_joins = "\n    ".join([
            f"JOIN product{i}_support ps{i} ON itemsets.product{i}_id = ps{i}.product_id"
            for i in range(1, itemset_size + 1)
        ])

        # Build confidence calculations (itemset -> last item)
        confidence_calc = (
            f"ROUND(itemsets.co_occurrence * 1.0 / "
            f"LEAST({', '.join([f'ps{i}.support_count' for i in range(1, itemset_size)])}) , 4) as confidence"
        )

        # Build lift calculation
        support_product = " * ".join([f"ps{i}.support_count" for i in range(1, itemset_size + 1)])
        lift_calc = f"""ROUND(
            itemsets.co_occurrence * POWER(total.total_orders, {itemset_size - 1}) * 1.0 /
            ({support_product}), 4) as lift"""

        limit_clause = f"LIMIT {limit}" if limit else ""

        query = f"""
WITH itemsets AS (
    SELECT
        {product_ids},
        COUNT(DISTINCT {aliases[0]}.order_id) as co_occurrence
    FROM {self._get_table('order_products_prior')} {aliases[0]}
        {join_clause}
    GROUP BY {product_ids}
    HAVING co_occurrence >= {min_co_occurrence}
),
{support_ctes},
total_orders AS (
    SELECT COUNT(DISTINCT order_id) as total_orders
    FROM {self._get_table('order_products_prior')}
)
SELECT
    {product_names},
    itemsets.co_occurrence,
    ROUND(itemsets.co_occurrence * 1.0 / total.total_orders, 6) as support,
    {confidence_calc},
    {lift_calc}
FROM itemsets
    {product_joins}
    {support_joins}
    CROSS JOIN total_orders total
ORDER BY lift DESC, co_occurrence DESC
{limit_clause}
        """

        return query

    def generate_transactions_query(
        self,
        max_transactions: Optional[int] = None
    ) -> str:
        """Generate query for transaction data in FP-Growth format"""
        limit_clause = f"LIMIT {max_transactions}" if max_transactions else ""

        return f"""
SELECT
    o.order_id,
    COLLECT_LIST(CAST(op.product_id AS STRING)) as items
FROM {self._get_table('orders')} o
JOIN {self._get_table('order_products_prior')} op ON o.order_id = op.order_id
WHERE o.eval_set = 'prior'
GROUP BY o.order_id
HAVING COUNT(*) >= 2
{limit_clause}
        """

    def generate_product_search_query(
        self,
        product_pattern: str,
        itemset_size: int,
        min_co_occurrence: int,
        limit: int = 20
    ) -> str:
        """Generate query to find associations containing a specific product"""
        base_query = self.generate_itemset_query(itemset_size, min_co_occurrence, limit * 2)

        # Add WHERE clause to filter by product name
        where_conditions = " OR ".join([
            f"p{i}.product_name LIKE '%{product_pattern}%'"
            for i in range(1, itemset_size + 1)
        ])

        # Insert WHERE clause before ORDER BY
        query_parts = base_query.rsplit("ORDER BY", 1)
        filtered_query = f"{query_parts[0]}\nWHERE {where_conditions}\nORDER BY{query_parts[1]}"

        # Update limit
        return filtered_query.rsplit("LIMIT", 1)[0] + f"LIMIT {limit}"

    def generate_department_associations_query(
        self,
        itemset_size: int,
        min_co_occurrence: int = 1000
    ) -> str:
        """Generate query for department-level associations"""
        if itemset_size < 2:
            raise ValueError("itemset_size must be at least 2")

        aliases = [f"db{i}" for i in range(1, itemset_size + 1)]

        # Build joins with ordering
        joins = [
            f"JOIN dept_baskets {aliases[i]} ON {aliases[0]}.order_id = {aliases[i]}.order_id "
            f"AND {aliases[i-1]}.department < {aliases[i]}.department"
            for i in range(1, itemset_size)
        ]
        join_clause = "\n        ".join(joins)

        # Build department selections
        dept_selections = ", ".join([f"{alias}.department as dept_{i}"
                                     for i, alias in enumerate(aliases, 1)])

        query = f"""
WITH dept_baskets AS (
    SELECT
        o.order_id,
        d.department,
        COUNT(DISTINCT op.product_id) as items_from_dept
    FROM {self._get_table('orders')} o
    JOIN {self._get_table('order_products_prior')} op ON o.order_id = op.order_id
    JOIN {self._get_table('products')} p ON op.product_id = p.product_id
    JOIN {self._get_table('departments')} d ON p.department_id = d.department_id
    WHERE o.eval_set = 'prior'
    GROUP BY o.order_id, d.department
)
SELECT
    {dept_selections},
    COUNT(DISTINCT {aliases[0]}.order_id) as co_occurrence,
    ROUND(COUNT(DISTINCT {aliases[0]}.order_id) * 100.0 /
        (SELECT COUNT(DISTINCT order_id) FROM dept_baskets), 2) as support_pct
FROM dept_baskets {aliases[0]}
    {join_clause}
GROUP BY {dept_selections}
HAVING co_occurrence >= {min_co_occurrence}
ORDER BY co_occurrence DESC
LIMIT 50
        """

        return query


class ItemsetAnalyzer:
    """
    Main class for performing market basket association analysis
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[AnalysisConfig] = None
    ):
        """
        Initialize the analyzer

        Args:
            spark: SparkSession instance
            config: Optional configuration object
        """
        self.spark = spark
        self.config = config or AnalysisConfig()
        self.sql_gen = SQLGenerator(self.config.database_name)
        self._fpgrowth_model: Optional[FPGrowthModel] = None
        self._transactions_df: Optional[DataFrame] = None

    def prepare_transactions(self) -> DataFrame:
        """
        Prepare transaction data for FP-Growth analysis

        Returns:
            DataFrame with order_id and items columns
        """
        if self._transactions_df is None:
            query = self.sql_gen.generate_transactions_query(
                self.config.max_transactions
            )
            self._transactions_df = self.spark.sql(query)
            self._transactions_df.cache()

        return self._transactions_df

    def run_fpgrowth(
        self,
        transactions_df: Optional[DataFrame] = None
    ) -> Tuple[DataFrame, DataFrame, FPGrowthModel]:
        """
        Run FP-Growth algorithm to find frequent itemsets and rules

        Args:
            transactions_df: Optional pre-prepared transactions dataframe

        Returns:
            Tuple of (frequent_itemsets, association_rules, model)
        """
        if transactions_df is None:
            transactions_df = self.prepare_transactions()

        fpgrowth = FPGrowth(
            itemsCol="items",
            minSupport=self.config.min_support,
            minConfidence=self.config.min_confidence
        )

        self._fpgrowth_model = fpgrowth.fit(transactions_df)

        frequent_itemsets = self._fpgrowth_model.freqItemsets
        association_rules = self._fpgrowth_model.associationRules

        # Filter by lift
        association_rules = association_rules.filter(
            col("lift") >= self.config.min_lift
        )

        return frequent_itemsets, association_rules, self._fpgrowth_model

    def get_itemsets_by_size(
        self,
        itemset_size: int,
        frequent_itemsets: Optional[DataFrame] = None,
        limit: Optional[int] = None
    ) -> DataFrame:
        """
        Filter frequent itemsets by size

        Args:
            itemset_size: Size of itemsets to retrieve (2=pairs, 3=triplets, 4=quadruplets)
            frequent_itemsets: Optional pre-computed frequent itemsets
            limit: Optional limit on results

        Returns:
            DataFrame containing itemsets of the specified size
        """
        if frequent_itemsets is None:
            if self._fpgrowth_model is None:
                frequent_itemsets, _, _ = self.run_fpgrowth()
            else:
                frequent_itemsets = self._fpgrowth_model.freqItemsets

        result = frequent_itemsets.filter(size(col("items")) == itemset_size)
        result = result.orderBy(desc("freq"))

        if limit:
            result = result.limit(limit)

        return result

    def analyze_itemsets(
        self,
        itemset_size: int,
        limit: Optional[int] = 100
    ) -> DataFrame:
        """
        Analyze itemsets of specified size with detailed metrics

        Args:
            itemset_size: Size of itemsets (2=pairs, 3=triplets, 4=quadruplets)
            limit: Maximum number of results

        Returns:
            DataFrame with products, metrics (support, confidence, lift)
        """
        query = self.sql_gen.generate_itemset_query(
            itemset_size=itemset_size,
            min_co_occurrence=self.config.min_co_occurrence,
            limit=limit
        )

        return self.spark.sql(query)

    def find_product_associations(
        self,
        product_pattern: str,
        itemset_size: int = 2,
        limit: int = 20
    ) -> DataFrame:
        """
        Find associations containing a specific product

        Args:
            product_pattern: Product name pattern to search for
            itemset_size: Size of itemsets to analyze
            limit: Maximum number of results

        Returns:
            DataFrame of associations containing the product
        """
        query = self.sql_gen.generate_product_search_query(
            product_pattern=product_pattern,
            itemset_size=itemset_size,
            min_co_occurrence=self.config.min_co_occurrence,
            limit=limit
        )

        return self.spark.sql(query)

    def analyze_department_associations(
        self,
        itemset_size: int = 2,
        min_co_occurrence: int = 1000
    ) -> DataFrame:
        """
        Analyze associations at department level

        Args:
            itemset_size: Number of departments in association
            min_co_occurrence: Minimum co-occurrence count

        Returns:
            DataFrame of department associations
        """
        query = self.sql_gen.generate_department_associations_query(
            itemset_size=itemset_size,
            min_co_occurrence=min_co_occurrence
        )

        return self.spark.sql(query)

    def get_top_associations(
        self,
        itemset_size: int = 2,
        metric: str = "lift",
        limit: int = 30
    ) -> DataFrame:
        """
        Get top associations by a specific metric

        Args:
            itemset_size: Size of itemsets
            metric: Metric to sort by ('lift', 'support', 'confidence', 'co_occurrence')
            limit: Number of results

        Returns:
            DataFrame of top associations
        """
        if metric not in ['lift', 'support', 'confidence', 'co_occurrence']:
            raise ValueError(f"Invalid metric: {metric}")

        df = self.analyze_itemsets(itemset_size, limit * 2)
        return df.orderBy(desc(metric)).limit(limit)

    def create_recommendations(
        self,
        user_id: int,
        n_recommendations: int = 10,
        based_on_itemset_size: int = 2
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Create personalized product recommendations

        Args:
            user_id: User ID to create recommendations for
            n_recommendations: Number of recommendations
            based_on_itemset_size: Use 2-item, 3-item, or 4-item associations

        Returns:
            Tuple of (user_history, recommendations)
        """
        # Get user purchase history
        user_history = self.spark.sql(f"""
            SELECT DISTINCT
                p.product_id,
                p.product_name,
                COUNT(*) as purchase_count
            FROM {self.config.database_name}.orders o
            JOIN {self.config.database_name}.order_products_prior op
                ON o.order_id = op.order_id
            JOIN {self.config.database_name}.products p
                ON op.product_id = p.product_id
            WHERE o.user_id = {user_id}
            GROUP BY p.product_id, p.product_name
            ORDER BY purchase_count DESC
        """)

        # Get recommendations based on associations
        recommendations = self.spark.sql(f"""
            WITH user_products AS (
                SELECT DISTINCT op.product_id
                FROM {self.config.database_name}.orders o
                JOIN {self.config.database_name}.order_products_prior op
                    ON o.order_id = op.order_id
                WHERE o.user_id = {user_id}
            ),
            associated_products AS (
                SELECT
                    op2.product_id as recommended_product,
                    COUNT(DISTINCT op1.order_id) as association_strength
                FROM {self.config.database_name}.order_products_prior op1
                JOIN {self.config.database_name}.order_products_prior op2
                    ON op1.order_id = op2.order_id
                WHERE op1.product_id IN (SELECT product_id FROM user_products)
                  AND op2.product_id NOT IN (SELECT product_id FROM user_products)
                GROUP BY op2.product_id
            )
            SELECT
                p.product_name,
                p.product_id,
                a.aisle,
                d.department,
                ap.association_strength,
                RANK() OVER (ORDER BY ap.association_strength DESC) as rank
            FROM associated_products ap
            JOIN {self.config.database_name}.products p
                ON ap.recommended_product = p.product_id
            JOIN {self.config.database_name}.aisles a ON p.aisle_id = a.aisle_id
            JOIN {self.config.database_name}.departments d ON p.department_id = d.department_id
            ORDER BY association_strength DESC
            LIMIT {n_recommendations}
        """)

        return user_history, recommendations

    def calculate_metrics(self) -> DataFrame:
        """
        Calculate recommendation quality metrics using train set as ground truth

        Returns:
            DataFrame with precision, recall, and other metrics
        """
        return self.spark.sql(f"""
            WITH train_baskets AS (
                SELECT
                    o.user_id,
                    COLLECT_SET(op.product_id) as actual_products
                FROM {self.config.database_name}.orders o
                JOIN {self.config.database_name}.order_products_train op
                    ON o.order_id = op.order_id
                GROUP BY o.user_id
            ),
            prior_frequent AS (
                SELECT
                    o.user_id,
                    op.product_id,
                    COUNT(*) as purchase_frequency,
                    ROW_NUMBER() OVER (
                        PARTITION BY o.user_id
                        ORDER BY COUNT(*) DESC
                    ) as rank
                FROM {self.config.database_name}.orders o
                JOIN {self.config.database_name}.order_products_prior op
                    ON o.order_id = op.order_id
                GROUP BY o.user_id, op.product_id
            ),
            recommendations AS (
                SELECT
                    user_id,
                    COLLECT_LIST(product_id) as recommended_products
                FROM prior_frequent
                WHERE rank <= 10
                GROUP BY user_id
            )
            SELECT
                COUNT(*) as total_users,
                AVG(SIZE(array_intersect(t.actual_products, r.recommended_products)))
                    as avg_correct_predictions,
                AVG(SIZE(array_intersect(t.actual_products, r.recommended_products)) /
                    SIZE(r.recommended_products)) as avg_precision,
                AVG(SIZE(array_intersect(t.actual_products, r.recommended_products)) /
                    SIZE(t.actual_products)) as avg_recall
            FROM train_baskets t
            JOIN recommendations r ON t.user_id = r.user_id
        """)

    def export_results(
        self,
        dataframe: DataFrame,
        table_name: str,
        save_as_csv: bool = False,
        csv_path: Optional[str] = None
    ) -> None:
        """
        Export analysis results

        Args:
            dataframe: DataFrame to export
            table_name: Name for the Delta table
            save_as_csv: Whether to also save as CSV
            csv_path: Path for CSV file (required if save_as_csv=True)
        """
        full_table_name = f"{self.config.database_name}.{table_name}"

        # Save as Delta table
        dataframe.write.mode("overwrite").saveAsTable(full_table_name)
        print(f"✓ Saved to Delta table: {full_table_name}")

        # Optionally save as CSV
        if save_as_csv:
            if not csv_path:
                csv_path = f"/tmp/{table_name}.csv"

            dataframe.toPandas().to_csv(csv_path, index=False)
            print(f"✓ Saved to CSV: {csv_path}")

    def analyze_temporal_patterns(
        self,
        itemset_size: int = 2,
        min_support: int = 50
    ) -> DataFrame:
        """
        Analyze shopping patterns by time of day and day of week

        Args:
            itemset_size: Size of itemsets to analyze
            min_support: Minimum co-occurrence count

        Returns:
            DataFrame with temporal patterns
        """
        if itemset_size < 2:
            raise ValueError("itemset_size must be at least 2")

        # Generate aliases for joins
        aliases = [f"op{i}" for i in range(1, itemset_size + 1)]

        # Build joins
        joins = [
            f"JOIN {self.config.database_name}.order_products_prior {aliases[i]} "
            f"ON o.order_id = {aliases[i]}.order_id AND {aliases[i-1]}.product_id < {aliases[i]}.product_id"
            for i in range(1, itemset_size)
        ]
        join_clause = "\n        ".join(joins)

        # Build product selections
        product_ids = ", ".join([f"{alias}.product_id as prod{i}"
                                 for i, alias in enumerate(aliases, 1)])

        # Build product name joins
        product_joins = "\n    ".join([
            f"JOIN {self.config.database_name}.products p{i} ON tp.prod{i} = p{i}.product_id"
            for i in range(1, itemset_size + 1)
        ])

        # Build product names and departments
        product_names = ", ".join([f"p{i}.product_name as product_{i}"
                                   for i in range(1, itemset_size + 1)])
        dept_joins = "\n    ".join([
            f"LEFT JOIN {self.config.database_name}.departments d{i} "
            f"ON TRY_CAST(p{i}.department_id AS BIGINT) = d{i}.department_id"
            for i in range(1, itemset_size + 1)
        ])
        dept_names = ", ".join([f"d{i}.department as dept_{i}"
                                for i in range(1, itemset_size + 1)])

        query = f"""
WITH time_patterns AS (
    SELECT
        CASE
            WHEN o.order_hour_of_day BETWEEN 6 AND 11 THEN 'Morning (6-11am)'
            WHEN o.order_hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon (12-5pm)'
            WHEN o.order_hour_of_day BETWEEN 18 AND 21 THEN 'Evening (6-9pm)'
            ELSE 'Night (10pm-5am)'
        END as time_period,
        CASE
            WHEN o.order_dow IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type,
        {product_ids},
        COUNT(DISTINCT o.order_id) as pattern_count
    FROM {self.config.database_name}.orders o
    JOIN {self.config.database_name}.order_products_prior {aliases[0]} ON o.order_id = {aliases[0]}.order_id
        {join_clause}
    WHERE o.eval_set = 'prior'
    GROUP BY time_period, day_type, {product_ids}
    HAVING COUNT(DISTINCT o.order_id) >= {min_support}
)
SELECT
    tp.time_period,
    tp.day_type,
    {product_names},
    tp.pattern_count,
    {dept_names},
    CONCAT(tp.day_type, ' - ', tp.time_period) as shopping_context
FROM time_patterns tp
    {product_joins}
    {dept_joins}
ORDER BY tp.pattern_count DESC
LIMIT 200
        """

        return self.spark.sql(query)

    def analyze_sequential_patterns(
        self,
        min_support: int = 80
    ) -> DataFrame:
        """
        Analyze the order in which products are added to cart

        Args:
            min_support: Minimum sequence count

        Returns:
            DataFrame with sequential patterns
        """
        query = f"""
WITH ordered_products AS (
    SELECT
        order_id,
        product_id,
        add_to_cart_order,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY add_to_cart_order) as position
    FROM {self.config.database_name}.order_products_prior
),
sequences AS (
    SELECT
        op1.product_id as first_product,
        op2.product_id as second_product,
        COUNT(*) as sequence_count,
        AVG(op1.position) as avg_first_position,
        AVG(op2.position) as avg_second_position
    FROM ordered_products op1
    JOIN ordered_products op2
        ON op1.order_id = op2.order_id
        AND op2.position = op1.position + 1
    GROUP BY op1.product_id, op2.product_id
    HAVING COUNT(*) >= {min_support}
)
SELECT
    p1.product_name as first_product,
    p2.product_name as second_product,
    s.sequence_count,
    ROUND(s.avg_first_position, 1) as avg_first_pos,
    ROUND(s.avg_second_position, 1) as avg_second_pos,
    d1.department as first_dept,
    d2.department as second_dept,
    a1.aisle as first_aisle,
    a2.aisle as second_aisle
FROM sequences s
JOIN {self.config.database_name}.products p1 ON s.first_product = p1.product_id
JOIN {self.config.database_name}.products p2 ON s.second_product = p2.product_id
LEFT JOIN {self.config.database_name}.departments d1
    ON TRY_CAST(p1.department_id AS BIGINT) = d1.department_id
LEFT JOIN {self.config.database_name}.departments d2
    ON TRY_CAST(p2.department_id AS BIGINT) = d2.department_id
LEFT JOIN {self.config.database_name}.aisles a1
    ON TRY_CAST(p1.aisle_id AS BIGINT) = a1.aisle_id
LEFT JOIN {self.config.database_name}.aisles a2
    ON TRY_CAST(p2.aisle_id AS BIGINT) = a2.aisle_id
ORDER BY sequence_count DESC
LIMIT 100
        """

        return self.spark.sql(query)

    def analyze_basket_composition(self) -> DataFrame:
        """
        Analyze basket size and diversity metrics

        Returns:
            DataFrame with basket composition statistics
        """
        query = f"""
WITH basket_stats AS (
    SELECT
        o.order_id,
        COUNT(DISTINCT op.product_id) as total_items,
        COUNT(DISTINCT p.department_id) as unique_departments,
        COUNT(DISTINCT p.aisle_id) as unique_aisles
    FROM {self.config.database_name}.orders o
    JOIN {self.config.database_name}.order_products_prior op ON o.order_id = op.order_id
    JOIN {self.config.database_name}.products p ON op.product_id = p.product_id
    WHERE o.eval_set = 'prior'
      AND TRY_CAST(p.department_id AS BIGINT) IS NOT NULL
      AND TRY_CAST(p.aisle_id AS BIGINT) IS NOT NULL
    GROUP BY o.order_id
),
size_categories AS (
    SELECT
        CASE
            WHEN total_items <= 5 THEN 'Small (1-5 items)'
            WHEN total_items <= 15 THEN 'Medium (6-15 items)'
            WHEN total_items <= 30 THEN 'Large (16-30 items)'
            ELSE 'XLarge (30+ items)'
        END as basket_size,
        total_items,
        unique_departments,
        unique_aisles
    FROM basket_stats
)
SELECT
    basket_size,
    COUNT(*) as basket_count,
    ROUND(AVG(total_items), 2) as avg_items,
    ROUND(AVG(unique_departments), 2) as avg_departments,
    ROUND(AVG(unique_aisles), 2) as avg_aisles,
    ROUND(AVG(unique_aisles * 1.0 / total_items), 3) as diversity_ratio,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_of_total
FROM size_categories
GROUP BY basket_size
ORDER BY
    CASE basket_size
        WHEN 'Small (1-5 items)' THEN 1
        WHEN 'Medium (6-15 items)' THEN 2
        WHEN 'Large (16-30 items)' THEN 3
        ELSE 4
    END
        """

        return self.spark.sql(query)

    def analyze_reorder_patterns(
        self,
        itemset_size: int = 2,
        min_support: int = 100
    ) -> DataFrame:
        """
        Analyze products frequently reordered together (loyalty patterns)

        Args:
            itemset_size: Size of itemsets to analyze
            min_support: Minimum reorder co-occurrence count

        Returns:
            DataFrame with reorder patterns
        """
        if itemset_size < 2:
            raise ValueError("itemset_size must be at least 2")

        # Generate aliases for joins
        aliases = [f"op{i}" for i in range(1, itemset_size + 1)]

        # Build joins with reorder filters
        joins = [
            f"JOIN {self.config.database_name}.order_products_prior {aliases[i]} "
            f"ON {aliases[0]}.order_id = {aliases[i]}.order_id "
            f"AND {aliases[i-1]}.product_id < {aliases[i]}.product_id "
            f"AND {aliases[i]}.reordered = 1"
            for i in range(1, itemset_size)
        ]
        join_clause = "\n        ".join(joins)

        # Build product selections
        product_ids = ", ".join([f"{alias}.product_id as prod{i}"
                                 for i, alias in enumerate(aliases, 1)])

        # Build product name joins
        product_joins = "\n    ".join([
            f"JOIN {self.config.database_name}.products p{i} ON rp.prod{i} = p{i}.product_id"
            for i in range(1, itemset_size + 1)
        ])

        # Build product names and departments
        product_names = ", ".join([f"p{i}.product_name as product_{i}"
                                   for i in range(1, itemset_size + 1)])
        dept_joins = "\n    ".join([
            f"LEFT JOIN {self.config.database_name}.departments d{i} "
            f"ON TRY_CAST(p{i}.department_id AS BIGINT) = d{i}.department_id"
            for i in range(1, itemset_size + 1)
        ])
        dept_names = ", ".join([f"d{i}.department as dept_{i}"
                                for i in range(1, itemset_size + 1)])

        # Build loyalty type logic
        if itemset_size == 2:
            loyalty_type = """
    CASE
        WHEN d1.department = d2.department THEN 'Same Department Loyalty'
        ELSE 'Cross Department Loyalty'
    END as loyalty_type
            """
        else:
            loyalty_type = "'Multi-Product Loyalty' as loyalty_type"

        query = f"""
WITH reordered_combos AS (
    SELECT
        {product_ids},
        COUNT(DISTINCT {aliases[0]}.order_id) as reorder_count
    FROM {self.config.database_name}.order_products_prior {aliases[0]}
        {join_clause}
    WHERE {aliases[0]}.reordered = 1
    GROUP BY {product_ids}
    HAVING COUNT(DISTINCT {aliases[0]}.order_id) >= {min_support}
)
SELECT
    {product_names},
    rp.reorder_count,
    {dept_names},
    {loyalty_type}
FROM reordered_combos rp
    {product_joins}
    {dept_joins}
WHERE {' AND '.join([f'd{i}.department IS NOT NULL' for i in range(1, itemset_size + 1)])}
ORDER BY reorder_count DESC
LIMIT 100
        """

        return self.spark.sql(query)

    def analyze_cross_department_synergies(
        self,
        min_lift: float = 2.5,
        min_support: int = 30
    ) -> DataFrame:
        """
        Find unexpected cross-department product combinations with high lift

        Args:
            min_lift: Minimum lift threshold
            min_support: Minimum co-occurrence count

        Returns:
            DataFrame with cross-department synergies
        """
        query = f"""
WITH dept_pairs AS (
    SELECT
        TRY_CAST(p1.department_id AS BIGINT) as dept1_id,
        TRY_CAST(p2.department_id AS BIGINT) as dept2_id,
        op1.product_id as prod1,
        op2.product_id as prod2,
        COUNT(DISTINCT op1.order_id) as pair_count
    FROM {self.config.database_name}.order_products_prior op1
    JOIN {self.config.database_name}.order_products_prior op2
        ON op1.order_id = op2.order_id AND op1.product_id < op2.product_id
    JOIN {self.config.database_name}.products p1 ON op1.product_id = p1.product_id
    JOIN {self.config.database_name}.products p2 ON op2.product_id = p2.product_id
    WHERE TRY_CAST(p1.department_id AS BIGINT) IS NOT NULL
      AND TRY_CAST(p2.department_id AS BIGINT) IS NOT NULL
      AND TRY_CAST(p1.department_id AS BIGINT) != TRY_CAST(p2.department_id AS BIGINT)
    GROUP BY dept1_id, dept2_id, op1.product_id, op2.product_id
    HAVING COUNT(DISTINCT op1.order_id) >= {min_support}
),
product_support AS (
    SELECT
        product_id,
        COUNT(DISTINCT order_id) as support
    FROM {self.config.database_name}.order_products_prior
    GROUP BY product_id
),
total_orders AS (
    SELECT COUNT(DISTINCT order_id) as total
    FROM {self.config.database_name}.order_products_prior
)
SELECT
    d1.department as department_1,
    d2.department as department_2,
    p1.product_name as product_1,
    p2.product_name as product_2,
    dp.pair_count,
    ROUND(dp.pair_count * 1.0 / ps1.support, 4) as confidence,
    ROUND(dp.pair_count * 1.0 * to.total / (ps1.support * ps2.support), 2) as lift
FROM dept_pairs dp
CROSS JOIN total_orders to
LEFT JOIN {self.config.database_name}.departments d1 ON dp.dept1_id = d1.department_id
LEFT JOIN {self.config.database_name}.departments d2 ON dp.dept2_id = d2.department_id
JOIN {self.config.database_name}.products p1 ON dp.prod1 = p1.product_id
JOIN {self.config.database_name}.products p2 ON dp.prod2 = p2.product_id
JOIN product_support ps1 ON dp.prod1 = ps1.product_id
JOIN product_support ps2 ON dp.prod2 = ps2.product_id
WHERE ROUND(dp.pair_count * 1.0 * to.total / (ps1.support * ps2.support), 2) > {min_lift}
ORDER BY lift DESC
LIMIT 100
        """

        return self.spark.sql(query)

    def check_data_quality(self) -> DataFrame:
        """
        Check for data quality issues in the products table

        Returns:
            DataFrame with data quality metrics
        """
        query = f"""
SELECT
    COUNT(*) as total_products,
    COUNT(CASE WHEN TRY_CAST(department_id AS BIGINT) IS NULL THEN 1 END) as invalid_dept_ids,
    COUNT(CASE WHEN TRY_CAST(aisle_id AS BIGINT) IS NULL THEN 1 END) as invalid_aisle_ids,
    COUNT(CASE WHEN TRY_CAST(department_id AS BIGINT) IS NOT NULL
               AND TRY_CAST(aisle_id AS BIGINT) IS NOT NULL THEN 1 END) as valid_products
FROM {self.config.database_name}.products
        """

        return self.spark.sql(query)

    def cleanup(self) -> None:
        """Cleanup cached dataframes"""
        if self._transactions_df is not None:
            self._transactions_df.unpersist()


def create_visualization_view(
    spark: SparkSession,
    analyzer: ItemsetAnalyzer,
    itemset_size: int = 2,
    limit: int = 30
) -> None:
    """
    Create a temporary view for visualization in Databricks

    Args:
        spark: SparkSession
        analyzer: ItemsetAnalyzer instance
        itemset_size: Size of itemsets to visualize
        limit: Number of top associations
    """
    associations = analyzer.get_top_associations(
        itemset_size=itemset_size,
        metric="lift",
        limit=limit
    )

    # Create readable labels
    if itemset_size == 2:
        associations = associations.withColumn(
            "association",
            concat_ws(" → ", col("product_1"), col("product_2"))
        )
    elif itemset_size == 3:
        associations = associations.withColumn(
            "association",
            concat_ws(" + ", col("product_1"), col("product_2"), col("product_3"))
        )
    elif itemset_size == 4:
        associations = associations.withColumn(
            "association",
            concat_ws(" + ", col("product_1"), col("product_2"),
                     col("product_3"), col("product_4"))
        )

    associations.createOrReplaceTempView(f"top_associations_{itemset_size}item")
    print(f"✓ Created view: top_associations_{itemset_size}item")


# Convenience functions for quick analysis
def quick_pairs_analysis(
    spark: SparkSession,
    min_support: float = 0.001,
    min_confidence: float = 0.1,
    limit: int = 100
) -> DataFrame:
    """Quick analysis of product pairs"""
    config = AnalysisConfig(
        min_support=min_support,
        min_confidence=min_confidence
    )
    analyzer = ItemsetAnalyzer(spark, config)
    return analyzer.analyze_itemsets(itemset_size=2, limit=limit)


def quick_triplets_analysis(
    spark: SparkSession,
    min_support: float = 0.0005,
    min_co_occurrence: int = 30,
    limit: int = 100
) -> DataFrame:
    """Quick analysis of product triplets"""
    config = AnalysisConfig(
        min_support=min_support,
        min_co_occurrence=min_co_occurrence
    )
    analyzer = ItemsetAnalyzer(spark, config)
    return analyzer.analyze_itemsets(itemset_size=3, limit=limit)


def quick_quadruplets_analysis(
    spark: SparkSession,
    min_support: float = 0.0003,
    min_co_occurrence: int = 20,
    limit: int = 100
) -> DataFrame:
    """Quick analysis of product quadruplets"""
    config = AnalysisConfig(
        min_support=min_support,
        min_co_occurrence=min_co_occurrence
    )
    analyzer = ItemsetAnalyzer(spark, config)
    return analyzer.analyze_itemsets(itemset_size=4, limit=limit)
