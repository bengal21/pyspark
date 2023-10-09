from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode


def get_product_category_pairs(products_df, categories_df, relationship_df):
    joined_df = products_df.join(relationship_df, "product_id", "left").join(
        categories_df, "category_id", "left"
    )

    product_category_df = joined_df.select("product_name", "category_name")

    product_category_df = product_category_df.dropDuplicates()

    return product_category_df
