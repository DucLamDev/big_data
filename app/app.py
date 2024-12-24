from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from flask import Flask, jsonify, make_response
from flask_cors import CORS
import os
import logging
from delta import configure_spark_with_delta_pip

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

def create_spark_session():
    try:
        builder = SparkSession.builder \
            .appName("Delta Lake Analysis") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "10")
        
        return configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise

delta_table_path = "/app/data/delta/user_behavior"
input_csv = "/app/data/user_behavior_large_sample.csv"

@app.route('/api/health', methods=['GET'])
def health_check():
    try:
        if spark.sparkContext.isActive:
            return jsonify({"status": "healthy", "spark": "active"})
        return make_response(jsonify({"status": "unhealthy", "spark": "inactive"}), 503)
    except Exception as e:
        return make_response(jsonify({"status": "error", "message": str(e)}), 500)

def init_delta_table():
    try:
        if not os.path.exists(delta_table_path):
            logger.info("Initializing Delta table from CSV...")
            data = spark.read.csv(input_csv, header=True, inferSchema=True)
            data.write.format("delta").mode("overwrite").save(delta_table_path)
            logger.info("Delta table initialized successfully")
        
        return spark.read.format("delta").load(delta_table_path)
    except Exception as e:
        logger.error(f"Failed to initialize Delta table: {str(e)}")
        raise

try:
    spark = create_spark_session()
    delta_data = init_delta_table()
except Exception as e:
    logger.error(f"Application startup failed: {str(e)}")
    raise

@app.route('/api/user-actions', methods=['GET'])
def user_actions():
    try:
        user_action_counts = delta_data.groupBy("UserID", "Action") \
            .agg(count("Action").alias("ActionCount")) \
            .cache().limit(50)
        
        result = user_action_counts.collect()
        return jsonify([row.asDict() for row in result])
    except Exception as e:
        logger.error(f"Error in user_actions: {str(e)}")
        return make_response(jsonify({"error": str(e)}), 500)

@app.route('/api/conversion-rate', methods=['GET'])
def conversion_rate():
    try:
        conversion_rate = delta_data.groupBy("UserID").agg(
            count(when(col("Action") == "View", 1)).alias("ViewCount"),
            count(when(col("Action") == "AddToCart", 1)).alias("AddToCartCount"),
            count(when(col("Action") == "Purchase", 1)).alias("PurchaseCount")
        ).cache().limit(50)
        
        result = conversion_rate.collect()
        return jsonify([row.asDict() for row in result])
    except Exception as e:
        logger.error(f"Error in conversion_rate: {str(e)}")
        return make_response(jsonify({"error": str(e)}), 500)

@app.route('/api/popular-products', methods=['GET'])
def popular_products():
    try:
        popular_products = delta_data.groupBy("ProductID") \
            .agg(count("Action").alias("ActionCount")) \
            .cache()
        
        result = popular_products.orderBy(col("ActionCount").desc()) \
            .limit(10) \
            .collect()
        
        return jsonify([row.asDict() for row in result])
    except Exception as e:
        logger.error(f"Error in popular_products: {str(e)}")
        return make_response(jsonify({"error": str(e)}), 500)

@app.route('/api/user-segmentation', methods=['GET'])
def user_segmentation():
    try:
        user_segments = delta_data.groupBy("UserID").agg(
            count(when(col("Action") == "Purchase", 1)).alias("PurchaseCount"),
            count(when(col("Action") == "View", 1)).alias("ViewCount")
        ).cache()
        
        user_segments = user_segments.withColumn(
            "EngagementLevel",
            when(col("PurchaseCount") > 5, "High")
            .when(col("PurchaseCount") > 1, "Medium")
            .otherwise("Low")
        )
        
        result = user_segments.collect()
        return jsonify([row.asDict() for row in result])
    except Exception as e:
        logger.error(f"Error in user_segmentation: {str(e)}")
        return make_response(jsonify({"error": str(e)}), 500)

@app.route('/api/convert-after-view', methods=['GET'])
def convert_after_addtocart():
    try:
        view_data = delta_data.filter(col("Action") == "View")
        purchase_data = delta_data.filter(col("Action") == "Purchase")
        
        users_view_product = view_data.select("UserID").distinct()
        total_users_view = users_view_product.count()

        users_purchase_after_view = purchase_data.join(
            users_view_product, "UserID", "inner"
        ).select("UserID").distinct()
        total_users_purchase_after_view = users_purchase_after_view.count()

        if total_users_view > 0:
            conversion_rate = (total_users_purchase_after_view / total_users_view) * 100
        else:
            conversion_rate = 0 

        # Kết quả trả về
        return jsonify({
            "total_users_view": total_users_view,
            "total_users_purchase_after_addtocart": total_users_purchase_after_view,
            "conversion_rate": conversion_rate
        })
    except Exception as e:
        logger.error(f"Error in convert_after_addtocart: {str(e)}")
        return make_response(jsonify({"error": str(e)}), 500)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

