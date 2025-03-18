from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task3_Trend_Analysis"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the movie ratings data from a CSV file into a Spark DataFrame.
    """
    schema = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT, 
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING, 
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def analyze_movie_watching_trends(df):
    """
    Analyze trends in movie watching over the years.
    
    1. Group by WatchedYear and count the number of movies watched.
    2. Order the results by WatchedYear.
    3. Find peak years based on the highest number of movies watched.
    """
    # Step 1: Group by 'WatchedYear' and count the number of movies watched
    movie_watching_trends = df.groupBy("WatchedYear").agg(count("MovieID").alias("Movies_watched"))
    
    # Step 2: Order the results by 'WatchedYear'
    movie_watching_trends = movie_watching_trends.orderBy("WatchedYear")
    
    # Step 3: Find the peak year(s)
    peak_year = movie_watching_trends.orderBy(col("Movies_watched").desc()).limit(1)

    # Combine both trends and peak year for the result
    return movie_watching_trends, peak_year

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 3.
    """
    spark = initialize_spark()

    input_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-Krishna-coder12/input/movie_ratings_data.csv"
    output_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-Krishna-coder12/Outputs/movie_watching_trends.csv"

    df = load_data(spark, input_file)
    
    # Analyze movie watching trends
    movie_watching_trends, peak_year = analyze_movie_watching_trends(df)
    
    # Write trends to output file
    write_output(movie_watching_trends, output_file)
    
    # Print peak year to console
    peak_year.show()

    spark.stop()

if __name__ == "__main__":
    main()
