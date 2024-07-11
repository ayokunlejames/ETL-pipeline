#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


# Stop the existing Spark session if any
try:
    spark.stop()
except:
    pass


# In[3]:


# Create a new Spark session with the specified configuration
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.driver.extraClassPath", "/Users/ayokunlejames/Downloads/postgresql-42.7.3.jar") \
    .getOrCreate()


# In[4]:


# Read the table using JDBC
def extract_movies_to_df():
    movies_df = spark.read \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline") \
      .option("dbtable", "etl_pipeline.movies") \
      .option("user", "postgres") \
      .option("password", "2Brothers1Sister#") \
      .option("driver", "org.postgresql.Driver") \
      .load()
    return movies_df


# In[8]:


# Show the data
extract_movies_to_df().show()


# In[9]:


def extract_user_to_df():
    user_df = spark.read \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline") \
      .option("dbtable", "etl_pipeline.user") \
      .option("user", "postgres") \
      .option("password", "2Brothers1Sister#") \
      .option("driver", "org.postgresql.Driver") \
      .load()
    return user_df


# In[10]:


extract_user_to_df().show()


# In[11]:


## transforming tables
def transform_avg_ratings(movies_df, user_df):
    avg_rating = user_df.groupBy("movie_id").mean("rating")
    
    ##join the movies_df and avg_ratings table on id
    
    df = movies_df.join(
        avg_rating, 
        movies_df.ID == avg_rating.movie_id
    )
    df = df.drop("movie_id")
    return df


# In[14]:


##print all the tables/dataframes
print(extract_movies_to_df().show())
print(extract_user_to_df().show())
print(transform_avg_ratings(extract_movies_to_df(),extract_user_to_df() ).show())


# In[18]:


##load transformed dataframe to the database
def load_df_to_db(df):
    mode = "overwrite"
    url = "jdbc:postgresql://localhost:5432/etl_pipeline"
    properties = {"user": "postgres",
                  "password": "2Brothers1Sister#",
                  "driver": "org.postgresql.Driver"
                  }
    df.write.jdbc(url=url,
                  table = "etl_pipeline.avg_ratings",
                  mode = mode,
                  properties = properties)

if __name__ == "__main__":
    movies_df = extract_movies_to_df()
    user_df = extract_user_to_df()
    ratings_df = transform_avg_ratings(movies_df, user_df)
    load_df_to_db(ratings_df)


# In[19]:


ratings_df.show()


# In[ ]:




