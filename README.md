# Analysis of IMDB Data

Christopher Zaman

#### Technologies Leveraged

This project used a stack of technologies which made it possible to conduct analysis on a very large dataset.

**Amazon Web Services** is used as the cloud platform that hosts and run our entire stack.

**Elastic MapReduce and EC2 Cluster** provided efficient distribution and processing of large datasets. 

**Jupyter Notebook** served as the integrated development environment to run the PySpark code. 

**PySpark** was used to conduct data analysis. PySpark used the API so python can work together. 

This evironment and tech stack handles datasets that is too large for environments such as Google Colab. 

#### **Part 1 – Installation and Initial Setup**

First, I installed Pandas/Matplotlib, imported them, and used `list_packages` to verify. 

Next, I loaded the data from the S3 bucket and saved it into a Spark DF. 

`printSchema` shows everything is a string. 

Next, I parallelized each dataset to convert to *RDD* and count elements (rows & columns).

#### **Part 2 – Analyzing Movie Genres**

<u>Association Table</u>

First, I used `from pyspark.sql.functions import split, col, explode, avg` to prepare.

I used `explode` and `split` to break out the comma separated list of genres from the genres column to get an *Association Table*. 

<u>Unique Genres 29?</u>

I used `from pyspark.sql.functions import countDistinct`  so I can call `countDistinct` on genres. 

At first glance, it seems there are *29* unique genres but there was a null value.

To remove null values, I create the variable `nll = '\\N'` and then use `.filter(col("genres") != nll)` 

<u>Total Unique Genres:</u> **28**

Average Rating Genre:

I used: `join_removed_null_df.groupBy("genres").agg(avg("averageRating"))`

Afterwards, I `cast` the averageRating column to `float`.

<u>Horizontal Bar Chart of top Genres:</u> 

Before plotting, I needed to convert the data frame to a `pandas` object. 

`average_rating_per_genre.toPandas()`

Then, I `import matplotlib.pyplot as plt` and plot the figure. 

![part-2](/Users/christopherzaman/Desktop/part-2.png)



#### **Part 3 – Analyzing Movie Genres**

<u>Total unique job categories:</u> **12**

<u>Top Job Categories:</u> **Actor, Self, Actress, Writer, Director.** 

First I used `.groupBy("category").count()` to get count, and then `.orderBy(col("count").desc())` to surface the top job categories. 

![top-job-categories-and-counts](/Users/christopherzaman/Desktop/top-job-categories-and-counts.png)

#### **Bar Chart of Top Job Categories**

To get the top five, I used:

`job_categories = principle.groupBy("category").count().orderBy(col("count").desc()).limit(5)`

To create the Bar Plot:

`job_categories_pd = job_categories.toPandas()
plt.figure(figsize=(10, 6))
job_categories_pd.plot.bar(x='category', y='count', title='Top Job Categories', color='orange', rot=45)
plt.subplots_adjust(bottom=0.2)
plt.ylabel('Count')
plt.xlabel('Job Categories')`

I didn't forget the Magic. 

# 🧙`%matplot plt`



![top-five-job-categories-bar-plot](/Users/christopherzaman/Desktop/top-five-job-categories-bar-plot.png)

#### **Part 4 – Answer the following questions**

1) ##### Provide ratings for the movies from the Harry Potter franchise.

   First, I needed to join the title and rating dataframes. I used the following:

   `title_rating_joined_number_1 = title.join(rating, on="tconst", how="inner")`

   Next, I needed to isolate movies fom other titleTypes, so I filtered for movies:

   `filter(col("titleType") == "movie")`

   Finally, I selected two columns primaryTitle and averageRating, renamed them, and sorted the averageRating column by descending order. 

2) ##### List the films featuring Cillian Murphy as an actor since 2007, including their ratings.

   This question required all tables to be joined, however, name and principle had to be joined on the `nconts`unique identifier *before* joining title and rating on `tconst`.

   With all four tables joined, I was ready to drill down further. 

   First, I filtered for actors only to ensure I get the correct Cillian Murphy:

   `.filter(col("category") == "actor")`

   Now that I am sure I have *actors* only, I filtered again, this time for *Cillian Murphy*. `.filter(col("primaryName") == "Cillian Murphy")`

   With the refined data frame, I filtered for movies only. 

   `.filter(col("titleType") == "movie")`

   Lastly, I filtered for years 2007 and later with the following:

   `.filter(col("startYear") >= 2007)`

   Finally, I renamed and showed the columns primaryTitle, startYear, and averageRating. 

   The result is a list of the *films featuring Cillian Murphy as an actor since 2007, including their ratings*.

3) ##### How many movies has Zendaya featured as an actress in each year?

   This question required three tables to be joined. Again, name and principle had to be joined on `nconst` 

   Afterwards, I joined title on `tconst` 

   Similar to the previous question, I filtered for name, category, and titleType, to ensure I have the correct actress "Zendaya". 

   With the refined data frame, I was ready to find the count of movies for each year. 

   `.groupBy("startYear").count()`

   I noticed I needed to filter out null values.

   `.filter(col("startYear") != '\\N')`

   Finally, I sorted by the year and displayed the result.

   `.sort(col("startYear").desc())`

4) ##### Which movies, released in 2023, have an average rating of 10?

   First, I joined title and ratings.

   Next, I matched the conditions for startYear, titleType, and averageRating. 

   Lastly, I select the primaryTitle column and rename it to Movies.

   Finally, I showed the list of Movies. 

5) ##### At what age did Audrey Hepburn, known for her role in the movie 'Breakfast at Tiffany's,' pass away?

   This question required some preparation. 

   First, I had to explode `knownForTitles` since this was a list of values that match `tconst` 

   `name.withColumn('knownForTitles', explode(split('knownForTitles', ",")))`

   Next, I renamed the exploded `knownForTitles` to `tconst` so I can then join on `tconst` 

   `name_exploded_number_five.withColumnRenamed('knownForTitles', 'tconst')`

   Next, I joined title:

   `name_exploded_number_five.join(title, on="tconst", how="inner")`

   Afterwards, I ensured it's the unique Audrey, and applied filters based on criteria.

   I made sure to `cast` the `birthYear` and `deathYear` to `int` to prepare for calculations.

   Lastly, I calculated the age at death by subtracting the birth year from the death year. 

   Finally, I selected the columns, renamed, and showed the result. 

6) ##### What is the movie(s) with the highest average rating among those featuring Chris Evans, known for his role in 'Captain America: The First Avenger'?

   First, I joined all four tables, starting with `nconst` and then `tconst`.

   I needed to find "Chris Evans" unique ID, so I returned the row with his `nconst` based on filter criteria.

   Using `nconst` I also filtered for movies. 

   Next I needed to use functions so I imported it.

   `from pyspark.sql import functions as F`

   I used aggregation and the `max` function to `collect` the maxRating and store it in a data frame. 

   Lastly, I filtered the columns to show only the max.

   Finally I selected, renamed, and showed the columns with the result. *(I agree with the ratings!)*

7) ##### Among the movies in which Clint Eastwood, known for 'The Good, the Bad and the Ugly', and Harrison Ford, known for 'Raiders of the Lost Ark', have acted, who has the higher average rating?

   This question had many steps and required all tables.

   I started by joining all tables.

   Next, I applied filters to select the `nconst` of both Clint Eastwood and Harrison Ford.

   Next, I applied additional filters with `nconst` to return movies for each actor. 

   To calculate average, I needed `sum` and `count`.

   I used `agg` to `collect` `sum` and `count` of `averageRating` for each actor.

   With this, I can easily divide sum by count to get the average.

   Next, I `print` the average rating for each actor and use .2f for formatting.

   Finally, I used an `if else` to determine which actor has the higher average rating to be outputted.  

8) ##### What are the movies in which both Johnny Depp and Helena Bonham Carter have acted together?

   I began by joining three tables, name, principle, and title.

   I applied filters to target the unique actors Johnny Depp and Helena Bonham Carter and select their `nconst`.

   I applied additional filter for movies and selected the `tconst`. 

   I joined the newly filtered dataframes.

   Finally, I selected one column and renamed it Common Movies and showed the output. 

9) ##### Find the top 5 longest movies directed by Martin Scorsese, known for his work "Gangs of New York".

   I joined three tables: name, principle, title.

   I found the `nconst` of *Martin Scorsese*.

   I filtered further for *director* and *movie*:

   `(col("category") == "director") & (col("titleType") == "movie"))`

   I `cast` runtime minutes to `int`

   I `orderBy` runtime minutes `desc`

   Lastly, I `select` and `limit` the output columns.



> [!NOTE]
>
> I've learned how to analyze big data using PySpark on AWS EMR within the Jupyter Notebook. 
>
> -Christopher Zaman
