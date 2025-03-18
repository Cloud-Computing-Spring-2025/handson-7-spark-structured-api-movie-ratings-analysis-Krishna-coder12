# Movie Ratings Analysis

## **Prerequisites**

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Apache Spark**:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

4. **Docker & Docker Compose** (Optional):
   - If you prefer using Docker for setting up Spark, ensure Docker and Docker Compose are installed.
   - [Docker Installation Guide](https://docs.docker.com/get-docker/)
   - [Docker Compose Installation Guide](https://docs.docker.com/compose/install/)

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

```
MovieRatingsAnalysis/
├── input/
│   └── movie_ratings_data.csv
├── outputs/
│   ├── binge_watching_patterns.csv
│   ├──churn_risk_users.csv
│   └── movie_watching_trends.csv
├── src/
│   ├── task1_binge_watching_patterns.py
│   ├── task2_churn_risk_users.py
│   └── task3_movie_watching_trends.py
├── docker-compose.yml
└── README.md
```

- **input/**: Contains the `movie_ratings_data.csv` dataset.
- **outputs/**: Directory where the results of each task will be saved.
- **src/**: Contains the individual Python scripts for each task.
- **docker-compose.yml**: Docker Compose configuration file to set up Spark.
- **README.md**: Assignment instructions and guidelines.

### **2. Running the Analysis Tasks**

You can run the analysis tasks either locally or using Docker.

#### **Running with Docker**

1. **Start the Spark Cluster**:
   ```bash
   docker-compose up -d
   ```

2. **Run the Python file**:
   ```bash
   python generate_dataset.py
   ```
   
3. **Run Your PySpark Scripts Using `spark-submit`**:
   Update all the Python files according to the requirements and run the below,
   
   ```bash
   spark-submit src/task1_binge_watching_patterns.py
   spark-submit src/task2_churn_risk_users.py
   spark-submit src/task3_movie_watching_trends.py
   ```

4. **Verify the Outputs**:
   On your host machine, check the `outputs/` directory for the resulting files.

5. **Stop the Spark Cluster**:
   ```bash
   docker-compose down
   ```

## **Overview**

In this assignment, you will leverage Spark Structured APIs to analyze a dataset containing movie ratings, user engagement, and streaming behavior. The goal is to extract insights about binge-watching patterns, churn risk users, and long-term streaming trends. This exercise will help you strengthen your data transformation and analytical skills using Spark Structured Streaming APIs.

## **Objectives**

By the end of this assignment, you should be able to:

1. **Data Loading and Preparation**: Import and preprocess data using Spark Structured APIs.
2. **Data Analysis**: Perform complex queries and transformations to address specific business questions.
3. **Insight Generation**: Derive actionable insights from the analyzed data.

## **Dataset**

## **Dataset: Advanced Movie Ratings & Streaming Trends**

You will work with a dataset containing information about **100+ users** who rated movies across various streaming platforms. The dataset includes the following columns:

| **Column Name**         | **Data Type**  | **Description** |
|-------------------------|---------------|----------------|
| **UserID**             | Integer       | Unique identifier for a user |
| **MovieID**            | Integer       | Unique identifier for a movie |
| **MovieTitle**         | String        | Name of the movie |
| **Genre**             | String        | Movie genre (e.g., Action, Comedy, Drama) |
| **Rating**            | Float         | User rating (1.0 to 5.0) |
| **ReviewCount**       | Integer       | Total reviews given by the user |
| **WatchedYear**       | Integer       | Year when the movie was watched |
| **UserLocation**      | String        | User's country |
| **AgeGroup**          | String        | Age category (Teen, Adult, Senior) |
| **StreamingPlatform** | String        | Platform where the movie was watched |
| **WatchTime**        | Integer       | Total watch time in minutes |
| **IsBingeWatched**    | Boolean       | True if the user watched 3+ movies in a day |
| **SubscriptionStatus** | String        | Subscription status (Active, Canceled) |

---

### **Sample Data**

Below is a snippet of the `movie_ratings_data.csv` to illustrate the data structure. Ensure your dataset contains at least 100 records for meaningful analysis.

```
UserID,MovieID,MovieTitle,Genre,Rating,ReviewCount,WatchedYear,UserLocation,AgeGroup,StreamingPlatform,WatchTime,IsBingeWatched,SubscriptionStatus
1,485,Spider-Man: No Way Home,Action,2.6,49,2018,UK,Senior,HBO Max,90,False,Active
2,660,Titanic,Romance,4.5,24,2018,India,Teen,Amazon,64,True,Canceled
3,714,Toy Story,Animation,2.9,11,2023,UK,Adult,Disney+,218,False,Canceled
4,906,The Dark Knight,Action,3.6,1,2021,France,Adult,Apple TV,72,True,Canceled
5,463,Spider-Man: No Way Home,Action,3.4,18,2019,Australia,Teen,Disney+,60,False,Canceled
...
```

## **Assignment Tasks**

You are required to complete the following three analysis tasks using Spark Structured APIs. Ensure that your analysis is well-documented, with clear explanations and any relevant visualizations or summaries.

### **1. Identify Departments with High Satisfaction and Engagement**

**Objective:**

Determine which movies have an average watch time greater than 100 minutes and rank them based on user engagement.

**Tasks:**

- **Filter Movies**: Select movies that have been watched for more than 100 minutes on average.
- **Analyze Average Watch Time**: Compute the average watch time per user for each movie.
- **Identify Top Movies**: List movies where the average watch time is among the highest.


**Outcome:**

A list of departments meeting the specified criteria, along with the corresponding percentages.

**Output:**

| Age Group   | Binge Watchers | Percentage |
|-------------|----------------|------------|
| Senior      | 10             | 34.48      |
| Teen        | 16             | 45.71      |
| Adult       | 21             | 58.33      |

---

### **2. Identify Churn Risk Users**  

**Objective:**  

Find users who are **at risk of churn** by identifying those with **canceled subscriptions and low watch time (<100 minutes)**.

**Tasks:**  

- **Filter Users**: Select users who have `SubscriptionStatus = 'Canceled'`.  
- **Analyze Watch Time**: Identify users with `WatchTime < 100` minutes.  
- **Count At-Risk Users**: Compute the total number of such users.  

**Outcome:**  

A count of users who **canceled their subscriptions and had low engagement**, highlighting **potential churn risks**.

**Output:**  


|Churn Risk Users                                  |	Total Users |
|--------------------------------------------------|--------------|
|Users with low watch time & canceled subscriptions|	16          |

---

### **3. Trend Analysis Over the Years**  

**Objective:**  

Analyze how **movie-watching trends** have changed over the years and find peak years for movie consumption.

**Tasks:**  

- **Group by Watched Year**: Count the number of movies watched in each year.  
- **Analyze Trends**: Identify patterns and compare year-over-year growth in movie consumption.  
- **Find Peak Years**: Highlight the years with the highest number of movies watched.  

**Outcome:**  

A summary of **movie-watching trends** over the years, indicating peak years for streaming activity.

**Output:**  

| Watched Year | Movies watched |
|--------------|----------------|
| 2018         | 16          |
| 2019         | 22          |
| 2020         | 12          |
| 2021         | 10          |
| 2022         | 24          |
| 2023         | 16          |

---
