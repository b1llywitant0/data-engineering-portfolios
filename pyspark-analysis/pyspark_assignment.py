# LIBRARIES
# After installing pyspark in local device, we need to install and import findspark
# !pip install findspark
import findspark
findspark.init()

# Importing pyspark libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col,isnan,when,count
from pyspark.sql.functions import to_date

# To export graphs later
# !pip install -U kaleido
# !pip install plotly
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# SPARK CONTEXT
print("Creating Spark Context Using SparkSession")
print("-------------------------------------------------")
spark_session = SparkSession.builder\
                .appName('spark_task_1')\
                .master('local[2]')\
                .getOrCreate()

spark_context_new = spark_session.sparkContext
spark_context_new
print("")

# IMPORT DATA TO SPARK
# Creating schema of data types - calendar.csv
schema_calendar = StructType([
    StructField('_c0', IntegerType(), True),
    StructField('date', DateType(), True),
    StructField('start_of_the_year', DateType(), True),
    StructField('start_of_the_quarter', DateType(), True),
    StructField('start_of_the_month', DateType(), True)
])
# Reading the csv data - calendar
calendar = spark_session.read.csv("D:\Billy's\DE\PySpark\calendar.csv", header=True, schema=schema_calendar)
# Deleting unnecessary column
calendar = calendar.drop('_c0')
# Showing the data type of each column from calendar
# calendar.printSchema()
# Showing the first 5 data from calendar 
# calendar.show(5)

# Creating schema of data types - customer_flight_activity.csv
schema_cust_flight = StructType([
    StructField('_c0', IntegerType(), True),
    StructField('loyalty_number', IntegerType(), True),
    StructField('year', StringType(), True),
    StructField('month', StringType(), True),
    StructField('total_flights', IntegerType(), True),
    StructField('distance', DoubleType(), True),
    StructField('points_accumulated', DoubleType(), True),
    StructField('points_redeemed', IntegerType(), True),
    StructField('dollar_cost_points_redeemed', StringType(), True)
])
# Reading the csv data - customer_flight_activity
cust_flight = spark_session.read.option("nullValue","").csv("D:\Billy's\DE\PySpark\customer_flight_activity.csv", header=True, schema=schema_cust_flight)
# Deleting unnecessary column
cust_flight = cust_flight.drop('_c0')
# Showing the data type of each column from cust_flight
print("printSchema for cust_flight")
print("-------------------------------------------------")
cust_flight.printSchema()
# Showing the first 5 data from cust_flight
print("Top 5 Data of cust_flight")
print("-------------------------------------------------")
cust_flight.show(5)
print("")

# Creating schema of data types - customer_loyalty_history.csv
schema_cust_loyalty = StructType([
    StructField('_c0', IntegerType(), True),
    StructField('loyalty_number', IntegerType(), True),
    StructField('country', StringType(), True),
    StructField('province', StringType(), True),
    StructField('city', StringType(), True),
    StructField('postal_code', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('education', StringType(), True),
    StructField('salary', DoubleType(), True),
    StructField('marital_status', StringType(), True),
    StructField('loyalty_card', StringType(), True),
    StructField('customer_lifetime_value', DoubleType(), True),    
    StructField('enrollment_type', StringType(), True),
    StructField('enrollment_year', StringType(), True),
    StructField('enrollment_month', StringType(), True),
    StructField('cancellation_year', StringType(), True),
    StructField('cancellation_month', StringType(), True)
])
# Reading the csv data - customer_loyalty_history
cust_loyalty = spark_session.read.option("nullValue","").csv("D:\Billy's\DE\PySpark\customer_loyalty_history.csv", header=True, schema=schema_cust_loyalty)
# Deleting unnecessary column
cust_loyalty = cust_loyalty.drop('_c0')
# Showing the data type of each column from cust_loyalty
print("printSchema for cust_loyalty")
print("-------------------------------------------------")
cust_loyalty.printSchema()
# Showing the first 5 data from cust_loyalty
print("Top 5 Data for cust_loyalty")
print("-------------------------------------------------")
cust_loyalty.show(5)
print("")

print("Based on the tables showed above, it can be seen that:")
print("cust_flight contains event-based information of flight from each person based on loyalty_number")
print("cust_loyalty contains user-based information with the details of user based on their loyalty_number")
print("")

# DATA UNDERSTANDING & CLEANING
# Creating temp tables for SQL querying
cust_loyalty.createOrReplaceTempView('cust_loyalty')
cust_flight.createOrReplaceTempView('cust_flight')
calendar.createOrReplaceTempView('calendar')

# CUST_FLIGHT
print("Checking NULL Values for cust_flight")
print("-------------------------------------------------")
columns_check=["loyalty_number","year","month","total_flights","distance","points_accumulated","points_redeemed",
               "dollar_cost_points_redeemed"]
cust_flight.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in columns_check]).show()
print("")

print("Checking the data overall")
print("-------------------------------------------------")
cust_flight.summary().show()
print("")

# I want to sort the data based on year and month columns for each loyalty number to look at the data better
print("Exploring cust_flight #1")
print("-------------------------------------------------")
spark_session.sql("""
    SELECT * FROM cust_flight
    WHERE loyalty_number = 100590
    ORDER BY year, month ASC
""").show()
print("Points was purely based on distance. Points = Distance (in km) * 1.5")
# Points accumulated also was based on the flights in the given period, not the total accumulated points over time
print("")

# Checking the mechanism of dollar_cost_points_redeemed
print("Exploring cust_flight #2")
print("-------------------------------------------------")
cust_flight.where(cust_flight['dollar_cost_points_redeemed']!=0).show()
print("dollar_cost_points_redeemed = points_redeemed * 0.18")
print("")

# Several conditions to clean this dataset:
# 1. Distance should always be filled everytime there is total_flights, vice versa
# 2. Points accumulated should always be filled everytime there is distance, vice versa
# 3. Dollar cost points redeemed should always be filled everytime there is points redeemed, vice versa

# Establishing condition based on data understanding (TRANSFORMATION)
cust_flight = cust_flight.withColumn('points_accumulated', F.round(F.col('distance')*1.5,2))
cust_flight = cust_flight.withColumn('dollar_cost_points_redeemed', F.round(F.col('points_redeemed')*0.18,0))

print("Getting flight period")
print("-------------------------------------------------")
cust_flight = cust_flight.withColumn('flight_period', F.concat_ws("-",F.col("year"),F.col("month")))
cust_flight = cust_flight.withColumn('flight_period', F.to_date(F.col("flight_period"), 'yyyy-M'))
cust_flight.show()

print("Deleting unnecessary rows from CUST_FLIGHT")
print("-------------------------------------------------")
cust_flight = cust_flight.replace(0.0,None)
cust_flight = cust_flight.na.drop(subset=['total_flights','distance','points_accumulated','points_redeemed','dollar_cost_points_redeemed'], how='all')
cust_flight = cust_flight.na.fill(value=0.0)
cust_flight.show()

# CUST_LOYALTY
print("Checking NULL Values for cust_loyalty")
print("-------------------------------------------------")
columns_check=["loyalty_number","country","province","city","postal_code","gender","education",
                "salary","marital_status","loyalty_card","customer_lifetime_value","enrollment_type","enrollment_year",
                "enrollment_month","cancellation_year","cancellation_month"]
cust_loyalty.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in columns_check]).show()
print("")

print("Checking the data overall")
print("-------------------------------------------------")
cust_flight.summary().show()
print("Negative salary")
print("")

# Trying to recover salary information based on the frequency of flights and membership status
# Checking whether membership have correlation with total_distance, total_flights
print("Exploring cust_loyalty #1")
print("-------------------------------------------------")
spark_session.sql("""
    WITH calc AS (
    SELECT
    cl.loyalty_card AS membership,
    COUNT(DISTINCT(cl.loyalty_number)) AS n_user,
    SUM(cf.distance) AS total_distance, 
    SUM(cf.total_flights) AS total_flights,
    MEDIAN(cl.salary) AS median_salary
    FROM cust_flight cf
    RIGHT JOIN cust_loyalty cl ON cf.loyalty_number = cl.loyalty_number
    GROUP BY membership
    )

    SELECT *, total_flights/n_user AS avg_flights, total_distance/n_user AS avg_distance FROM calc
""").show()

print("No significant difference in average salary, average flights, average distance between memberships")
print("However, it can be seen that the order of membership from lowest to highest: Star > Nova > Aurora")
print("I don't think it is wise to change the NULL values with avg_salary based on membership")
print("-------------------------------------------------")
print("")
print("CLTV: total invoice value for all flights ever booked by member")
print("Based on the definition, it should be the correlated with transaction value of flights from users")
print("No information regarding that, so it can't be recovered")
print("-------------------------------------------------")
print("")

spark_session.sql("""
    WITH 
    data_flight AS (
    SELECT loyalty_number, MIN(year) AS first_flight 
    FROM cust_flight
    GROUP BY loyalty_number
    ),
    joined AS (
    SELECT cl.loyalty_number, cl.enrollment_year, cl.cancellation_year, df.first_flight 
    FROM cust_loyalty cl
    LEFT JOIN data_flight df ON cl.loyalty_number = df.loyalty_number
    )
    SELECT * FROM joined
    WHERE cancellation_year < first_flight
""").show()

print("From this result, it can be concluded that enrollment date didn't have any correlation with flight date")
print("Thus, the data can't be recovered")

# Cleaning negative salary
cust_loyalty = cust_loyalty.withColumn('salary', F.abs(F.col('salary')))

# Drop duplicates
dedup_cust_loyalty = cust_loyalty.dropDuplicates(subset=['loyalty_number'])

# VISUALIZATION
# Creating temp tables for SQL querying
dedup_cust_loyalty.createOrReplaceTempView('dedup_cust_loyalty')
cust_flight.createOrReplaceTempView('cust_flight')
calendar.createOrReplaceTempView('calendar')

# When is the peak season of flights?
df1 = spark_session.sql("""
    SELECT 
    cf.flight_period AS period,
    cal.start_of_the_quarter AS quarter_cat,
    cal.start_of_the_year AS year_cat,
    SUM(cf.total_flights) AS total_flights,
    SUM(cf.points_redeemed) AS redeemed_points
    FROM cust_flight cf
    LEFT JOIN calendar cal ON cal.date = cf.flight_period
    GROUP BY period, quarter_cat, year_cat
    ORDER BY period ASC
""")

pandas_df1 = df1.toPandas()
pandas_df1


fig = make_subplots(specs=[[{"secondary_y": True}]])

fig.add_trace(go.Scatter(x=pandas_df1['period'], y=pandas_df1['total_flights'],
                    mode='lines',
                    name='total_flights'), secondary_y=False)
fig.add_trace(go.Scatter(x=pandas_df1['period'], y=pandas_df1['redeemed_points'],
                    mode='lines',
                    name='redeemed_points'), secondary_y=True)

fig.update_xaxes(title_text="Period")
fig.update_yaxes(title_text="Total Flights", secondary_y=False)
fig.update_yaxes(title_text="Redeemed Points", secondary_y=True)

fig.show()
# pio.write_image(fig, "line.jpeg")
# px.line(pandas_df1, x='period', y='total_flights')
# The seasonal trend was observed with peaks on March, July, and December.
# It is surprising that the peak of flights season of the dataset located mostly in July.

# cust_loyalty.select('country').distinct().show()
# Considering the location of the dataset (Canada), the peak in July was caused by Summer Break Holiday.
# Reference: https://www.edarabia.com/school-holidays-canada/#:~:text=Summer%20break%20in%20Canada%20typically,and%20ends%20in%20New%20Year).

# Is there correlation between salary and the frequency or distance of flights?
df2 = spark_session.sql("""
    SELECT 
    cl.loyalty_number,
    cl.loyalty_card,
    SUM(cf.total_flights) AS total_flights,
    SUM(cf.distance) AS total_distance,
    AVG(cl.salary) AS salary
    FROM dedup_cust_loyalty cl
    LEFT JOIN cust_flight cf ON cf.loyalty_number = cl.loyalty_number
    GROUP BY cl.loyalty_number, cl.loyalty_card
    HAVING salary IS NOT NULL
""")

pandas_df2 = df2.toPandas()
pandas_df2

fig1 = px.scatter(pandas_df2,x='total_flights',y='total_distance',color='loyalty_card')
fig1.show()
# pio.write_image(fig1, "total_flights_vs_distance.jpeg")
# Since total flights and total distance was highly correlated, let's just pick one to compare with salary
# Add loyalty card information to see whether there are differences between groups

fig2 = px.scatter(pandas_df2,x='total_flights',y='salary',color='loyalty_card')
fig2.show()
# pio.write_image(fig2, "total_flights_vs_salary.jpeg")
# No pattern was seen from the graph. 
# Even without statistics, we can assume that no significant correlation between total_flights and salary

fig3 = px.histogram(pandas_df2,x='total_flights',color='loyalty_card')
fig3.show()
# pio.write_image(fig3, "hist_loyalty1.jpeg")

fig3 = px.histogram(pandas_df2,x='salary',color='loyalty_card')
fig3.show()
# pio.write_image(fig3, "hist_loyalty2.jpeg")

# No difference of behaviour between groups based on total flights

# Gender, Education vs Total Flights
df3 = spark_session.sql("""
    SELECT 
    cl.gender,
    cl.education,
    SUM(cf.total_flights) AS total_flights,
    SUM(cf.distance) AS total_distance
    FROM dedup_cust_loyalty cl
    LEFT JOIN cust_flight cf ON cf.loyalty_number = cl.loyalty_number
    GROUP BY cl.gender, cl.education
""")

pandas_df3 = df3.toPandas()
pandas_df3

fig4 = px.bar(pandas_df3,x='gender',y='total_flights',color='education',title='Flights based on Gender and Education')
# pio.write_image(fig4, "bar_gender_edu.jpeg")
fig4.show()

# Province with Most Flight
df4 = spark_session.sql("""
    SELECT 
    cl.province,
    SUM(cf.total_flights) AS total_flights,
    SUM(cf.distance) AS total_distance
    FROM dedup_cust_loyalty cl
    LEFT JOIN cust_flight cf ON cf.loyalty_number = cl.loyalty_number
    GROUP BY cl.province
    ORDER BY total_flights DESC
""")

pandas_df4 = df4.toPandas()
pandas_df4

fig5 = px.bar(pandas_df4,x='province',y='total_flights',title='Top Cities with Most Flights')
# pio.write_image(fig5, "bar_cities.jpeg")
fig5.show()