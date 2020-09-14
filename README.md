# tatvic_internship

The project focuses on extracting the data from a cloud platform in a particular desired format,
before running it on the server. The data obtained, is then used to perform univariate analysis and
decision tree formulation. After the decision tree formulation, the insights obtained will be used
by the client to drive marketing campaigns.

## A.) BIGQUERY AND DATASET CREATION

### a.) Explore Python Client library of BigQuery

● Python has several in-built packages and libraries which can be used to achieve several
tasks in a simpler and efficient way.

● BigQuery is a google-powered serverless data storing warehouse which can be used to
store vast amounts of data at a very cheap cost.

● To run the operations on the dataset on the server, it requires a lot of time, because the
server needs to send the request to bigquery to be applied on the dataset multiple times.

● Therefore, we will run the operations on bigquery and get the preprocessed data directly
from bigquery.

● The process of fetching data from bigquery requires the use of a python client library.

● The python client library has a lot of other methods also, which can be used to carry out
other operations like extracting a particular type of column.

### b.) Get column data types from the dataset.

● Each dataset that is used in analytics has two types of columns, categorical and
numerical.

● The dataset type is important to know because that will help us give an idea about the
data present in it and how to preprocess it.

● To access the data about the column type, INFORMATION_SCHEMA.COLUMNS is
used.

● This will help us in identifying which operations are to be carried out on which column
of the dataset.

### c.) Create a dataset for insights generation

● So, whatever I had learnt under the explore bigquery and python client library task, I had
to use that to connect a main function with parameters.

● The parameter in the function is the Query that is to be passed to bigquery using the
python client library.

● The data to be fetched from the bigquery should be arriving back at the end of the
function in the form of a dataframe.

● This function will be repeated again and again by various functions for getting the data
from bigquery.

● The single function reduces the lines of code that could have been redundant.


### d.) Create additional fields from VisitStartTime from BigQuery

● The dataset in Bigquery has the time in UTC format (time in seconds from a particular
date till the time that observation was recorded).

● The UTC-time is converted to normal time format.

● Then, various other column data were extracted from the normal date and time format
column.

● The different data that were extracted are:

○ Month.

○ Day of the week.

○ Day of the month.

○ Week of the year.

● This will help in making the predictions and classifying the different types of user more
precisely.


## B.) UNIVARIATE ANALYSIS

### a.) Dynamic Bucket Size Selection For Numeric
 
 ● The data that is present in a numeric column is really hard to analyse, since there can be a
large number of discrete values present.
 
 ● In order to deal with this problem, we had to make different buckets for the columns and
count the number of values coming in each column, along with coverage of each bucket
generated.

● The mean of the column is calculated and a z-score of 2 is allowed, for data to be
considered while bucket formation.

● The lower limit and upper limit for bucket formation are set by subtracting the z-score
from the mean values.

● Two more buckets are made to consider the values lying outside the lower and upper
limit points for bucket formation.

● The SQL CASE operation is used to classify different discrete points in various buckets.

### b.) Basic Overview of Distribution For Numeric

● All the data points for numeric will be classified into bins using SQL CASES and the
count will also be generated.

● After calculating the count, the coverage of each basket is to be calculated and sorted to
give an idea to the user regarding the basket with the highest and the least coverage from
the data set.


### c.) Top Count And Coverage For Categorical Data

● In the categorical columns,the count for each discrete value was to be calculated and a
dataframe with the count for each discrete value should have come in the output.

● The coverage is also to be calculated for the discrete values and then it should be sorted
in decreasing order to get an idea of which discrete value is most important from the
categorical column.


### d.)Basic Overview of Distribution For Numeric

● A data frame with the discrete value along with count and coverage should be given in
the form of output.

● This output will be useful for the user to analyse the column and get the idea of the
distribution of the data in the column.


### e.) Comparison for converter and non-converter data

● Both the numeric baskets and categorical values along with the count and coverage
values are segregated on the basis of whether the lead is converted or
non-converted.

● The coverage values were counted for converted and unconverted numeric baskets and
categorical values.

● The converted percentage was kept on one side,while unconverted was on the other side
with the numeric baskets or categorical values in between depending on the column
type.

● Then, the output is given in the form of a data frame.


## C.) DECISION TREE

### a.) Data Cleaning (Drop Null Values and Repetitive Fields)

● The data present in the numerical columns has a lot of null values and repetitive fields
present in them.

● For each column, the count of each distinct value was found and a parameter was given
to the function which could be set by the user.

● This parameter acts as a threshold value.

● If the count of any distinct value crosses the threshold, then those values are dropped
from the dataset.

● In order to deal with the null values, after removal of the repetitive fields, DROPNA
command is used for the data frame to remove any null value present in a column.

### b.) Pre-processing(Filling of missing values) of numeric and categorical column

● Since, missing values in the dataset can adversely affect the classification of different
values using a decision tree, that's why it is very important to fill in the missing values.

● For numeric columns, all the missing values are to be filled by a very distant number,
because filling a close number may hamper the classification process of the decision
tree.

● For categorical columns, all the missing values are substituted using mode value
because as a particular value is appearing too many times, we can assume that the
missing values had the same value as that.

● Pre-processing forms an important part as it can make the classification good or bad on
the basis of the method of filling these missing values.


### c.) Pre-processing (Top count/Others grouping) of categorical columns

● Since there is a probability that the number of distinct values in a categorical column are
too much.

● So, to deal with this situation, a function was made with a threshold to be set by the user,
on how many values can be allowed to be exempt from grouping into Other category.

● The count and coverage was calculated for the categorical column and then the column
discrete values were arranged on the basis of decreasing order of coverage.

● The values after the threshold value are clubbed together to form the ‘Other’ category.

### d.) Pre-processing (One hot encoding) of categorical columns

● Since the code is to be run on a computer which cannot understand categorical data
present in the dataset, so it becomes very important to convert it into numerical form to be
processed.

● We will use pd.dummies on the dataset to create dummy variables for the categorical data
present.


### e.) Decision Tree Hyperparameter Tuning

● Since we cannot manually tune the model to get the perfect decision tree classification on
the dataset.

● We need to run the dataset on a cross validation model which finds out the best
hyperparameters on which the data will give the best outputs.

● Two-three hyperparameters are kept in the cross validation model for which the values are
saved.

● The model after running on those particular values, is saved on the system using the pickle
python library.

## Simplified process Diagram:

![Process_flow](https://user-images.githubusercontent.com/44407232/93124398-25c88d00-f6e7-11ea-8e6e-2abc59cdf4d0.PNG)


## One of the output screenshot:

![output](https://user-images.githubusercontent.com/44407232/93124140-c8ccd700-f6e6-11ea-8c04-2358a8d6e1c2.PNG)
