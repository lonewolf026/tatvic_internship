"""This module contains various functions required
to carry out univariate analysis of a
table after generating it dynamically from bigquery. """
from google.cloud import bigquery
import pandas as pd
def column_info(column_name, project_name, table_name):
    """ This function gives information regarding the datatypes of different columns
        Parameters required:a.)project_name: The project name in which the table is located
                            b.)table_name: Name of the table.
                            c.)column_name: Name of the column.
        Result: A dataframe with all the column names and their datatypes"""
    query = """WITH column_numeric as(
                    SELECT DATA_TYPE,COLUMN_NAME FROM {project_name}.INFORMATION_SCHEMA.COLUMNS 
                    WHERE table_name="{table_name}")
                    Select DATA_TYPE FROM column_numeric WHERE COLUMN_NAME = "{col_name}"; 
                """.format(project_name=project_name, table_name=table_name, col_name=column_name)
    result = main_func(query)
    return result
def dynamic_bucket(column_name, project_name, table_name, buckets=10):
    """This function will automatically make buckets for any numeric column,
    Parameters Required:
        a.)column_name: The name of the numeric column for which you want
                        to make dynamic buckets and get the count and coverage.
        b.)buckets: The number of buckets you want to make.
        c.)project_name: The project name in which the table is located
        d.)table_name: Name of the table.
    Result: A string with dynamic buckets will be made."""
    #This Query will be passed to the main function to get the data for the
    #column name provided after formatting the column name in the string.
    query = """SELECT AVG({col_name}) as Mean, STDDEV({col_name}) as St_deviation,
            APPROX_QUANTILES({col_name}, 100)[OFFSET(0)] AS min,
            APPROX_QUANTILES({col_name}, 100)[OFFSET(100)] AS max
            from
            {project_name}.{table_name}""".format(
                col_name=column_name, project_name=project_name, table_name=table_name)
    data = main_func(query)
    #converting to float to NaN incase the any value in the column is in string format
    data = data.transpose()
    data = data.reset_index(drop=True)
    #converting dataframe into series for calculating mean
    #standard deviation without dtypes in the output.'''
    data_mean = round(data.iloc[0, :].values[0], 2)
    data_std = round(data.iloc[1, :].values[0], 2)
    #setting the min and max limit for the buckets.
    # taking the range from -2σ to 2σ for removing outliers.
    min_range = round(data_mean-(2*data_std), 2)
    max_range = round(data_mean+(2*data_std), 2)
        #width of bucket considering the range from min_range to max_range.
    bucket_width = (max_range-min_range)/buckets
    #the outliers will be be moved to two seperate buckets.
    #so the total number of buckets will be two more .
    #than the buckets provided by the user.
    number_of_buckets = buckets+2
    #i is used as an iterator for number of buckets.
    #k is a constraint put up to stop the bucket creation
    # when the upper limit of a bucket is same as the max_range.
    i, k = 0, 0
    #This constant is used to declare an empty string
    # in which the values obtained after each iteration will be added.
    query = ""
    for i in range(0, number_of_buckets):
        #creating outlier bucket
        # for values lower than the min_range
        if i == 0:
            text_2 = "WHEN {col_name} < {min_range} THEN '[{min_data}-{min_range})'\n".format(
                col_name=column_name, min_data=str(round(data.iloc[2, :].values[0], 2)),
                min_range=str(round(data.iloc[2, :].values[0], 2)))
            query = query+text_2
        #creating outlier bucket for values higher than the max_range
        elif i == number_of_buckets-1:
            text_3 = "WHEN {col_name} >= {max_range} THEN '[{max_range}-{max_data})'\n".format(
                col_name=column_name, max_data=str(round(data.iloc[3, :].values[0], 2)),
                max_range=str(round(data.iloc[3, :].values[0], 2)))
            query = query+text_3
        #creating dynamic buckets using min_range and max_range.
        elif k != number_of_buckets-2:
            #increasing the value of k by 1 to change
            # the new lower limit by the previous iteration upper limit
            # and new upper limit increases by the bucket width.
            lower_limit = round(min_range+(k*bucket_width), 2)
            upper_limit = round(min_range+((k+1)*bucket_width), 2)
            k = k+1
            text_4 = "WHEN {lower_limit}>={column_name} AND {column_name}<{upper_limit} THEN '[{lower_limit}-{upper_limit})'\n".format(
                lower_limit=str(lower_limit), upper_limit=str(upper_limit), column_name=column_name)
            query = query+text_4
        i += 1
    return query
def numeric_data_overview(column_name, project_name, table_name):
    """ This function provides the basic overview for a numeric column.
        Parameters to be passed :
            a.)column_name: Name of the numeric column for which you want to see the overview
            b.)project_name: The project name in which the table is located
            c.)table_name: Name of the table.
        Result: You will get a singular column matrix with the values Mean,Standard_deviation,
                quantiles(25,50,75),Min,Max.
    """
    # This Query will be passed to the main function
    # for calculation of mean,std_deviation,quantiles,minimum and maximum
    # after formatting.
    query = """SELECT AVG({col_name}) as Mean, STDDEV({col_name}) as St_deviation,
            APPROX_QUANTILES({col_name}, 100)[OFFSET(0)] AS min,
            APPROX_QUANTILES({col_name}, 100)[OFFSET(25)] AS quantile_25,
            APPROX_QUANTILES({col_name}, 100)[OFFSET(50)] AS quantile_50,
            APPROX_QUANTILES({col_name}, 100)[OFFSET(75)]  AS quantile_75,
            APPROX_QUANTILES({col_name}, 100)[OFFSET(100)] AS max 
            FROM (SELECT SAFE_CAST({col_name} AS FLOAT64) as {col_name} FROM 
            {project_name}.{table_name})""".format(
                col_name=column_name, project_name=project_name, table_name=table_name)
    result = main_func(query)
    #transposing the result to get the answers in a singular column.
    result = result.transpose()
    # renaming the column after transpose with the column name provided in the parameters.
    # 0 is the predefined name given to the singular column after transpose.
    result = result.rename(columns={0:column_name})
    return result
def categorical_overview(column_name, project_name, table_name):
    """ This function provides the basic overview for a categorical column.
        Parameters to be passed :
            a.)column_name: Name of the numeric column for which you want to see the overview
            b.)project_name: The project name in which the table is located
            c.)table_name: Name of the table.
        Result: You will get a three column matrix with the values distinct values(null not
         included),null_count and total count.
    """
    query = ("""With table as(
            SELECT COUNT(DISTINCT {col_name}) FROM {project_name}.{table_name})
            Select * from table ;
        """).format(col_name=column_name, project_name=project_name, table_name=table_name)
    distinct = main_func(query)
    distinct = distinct.rename(columns={'f0_': 'distinct'})
    query = """With table as(
            SELECT Count(*) as Count,{col_name} as {col_name}
            FROM (SELECT {col_name}  FROM {project_name}.{table_name})
            Group by {col_name}
            ORDER by COUNT(*) DESC)
            Select Count from table WHERE {col_name} is NULL;""".format(
                col_name=column_name, project_name=project_name, table_name=table_name)
    count_null = main_func(query)
    count_null = count_null.rename(columns={'Count': 'count_null'})
    query = ("""With table as(
            SELECT Count({col_name}) FROM {project_name}.{table_name})
            Select * from table ;
        """).format(col_name=column_name, project_name=project_name, table_name=table_name)
    total_count = main_func(query)
    total_count = total_count.rename(columns={'f0_': 'total_count'})
    result = pd.concat([distinct, count_null, total_count], axis=1)
    return result
def count_coverage_categorical(column_name, project_name, table_name, terms=10):
    """This function calculates count and coverage for a categorical column.
    Parameters to be passed:
        a.)column_name: Name of the categorical column for which
                        the count and coverage is to be calculated.
        b.)terms:number of terms in the categorical columns for
                which you want to calculate the count and coverage.
        c.)project_name: The project name in which the table is located
        d.)table_name: Name of the table.
    Result: A 3-columnar dataframe with the terms(arranged in descending
            order on the value of counts),count and coverage.
    """
    #This Query will be passed to the main function
    # for calculation of count and coverage after formatting.
    query = ("""With table as(
            SELECT Count(*) as Count,{col_name} as {col_name}
            FROM (SELECT {col_name}  FROM {project_name}.{table_name})
            Group by {col_name}
            ORDER by COUNT(*) DESC)
            Select {col_name},Count,Count*100/(Select Sum(COUNT) from table)as Coverage from table ORDER By Coverage DESC LIMIT {terms};  
                    """).format(col_name=column_name,
                                terms=str(terms), project_name=project_name, table_name=table_name)
    result = main_func(query)
    #returning the result obtained.
    return result
def count_coverage_numeric(column_name, project_name, table_name, buckets=10):
    """This function will take the column_name and buckets as the input and will give an output with
    count,coverage for the numeric column.
    Parameters passed:
        a.)column_name:Name of the numeric column
        b.)buckets: Number of buckets to be made
        c.)project_name: The project name in which the table is located
        d.)table_name: Name of the table.
    Result: A 3-columnar dataframe with buckets,count and their coverage. """
    #This is the final query
    # that will be passed to the main function
    # after formatting the values of Query and column_name.
    query = dynamic_bucket(column_name, project_name, table_name, buckets)
    query_final = """With table as(
        SELECT Count(*) as Count,
        CASE 
        {Query}
        END AS Buckets
        FROM (SELECT SAFE_CAST({col_name} AS FLOAT64) as {col_name} FROM {project_name}.{table_name})
        Group by Buckets
        ORDER by COUNT(*) DESC)
        Select Buckets,Count,Count*100/(Select Sum(COUNT) from table)as 
        Coverage from table ORDER By Coverage DESC;""".format(
            Query=query, col_name=column_name, project_name=project_name, table_name=table_name)
    #calling the main function.
    result = main_func(query_final)
    #Note:in the result,there may be lesser buckets sometimes,
    # because in some buckets the count was 0.
    return result
def compare_leads_numeric(column_name, project_name, table_name, buckets=10):
    """This function will provide the comparision between converted and non-converted coverage
    for various buckets.
    Parameters passed:
        a.)column_name:Name of the numeric column
        b.)buckets: Number of buckets to be made
        c.)table_location: The address where the table is located on which
                        the user wants to perform some action.
    Result: A 3-columnar dataframe with buckets,converted coverage and non-converted coverage.
    Note:An inner join will be performed between the tables of converted coverage
        and non-converted coverage to find the common buckets. """
    query = dynamic_bucket(column_name, project_name, table_name, buckets)
    #This is the final query that will be passed to the main function
    # after formatting the values of Query and column_name.
    query_final = """With table as(
        SELECT Count(*) as Count,
        CASE 
        {Query}
        END AS Buckets
        FROM (SELECT SAFE_CAST({col_name} AS FLOAT64)as {col_name} FROM {project_name}.{table_name} where label=0)
        Group by Buckets),
        table_2 as(
        SELECT Count(*) as Count,
        CASE 
        {Query}
        END AS Buckets_1
        FROM (SELECT SAFE_CAST({col_name} AS FLOAT64)as {col_name} FROM {project_name}.{table_name} where label=1)
        Group by Buckets_1)
        Select a.Count*100/(Select Sum(COUNT) from table) as non_converted_coverage,a.Buckets,b.Count*100/(Select Sum(COUNT) from table_2) as converted_coverage
        from table as a INNER JOIN table_2 as b 
        ON a.Buckets=b.Buckets_1
        Order By a.Count DESC;""".format(Query=query,
                                         col_name=column_name,
                                         project_name=project_name,
                                         table_name=table_name)
    # calling the main function.
    result = main_func(query_final)
    #returning the result obtained.
    return result
def compare_leads_categorical(column_name, project_name, table_name, terms=10):
    """This function compares the leads values for a categorical column.
    Two tables are created one in which the data is sorted according to coverage_converted and
    the other is created on coverage_non_converted.
    Parameters:
        a.)column_name: Name of the numeric column for which you want
            to compare the converted and non-converted values.
        b.)terms: Number of categorical values for which you want to see the comparision.
        c.)project_name: The project name in which the table is located
        d.)table_name: Name of the table.
    Result: A 3-columnar dataframe with converted coverage,terms ,non-converted coverage
    """
    query = ("""With table as(
            SELECT Count(*) as Count,{col_name} as {col_name}
            FROM (SELECT {col_name}  FROM {project_name}.{table_name} Where label=0)
            Group by {col_name}
            ORDER by COUNT(*) DESC),
            table_2 as(
            SELECT Count(*) as Count,{col_name} as {col_name}
            FROM (SELECT {col_name}  FROM {project_name}.{table_name} Where label=1)
            Group by {col_name}
            ORDER by COUNT(*))
            (Select a.Count*100/(Select Sum(COUNT) from table) as non_converted_coverage,a.{col_name},b.Count*100/(Select Sum(COUNT) from table_2) as converted_coverage
            from table as a INNER JOIN table_2 as b 
            ON a.{col_name}=b.{col_name}
            Order BY converted_coverage DESC
            LIMIT {terms})
            UNION DISTINCT
            (Select a.Count*100/(Select Sum(COUNT) from table) as non_converted_coverage,a.{col_name},b.Count*100/(Select Sum(COUNT) from table_2) as converted_coverage
            from table as a INNER JOIN table_2 as b 
            ON a.{col_name}=b.{col_name}
            Order BY non_converted_coverage DESC
            LIMIT {terms});""").format(col_name=column_name,
                                       terms=str(terms),
                                       project_name=project_name,
                                       table_name=table_name)
    result = main_func(query)
    return result
def main_func(query_passed):
    """ This function takes the query as the parameter and runs
    it on bigquery to generate a table which is
    then converted to dataframe.
    Parameters:
        a.)query_passed:The query which is to be run on bigquery.
    Result:The table generated by the query will be converted to a dataframe."""
    client = bigquery.Client()
    data = client.query(query_passed).to_dataframe()
    return data
def test_func(column_list, project_name, table_name, terms=10, buckets=10):
    """This function is created to test
    all the functions that were created
    for columns.
    Parameters required:a.)column_list:Name of all columns for which univariate
                            analysis is to be carried out.
                        b.)project_name: The project name in which the table is located
                        c.)table_name: Name of the table."""
    for i in column_list:
        column_name = i
        result = column_info(column_name, project_name, table_name)
        if (result.iloc[0, :] == "INT64").bool() or (result.iloc[0, :] == "FLOAT64").bool():
            bucket_limits = dynamic_bucket(column_name, project_name, table_name, buckets)
            print(bucket_limits)
            overview = numeric_data_overview(column_name, project_name, table_name, buckets)
            print(overview)
            count_coverage = count_coverage_numeric(column_name, project_name, table_name, buckets)
            print(count_coverage)
            compare_numeric = compare_leads_numeric(column_name, project_name, table_name, buckets)
            print(compare_numeric)
        if (result.iloc[0, :] == "STRING").bool():
            categorical_view = categorical_overview(column_name, project_name, table_name)
            print(categorical_view)
            count_coverage = count_coverage_categorical(column_name, project_name, table_name, terms)
            print(count_coverage)
            compare_categorical = compare_leads_categorical(
                column_name, project_name, table_name, terms)
            print(compare_categorical)
