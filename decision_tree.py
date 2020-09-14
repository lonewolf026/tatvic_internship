"""This module consists of functions that helps to generate
 a decision tree automatically after finding the
best hyperparameters using grid search cross validation
and outputs the precision-recall and accuracy score in the end.."""
import datetime
import pickle
from google.cloud import bigquery
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn import metrics
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import confusion_matrix
from sklearn.metrics import classification_report
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
def count_coverage(column_name, project_name, table_name, threshold):
    """This function calculates count and coverage for a categorical column.
    Parameters to be passed:
        a.)column_name: Name of the column for which
                        the count and coverage is to be calculated.
        b.)project_name: The project name in
                        which the table is located
        c.)table_name: Name of the table.
        d.)threshold: If the amount of any particular value(in percentage) in
                a column , is greater than the threshold
                then that particular column will be not
                be considered while making the list for
                dataset generation.
    Result: A 3-columnar dataframe with the terms(arranged in descending
            order on the value of counts),count and coverage.
    """
    #This Query will be passed to the main function
    # for calculation of count and coverage after formatting.
    query = ("""With table as(
            SELECT Count(*) as Count,{col_name} as {col_name}
            FROM (SELECT {col_name}
            FROM {project_name}.{table_name})
            Group by {col_name}
            ORDER by COUNT(*) DESC),
            table_2 as(
            Select {col_name},Count,Count*100/(Select Sum(COUNT) from table)as Coverage
            from table 
            ORDER By Coverage DESC )
            Select * from table_2 where Coverage>{threshold};
        """).format(col_name=column_name,
                    threshold=str(threshold), project_name=project_name, table_name=table_name)
    result = main_func(query)
    #returning the result obtained.
    return result
def main_dt_list(column_list, project_name, table_name, threshold):
    """This function generates the list of columns
    that are to be considered for dataset generation.
    Parameters required: a)column_list:All the names of the columns
                        that you want to consider for dataset generation.
                        b.)project_name: The project name in
                        which the table is located
                        c.)table_name: Name of the table.
                        d.)threshold: amount of null values in a column
                        that can be tolerated in percentage.
    Result: A list with names of columns that has coverage for none
            of its values greater than the threshold."""
    #the list for final dataset generation.
    final_columns = []
    for i in column_list:
        #passing the value to count_coverage function.
        data = count_coverage(i, project_name, table_name, threshold)
        #returned dataframe contains values which has
        #coverage over the threshold limit.
        if data.empty:
            #column_name is added to the list.
            final_columns.append(i)
        elif i == "label":
            #considering label will be imbalanced
            #most of the time and we cannot drop that column
            #this condition allows the addition of label to
            #column_list even if it crosses the threshold.
            final_columns.append(i)
    return final_columns
def null_fill(column_list, project_name, table_name, threshold):
    """This function is used to administer the process of
    filling null values after findind whether a column
    is string or numeric and directs them to their particular
    null fill functions.
    Parameters required:a)column_list:All the names of the columns
                        that you want to consider for dataset generation.
                        b.)project_name: The project name in which the table is located
                        c.)table_name: Name of the table.
                        d.)threshold: amount of null values in a column that can be
                        tolerated in percentage.
    Result: A dataframe with all the columns asked alongwith additional time columns
            with no null values."""
    col_list = main_dt_list(column_list, project_name, table_name, threshold)
    # creation of an empty dataframe with all
    #the final column list values.
    result = pd.DataFrame(columns=col_list)
    for i in col_list:
        #query to check the column datatype.
        query = """WITH column_numeric as(
                    SELECT DATA_TYPE,COLUMN_NAME FROM {project_name}.INFORMATION_SCHEMA.COLUMNS 
                    WHERE table_name="{table_name}")
                    Select DATA_TYPE FROM column_numeric 
                    WHERE COLUMN_NAME='{col_name}'""".format(
                        project_name=project_name, table_name=table_name, col_name=i)
        data = main_func(query)
        #checks whether the datatype is string or Integer or float.
        if (data.iloc[0, 0] == "INT64") or (data.iloc[0, 0] == "FLOAT64"):
            data = numeric_na_fill(i, project_name, table_name)
        elif (data.iloc[0, 0] == "STRING"):
            data = categorical_na_fill(i, project_name, table_name)
        #inserts the data after filling of null values
        #in the same column_value in the empty dataframe
        #that we formed earlier in this function.a
        result[i] = data[i]
    # passes to time_data function for breaking down utc format
    # to hours,months,day of week, week of year.
    final_result = time_data(result, project_name, table_name)
    return final_result
def time_data(data, project_name, table_name):
    """This function checks the presence of visitStartTime column
    and if it is present, it divides the column into hour, week_day,
    week_year and day_month, else just returns the passed value if the
    column is not found.
    Parameters required:a.)data: data formed after filling of null
                            values.
                        b.)project_name: The project name in which
                        the table is located
                        c.)table_name: Name of the table.
    Result: Data with added time columns if visitStartTime was there in the passed dataset."""
    #checks whether the visitStartTime column is present in the dataset.
    if 'visitStartTime' in data.columns:
        query = """SELECT
                    FORMAT_TIMESTAMP('%A',dt_format) AS week_day,
                    FORMAT_TIMESTAMP('%H',dt_format) AS hour,
                    FORMAT_TIMESTAMP('%W',dt_format) AS week_year,
                    FORMAT_TIMESTAMP('%e',dt_format) AS day_month
                    FROM (
                        SELECT
                            TIMESTAMP_SECONDS(visitStartTime) AS dt_format
                        FROM
                            {project_name}.{table_name})
                    """.format(project_name=project_name, table_name=table_name)
        data_dt = main_func(query)
        data_dt[["hour", "week_year", "day_month"]] = data_dt[["hour", "week_year", "day_month"]].apply(
            pd.to_numeric)
        #since the new generated columns may have null values
        #as they were formed after the null_value filling process
        #so these steps are used to fill the null values .
        #the new generated columns are combined with the
        #passed dataset.
        data = data.reset_index(drop=True)
        data_dt = data_dt.reset_index(drop=True)
        final_data = pd.concat([data, data_dt], axis=1)
        #to avoid redundancy we drop the visitStartTime column.
        final_data = final_data.drop(["visitStartTime"], axis=1)
        data = final_data
    return data
def null_coverage(column_name, project_name, table_name):
    """This function calculates the percentage of null values
    that are present in a numeric or categorical column
    Parameters required:a.)column_name:Name of the column for
                            which the null_coverage is to be
                            counted.
                        b.)project_name: The project name in which
                        the table is located
                        c.)table_name: Name of the table.
    Result: A 1x1 dataframe with null coverage value."""
    query = ("""With table as(
            SELECT Count(*) as Count,{col_name} as {col_name}
            FROM (SELECT {col_name}  FROM {project_name}.{table_name})
            Group by {col_name}
            ORDER by COUNT(*) DESC),
            table_2 as(
            Select {col_name},Count,Count*100/(Select Sum(COUNT) from table)as Coverage 
            from table ORDER By Coverage DESC)
            Select Coverage from table_2 where {col_name} IS NULL;
        """).format(col_name=column_name, project_name=project_name, table_name=table_name)
    result = main_func(query)
    #returning the result obtained.
    return result
def numeric_na_fill(column_name, project_name, table_name):
    """ This function checks the percentage of null in numeric column calculated using
    null coverage and then if the threshold is less than 10, fills the null values with median
    and if it is more than 10 then fills with an extreme value.
    Parameters required:a.)column_name:Name of the column for
                            which the null_coverage is to be
                            counted.
                        b.)project_name: The project name in which the table is located
                        c.)table_name: Name of the table.
    Result: A singular column dataframe with the column data generated
            after filling the null values for the specified column name."""
    query = """Select {col_name} from {project_name}.{table_name}""".format(
        col_name=column_name, project_name=project_name, table_name=table_name
    )
    col_data = main_func(query)
    data = null_coverage(column_name, project_name, table_name)
    #in case of no null values.
    if data.empty:
        query_final = """Select {col_name} from {project_name}.{table_name}""".format(
            col_name=column_name, project_name=project_name, table_name=table_name)
        result = main_func(query_final)
    #null value coverage less than 10,then fill with median.
    else:
        if data['Coverage'].iloc[0] <= 10:
            query_final = """Select IFNULL({col_name},{value}) {col_name}
            FROM (SELECT SAFE_CAST({col_name} AS FLOAT64) as {col_name}
            FROM {project_name}.{table_name})""".format(
                col_name=column_name, value=str(col_data.median().values[0]),
                project_name=project_name, table_name=table_name
            )
            result = main_func(query_final)
        #nul value coverage less than 10,then fill with extreme value.
        if data['Coverage'].iloc[0] > 10:
            query_final = """Select IFNULL({col_name},{value}) {col_name}
            FROM (SELECT SAFE_CAST({col_name} AS FLOAT64) as {col_name} 
            FROM {project_name}.{table_name})""".format(
                col_name=column_name, value=str(-9999999999),
                project_name=project_name, table_name=table_name)
            result = main_func(query_final)
    return result
def categorical_na_fill(column_name, project_name, table_name):
    """ This function checks the percentage of null in categorical column calculated using
    null coverage and then if the threshold is less than 10, fills the null values with mode
    and if it is more than 10 then fills with not set.
    Parameters required:a.)column_name:Name of the column for
                            which the null_coverage is to be
                            counted.
                        b.)project_name: The project name in which the table is located
                        c.)table_name: Name of the table.
    Result: A singular column dataframe with the column data generated
            after filling the null values for the specified column name."""
    query = """Select {col_name} from {project_name}.{table_name}""".format(
        col_name=column_name, project_name=project_name, table_name=table_name
    )
    col_data = main_func(query)
    data = null_coverage(column_name, project_name, table_name)
    #in case of no null value.
    if data.empty:
        query_final = """Select {col_name} from {project_name}.{table_name}""".format(
            col_name=column_name, project_name=project_name, table_name=table_name)
        result = main_func(query_final)
    #in case of null value of less than 10% coverage.
    else:
        if data['Coverage'].iloc[0].values[0] <= 10:
            query_final = """Select IFNULL({col_name},"{value}") {col_name}
            FROM (SELECT {col_name} FROM {project_name}.{table_name})""".format(
                col_name=column_name, value=str(col_data.mode().values[0]),
                project_name=project_name, table_name=table_name)
            result = main_func(query_final)
        #in case of null value of more than 10% coverage.
        if data['Coverage'].iloc[0].values[0] > 10:
            query_final = """Select IFNULL({col_name},"{value}") {col_name}
            FROM (SELECT {col_name} FROM {project_name}.{table_name})""".format(
                col_name=column_name, value="not set",
                project_name=project_name, table_name=table_name)
            result = main_func(query_final)
    return result
def grouping(columns, project_name, table_name, threshold, cat_threshold):
    """This function takes all the categorical columns and checks
    whether the number of unique values in that column are more than
    a certain threshold,if it is more,then it categorizes all the values
    above the threshold in 'Others' .
    Parameters required:a)column_list:All the names of the columns
                        that you want to consider for dataset generation.
                        b.)project_name: The project name in which the table is located
                        c.)table_name: Name of the table.
                        d.)cat_threshold: The number of values after which every value
                        will be considered under 'Others
    Result: A multi columnar dataframe with all categorical columns with max (threshold+1)
            unique values."""
    #formation of dataset with null value filling.
    answer = null_fill(columns, project_name, table_name, threshold)
    for i in columns:
        query = """WITH column_numeric as(
                    SELECT DATA_TYPE,COLUMN_NAME FROM {project_name}.INFORMATION_SCHEMA.COLUMNS 
                    WHERE table_name="{table_name}")
                    Select DATA_TYPE FROM column_numeric 
                    WHERE COLUMN_NAME='{col_name}'""".format(
                        project_name=project_name, table_name=table_name, col_name=i)
        data = main_func(query)
        #checks whether any passed column is of categorical type.
        if (data.iloc[0, 0] == "STRING"):
            #checks whether the number of unique columns in a
            # categorical column is more than the threshold.
            if answer[i].nunique() > cat_threshold:
                #takes the count of for the threshold value.
                count = answer[i].value_counts()[0:cat_threshold].to_frame().reset_index(drop=True).iloc[-1, :].values[0]
                #all the counts for different values of the column.
                column_count = answer[i].value_counts()
                #column values whose count is lesser than the threshold value count.
                column_values = answer[i].isin(column_count.index[column_count < count])
                #replaces the values whose count is lesser than a certain limit.
                answer.loc[column_values, i] = "Others"
    #one-hot encoding of the dataset.
    result = pd.get_dummies(answer)
    return result
def decision_tree(column_list, project_name, table_name, threshold=80, cat_threshold=10):
    """This function helps in generation of the decision tree
    automatically and provides the user with values like precision,
    accuracy,recall,f1-score.
    Parameters required:a)column_list:All the names of the columns
                        that you want to consider for dataset generation.
                        b.)project_name: The project name in which the table is located
                        c.)table_name: Name of the table.
                        d.)threshold: amount of null values in a column that can be
                        tolerated in percentage.
                        e.) cat_threshold: number of values above which any value
                        in the categorical column will be considered as "Others"
    Result:Accuracy rate,classification repor and confusion matrix will be formed
            on the basis of the decision tree generated."""
    data = grouping(column_list, project_name, table_name, threshold, cat_threshold)
    train = data.drop(['label'], axis=1)
    train_label = data['label']
    x_train, x_test, y_train, y_test = train_test_split(
        train, train_label, test_size=0.3, random_state=0)
    param_dist = {
        'max_depth':[5, 20],
        'min_samples_leaf':[5, 20]
    }
    tree = DecisionTreeClassifier()
    tree_cv = GridSearchCV(tree, param_dist, cv=2)
    tree_cv.fit(x_train, y_train)
    print('Tuned Decision Tree Parameters:{}'.format(tree_cv.best_params_))
    print('Best Score:{}'.format(tree_cv.best_score_))
    y_pred_class = tree_cv.predict(x_test)
    accuracy = metrics.accuracy_score(y_test, y_pred_class)
    print('Accuracy: {0:0.2f}'.format(
        accuracy))
    print(confusion_matrix(y_test, y_pred_class))
    classification_report_ = classification_report(y_test, y_pred_class, zero_division=1)
    print(classification_report_)
    filename = 'finalized_model.sav'
    pickle.dump(tree_cv, open(filename, 'wb'))