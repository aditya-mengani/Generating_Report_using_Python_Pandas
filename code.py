from __future__ import print_function
import boto3
import json
import decimal
import matplotlib.pyplot as plt
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib
import pandas as pd
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from pandas import DataFrame
import io
from matplotlib.font_manager import FontProperties

# The below function highlights the value of time difference to red if it is greater than 1 hour, else it highlights it to green
def color_negative_red(value):
  """
  Colors elements in a dateframe
  green if positive and red if
  negative. Does not color NaN
  values.
  """

  if value > 3600:
    color = 'red'
  elif value < 3600:
    color = 'green'

  return 'color: %s' % color
  
  # Module to convert DynamoDB item to JSON
  class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
        
# The below code defines the DynamoDB tables and uses the scan operation to fetch the records. (change to get entire dataset)
#In future, we can add query or BatchGet to get a group of records if scan operation turns expensive

dynamodb = boto3.resource("dynamodb", region_name='us-east-1')

table_specs = dynamodb.Table('XXXX')
table_workflow = dynamodb.Table('XXXX')
table_workflow_det = dynamodb.Table('XXXXX')

# Expression Attribute Names for Projection Expression only.
response_specs = table_specs.scan()
response_workflow = table_workflow.scan()
response_worklow_det = table_workflow_det.scan()

df_response_worklow_det = json.dumps(response_worklow_det, cls=DecimalEncoder)

# Define a pandaDataframe on the dictionary object returned by DynamoDB call. 
# Then Extract the needed elements for the report. 
# Convert the Date attributes in string to Date type and find the difference between the runs

df_response_worklow_det = pd.DataFrame(response_worklow_det["Items"])

df_response_worklow_det = df_response_worklow_det[["Workflow_Id","Table_Name","Status","Time_From","Time_To"]]

df_response_worklow_det = df_response_worklow_det.drop(df_response_worklow_det.index[6])
df_response_worklow_det = df_response_worklow_det.drop(df_response_worklow_det.index[10])

df_response_worklow_det['Date_From']= pd.to_datetime(df_response_worklow_det['Time_From']).dt.date
df_response_worklow_det['Date_To']= pd.to_datetime(df_response_worklow_det['Time_To']).dt.date

df_response_worklow_det['Time_From']= pd.to_datetime(df_response_worklow_det['Time_From'])
df_response_worklow_det['Time_To']= pd.to_datetime(df_response_worklow_det['Time_To'])

df_response_worklow_det['Time_Diff']= (df_response_worklow_det['Time_To']-df_response_worklow_det['Time_From'])
df_response_worklow_det['Time_Diff_Sec']= (df_response_worklow_det['Time_To']-df_response_worklow_det['Time_From'])/np.timedelta64(1,'s')
df_response_worklow_det['Run_Date']= (df_response_worklow_det['Time_To']-df_response_worklow_det['Time_From'])

# Group the records in the dataframe by "Workflow_ID" and get maximum time difference for each 
# workflow, minimum start date, maximum end_date and count of tables loaded for each workflow run

df_report = df_response_worklow_det.groupby(["Workflow_Id"],as_index=False).agg({"Time_Diff_Sec" : "max" , "Date_From" : "min", "Date_To" : "max","Table_Name" : "count"}).rename(columns={"Table_Name":"Count_of_tables"})


# Style the records in dataframe by calling the styling method "color_negative_red" using matplot lib. 
# All the records having time > 3600 seconds are highlighted in red
df_report.style.applymap(color_negative_red, subset=['Time_Diff_Sec'])

# Histogram showing the spread of attribute "Time_Diff_Sec"
ax = df_report.hist(column= "Time_Diff_Sec" ,bins=100)

# Histogram showing the spread of attribute "Count_of_tables"
bx = df_report.hist(column= "Count_of_tables" ,bins=10)

# Histogram showing the spread of attribute "Count_of_tables" grouping over "Date_From" date range
bx = df_report.hist(column= "Count_of_tables" ,by = "Date_From",bins=10)

# Histogram showing the spread of attribute "Time_Diff_Sec" grouping over "Date_From" date range
bx = df_report.hist(column= "Time_Diff_Sec" ,by = "Date_From",bins=10)

# Line graph showing the spread of attribute "Time_Diff_Sec" across range of group
cx = df_report["Time_Diff_Sec"].plot()


df_response_worklow_det.style.applymap(color_negative_red, subset=['Time_Diff_Sec'])

buf = io.BytesIO()


with PdfPages('/tmp/Charts.pdf') as export_pdf:
    
    df_response_worklow_det
    fig,ax =plt.subplots(figsize=(12,4))
    fig.suptitle('Report for task/table level metrics', fontsize=12,x= 0.55,y=0.75,fontweight='bold')
    plt.subplots_adjust(top=0.85)
    ax.axis('tight')
    ax.axis('off')
    the_table = ax.table(cellText=df_response_worklow_det.values,
                         colLabels=df_response_worklow_det.columns,
                         loc='center')
    for (row, col), cell in the_table.get_celld().items():
        if (row == 0) or (col == -1):
            cell.set_text_props(fontproperties=FontProperties(weight='bold'))
        if (col == 6 and (row==1 or row==2 or row==3 or row==4) ):
            cell.set_facecolor("red")
            
    export_pdf.savefig(fig, bbox_inches='tight')
    plt.show()
    plt.close()
    
    # df_response_workflow 
    fig,ax =plt.subplots(figsize=(12,4))
    fig.suptitle('Report for workflow level metrics', fontsize=12,x= 0.52,y=0.76,fontweight='bold')
    ax.axis('tight')
    ax.axis('off')
    the_table = ax.table(cellText=df_response_workflow.values,
                         colLabels=df_response_workflow.columns,
                         loc='center')
    for (row, col), cell in the_table.get_celld().items():
        if (row == 0) or (col == -1):
            cell.set_text_props(fontproperties=FontProperties(weight='bold'))
        if (col == 6 and (row==2 or row==6 or row==7 or row==8) ):
            cell.set_facecolor("red")

    export_pdf.savefig(fig)
    # export_pdf.savefig(fig, bbox_inches='tight')
    plt.show()
    plt.close()
    
    
    #################
     # df_response_worklow_det hist
    #fig,ax =plt.subplots(figsize=(6,4))
    names = list(df_response_workflow_temp["Workflow_Start_Time"])
    values = list(df_response_workflow_temp["Number_of_Tasks_Success"])
    values1 = list(df_response_workflow_temp["Number_of_Tasks_Failed"])
    fig, ax = plt.subplots(figsize=(6,4), sharey=True)
    fig.suptitle('Bar Chart Successful runs vs. Date', fontsize=12,x= 0.55,y=0.95,fontweight='bold')
    #plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    ax.bar(names, values,color='green')
    #axs[2].plot(names, values)
    
    #fig.tight_layout()
    plt.ylabel('Number of successful Runs')
    plt.xlabel('Run Dates')
    export_pdf.savefig()
    plt.show()
    plt.close()
    ##################
     #################
     # df_response_worklow_det hist
    #fig,ax =plt.subplots(figsize=(6,4))
    names = list(df_response_workflow_temp["Workflow_Start_Time"])
    values = list(df_response_workflow_temp["Number_of_Tasks_Failed"])
    fig, ax = plt.subplots(figsize=(6, 4), sharey=True)
    fig.suptitle('Bar Chart Failure runs vs. Date', fontsize=12,x= 0.55,y=0.95,fontweight='bold')
    #plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    ax.bar(names, values,color='red')
    #axs[1].scatter(names, values)
    #axs[2].plot(names, values)
    #fig.tight_layout()
    plt.ylabel('Number of Failed Runs')
    plt.xlabel('Run Dates')
    export_pdf.savefig()
    plt.show()
    plt.close()
    ##################
    ##################
    names = list(df_response_worklow_det["Table_Name"])
    values = np.array(df_response_worklow_det["Time_Diff_Sec"])
    threshold = 200000.0
    above_threshold = np.maximum(values - threshold, 0)
    below_threshold = np.minimum(values, threshold)
    fig, ax = plt.subplots(figsize=(7, 5), sharey=True)
    ax.bar(names, below_threshold, 0.35, color="g")
    ax.bar(names, above_threshold, 0.35, color="r",bottom=below_threshold)
    plt.axhline(y=threshold,linewidth=1, color='red')
    fig.suptitle('Bar Chart of Run times for Tables vs Threshold', fontsize=12,x= 0.55,y=0.95,fontweight='bold')
    plt.ylabel('Run times')
    plt.xlabel('Tables')
    export_pdf.savefig()
    plt.show()
    plt.close()
    ############
    names = list(df_response_workflow["Workflow_Id"])
    values = np.array(df_response_workflow["Time_Diff_Sec"])
    threshold = 300.0
    above_threshold = np.maximum(values - threshold, 0)
    below_threshold = np.minimum(values, threshold)
    fig, ax = plt.subplots(figsize=(8, 8), sharey=True)
    ax.bar(names, below_threshold, 0.35, color="g")
    ax.bar(names, above_threshold, 0.35, color="r",bottom=below_threshold)
    plt.axhline(y=threshold,linewidth=1, color='red')
    fig.suptitle('Bar Chart of Run times for Workflow vs Threshold', fontsize=12,x= 0.55,y=0.95,fontweight='bold')
    plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.ylabel('Run times')
    plt.xlabel('Workflow id')
    export_pdf.savefig()
    plt.show()
    plt.close()



file = open('/tmp/Charts.pdf', 'rb')

s3 = boto3.resource('s3')
bucket = s3.Bucket('s3_bucket_name')
response = bucket.put_object(
    Body=file,
    Key='s3_bucket_prefix/file.pdf',
    ContentType='pdf'
)




  
