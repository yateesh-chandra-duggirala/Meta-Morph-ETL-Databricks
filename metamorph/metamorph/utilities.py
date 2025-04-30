from datetime import timedelta,datetime,timezone,date
from email.mime.text import MIMEText
import pandas as pd
import pytz
from IPython.display import HTML
from pyspark.sql.functions import *
import smtplib
from pyspark.sql.window import *
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import logging

spark = SparkSession.builder.appName("GCS_to_Postgres") \
    .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar,/usr/local/airflow/jars/gcs-connector-hadoop3-latest.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .getOrCreate()
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/usr/local/airflow/jars/meta-morph-d-eng-pro-admin.json")
logging.info("Spark session Created")

def raptor_data_fetch(source,sql):
  
  if source.lower().strip() == "bd":
    dataframe  = spark.sql(sql)
    
  else : 
    raise Exception("Source not Supported")
    
  return dataframe


def send_alert_emails_html(subject,user_email,body):

    sender_email = "ece.operations01@gmail.com"
    sender_password = "qtyl axzc zpcr naix"

    html_message = MIMEText(body, 'html')
    html_message['Subject'] = subject
    html_message['From'] = sender_email
    html_message['To'] = user_email

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, user_email.split(","), html_message.as_string())

def email_results(overall_summary_df, col_mismatch_df, col_summary_df, src_extra_df, tgt_extra_df, output_table_name_suffix, email_address_list):
    # Define CSS for styling the email content
    email_styles = """
    <style>
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            border: 2px solid black;
            text-align: left;
            padding: 8px;
            font-size: 14px;
        }
        th {
            background-color: #d4ff00; /* Header background color */
            font-size: 16px;
            text-align: center;
            font-weight: bold;
        }
        td {
            background-color: #05f5f1;
        }
        tr:nth-child(even) {
            background-color: #05f5f1;
        }
        tr:nth-child(odd) {
            background-color: #05f5f1;
        }
        h2 {
            color: #333333;
        }
        .section-title {
            margin-top: 20px;
            font-size: 18px;
            color: #444444;
            font-weight: bold;
        }
    </style>

    """

    # Helper function to convert DataFrame to styled HTML
    def render_html_table(spark_df, limit=15):
        pandas_df = spark_df.limit(limit).toPandas()
        return pandas_df.style.hide_index().render()

    # Generate individual tables
    overall_summary_html = render_html_table(overall_summary_df)
    col_mismatch_df2 = col_mismatch_df.withColumn("row", row_number().over(
        Window.partitionBy("mismatch_column_name").orderBy(
            col("source_value").desc(), col("target_value").desc())))
    col_mismatch_html = render_html_table(col_mismatch_df2.filter("row=1").drop("row"))
    col_summary_df_new = col_summary_df.withColumn(
        "pct", regexp_replace(col("Percentage_Of_Mismatch"), "%", "").cast("double")).orderBy(desc("pct")).drop("pct")
    col_summary_html = render_html_table(col_summary_df_new)
    src_extra_html = render_html_table(src_extra_df)
    tgt_extra_html = render_html_table(tgt_extra_df)

    # Extract key information
    source_system = str(output_table_name_suffix.split("_", 1)[0])
    target_system = str(output_table_name_suffix.split("_", 3)[2])
    Dataset_Name = str(output_table_name_suffix.split("_", 4)[3])
    RunDate = str(output_table_name_suffix.split("_", 5)[4])

    # HTML structure for the email body
    mail_body = f"""
    <html>
      <head>{email_styles}</head>
      <body>
        <h2>Data Raptor Summary</h2>
        <p><b>Source System:</b> {source_system}</p>
        <p><b>Target System:</b> {target_system}</p>
        <p><b>Dataset Name:</b> {Dataset_Name}</p>
        <p><b>Run Date:</b> {RunDate}</p>

        <div class="section-title">Overall Summary</div>
        {overall_summary_html}

        <div class="section-title">Column Level Mismatch Summary</div>
        {col_mismatch_html}

        <div class="section-title">Column Level Mismatch Percentage Summary</div>
        {col_summary_html}

        <div class="section-title">Source Extra Records Sample</div>
        {src_extra_html}

        <div class="section-title">Target Extra Records Sample</div>
        {tgt_extra_html}
      </body>
    </html>
    """

    # Subject and recipient handling
    mail_to_list = email_address_list
    MST = pytz.timezone('US/Arizona')
    run_date = format(datetime.now(timezone.utc).astimezone(MST).strftime("%m-%d-%Y"))

    if overall_summary_df.count() > 0:
        mail_subject = f"DATA RAPTOR Summary for {output_table_name_suffix}"
    else:
        mail_subject = f"DATA RAPTOR FAILED for {output_table_name_suffix}. Run Date: [{run_date}]"
        mail_body = "<p>DATA RAPTOR FAILED</p>"

    # Call to send the email
    send_alert_emails_html(mail_subject, mail_to_list, mail_body)

current_timestamp = datetime.now()
formatted_timestamp = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")

def raptor_result_summary(validateData,source,target,uniqueKeyColumns,output_table_name_suffix):
  
  print("Printing Summary ")
  
  source_count = source.count()
  target_count = target.count()
  compared_rec_count = source.join(target,uniqueKeyColumns).count()
  mismatch_rec_count = validateData.count()
  target_missing_rec_count= source.join(target,uniqueKeyColumns,"left").filter("Target_Record is null").count()
  source_missing_rec_count = source.join(target,uniqueKeyColumns,"right").filter("Source_Record is null").count()
  
  source_system=str(output_table_name_suffix.split("_",1)[0])
  target_system=str(output_table_name_suffix.split("_",3)[2])
  Dataset_Name=str(output_table_name_suffix)
  RunDate=str(output_table_name_suffix.split("_",5)[4])
  
  columns = ['Description', 'Value']
  data = []
  print(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Matched on: " +str(uniqueKeyColumns) )
  
  data.append(("Source System Name  ",str(source_system)))
  data.append(("Target System Name  ",str(target_system)))
  data.append(("DataSet Compared b/w Source & Target  ",str(Dataset_Name)))
  data.append(("Primary Keys used to Compare b/w Source & Target  ",str(uniqueKeyColumns)))
  
  print(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Number of rows in Source ["+source_system+"] "+str("{:,}".format(source_count)))
  data.append(("Number of rows in Source ["+source_system+"]",str("{:,}".format(source_count))))
  
  print(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Number of rows in Target ["+target_system+"] "+str("{:,}".format(target_count)))
  data.append(("Number of rows in Target ["+target_system+"]",str("{:,}".format(target_count))))
  
  print(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Number of rows in common "+str("{:,}".format(compared_rec_count)))
  data.append(("Number of rows in common ",str("{:,}".format(compared_rec_count))))
  
  print(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Number of rows mismatch "+str("{:,}".format(mismatch_rec_count)))
  data.append(("Number of rows mismatch ",str("{:,}".format(mismatch_rec_count))))
  
  if(mismatch_rec_count != 0 ):
    print(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Row Mismatch Percentage "+str("{:.2%}".format(((mismatch_rec_count/compared_rec_count)))))
    data.append(("Row Mismatch Percentage ",str("{:.2%}".format(((mismatch_rec_count/compared_rec_count))))))
    
  print(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Number of rows in Source "+source_system+" but not in Target "+target_system+": "+str("{:,}".format(target_missing_rec_count)))
  data.append(("Number of rows in Source "+source_system+" but not in Target "+target_system,str("{:,}".format(target_missing_rec_count))))
  
  print(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Number of rows in Target "+target_system+" but not in Source "+source_system+"        : "+str("{:,}".format(source_missing_rec_count)))
  data.append(("Number of rows in Target "+target_system+" but not in Source "+source_system,str("{:,}".format(source_missing_rec_count))))
        
  data.append(("Column Level Mismatch DataSet            ",str("qa_work.raptor_dataset_col_level_"+output_table_name_suffix)))
  data.append(("Column Level Mismatch Percentage Summary ",str("qa_work.raptor_dataset_col_level_smry_"+output_table_name_suffix)))
  data.append(("Source Extra DataSet                     ",str("qa_work.raptor_dataset_src_extra_"+output_table_name_suffix)))
  data.append(("Target Extra DataSet                     ",str("qa_work.raptor_dataset_tgt_extra_"+output_table_name_suffix)))
  
  summary_df = spark.createDataFrame(data=data, schema = columns)
  return summary_df

def raptor_column_summary(source,target,uniqueKeyColumns,df,sourcetablename):
  
  df.createOrReplaceTempView("mismatch_table_output")
  
  compared_rec_count = source.join(target,uniqueKeyColumns).count()
  
  columnwise_mismatch_count = spark.sql("""
  select Mismatch_Column_Name
  ,count(*) as Mismatch_Record_Count_Column_Level 
  from mismatch_table_output group by 1""").withColumn("Percentage_Of_Mismatch",concat((col("Mismatch_Record_Count_Column_Level")/lit(compared_rec_count) * 100).cast("decimal(10,2)"),lit('%'))).orderBy(desc("Percentage_Of_Mismatch"))
  
  columnwise_mismatch_count.write.mode("overwrite").option("mergeSchema",True).saveAsTable("qa_work.raptor_dataset_col_level_smry_"+sourcetablename)  
  print(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Column Level Summary Written to table                          : " + "qa_work.raptor_dataset_col_level_smry_"+sourcetablename)
  return columnwise_mismatch_count

def submit_raptor_request(source_type,source_sql,target_type,target_sql,primary_key,email=None,output_table_name_suffix="test"):
  MST = pytz.timezone('US/Arizona')
  spark.conf.set("spark.sql.shuffle.partitions","auto")
  runDate = format(datetime.now(timezone.utc).astimezone(MST).strftime("%m%d%Y_%H%M%S"))
  
  output_table_name_suffix = source_type+ "_vs_" + target_type +  "_"+ output_table_name_suffix + "_"+ runDate
  
  sourceDF = raptor_data_fetch(source_type,source_sql)
  targetDF = raptor_data_fetch(target_type,target_sql)
  
  sourceDF.cache()
  targetDF.cache()
  
  uniqueKeyColumns = [x.strip() for x in primary_key.split(',')]
  
  col_list = sourceDF.columns
  sourceDF = sourceDF.select(*sourceDF.columns)
  sourceDF1 = sourceDF.select([col(c).cast("string") for c in sourceDF.columns]).na.fill('')
  targetDF = targetDF.select(*sourceDF.columns)
  targetDF1 =targetDF.select([col(c).cast("string") for c in targetDF.columns]).na.fill('')
  
  source = sourceDF1.withColumn('Source_Record',concat_ws("\u0001",*sourceDF.columns)).select(*uniqueKeyColumns,col("Source_Record"))
  target = targetDF1.withColumn('Target_Record',concat_ws("\u0001",*sourceDF.columns)).select(*uniqueKeyColumns,col("Target_Record"))
  
#   display(source)
#   display(target)
  
  validateData = source.join(target, uniqueKeyColumns).where("Source_Record != Target_Record").select(*uniqueKeyColumns,split(col("Source_Record"),"\u0001").alias("_2"),split(col("Target_Record"),"\u0001").alias("_3"))
#   display(validateData)
  transform_expr = "transform(_2, (x, i) -> struct(_2[i] as source_value, _3[i] as target_value, i+1 as index))"
  df=validateData.withColumn("merged_arrays", explode(expr(transform_expr))) \
        .withColumn("source_value", col("merged_arrays.source_value")) \
        .withColumn("target_value", col("merged_arrays.target_value")) \
        .withColumn("column_name_index", col("merged_arrays.index") ) \
    .drop("merged_arrays").drop("_3").drop("_2").filter("target_value!=source_value")

  df=df.withColumn("column_name",array([lit(i) for i in col_list]))
  col_mismatch_df=df.select(*uniqueKeyColumns,"source_value","target_value",element_at("column_name",col("column_name_index")).alias("mismatch_column_name"))
  
  col_mismatch_df.write.mode("overwrite").option("mergeSchema",True).saveAsTable("qa_work.raptor_dataset_col_level_"+output_table_name_suffix)
  
  print(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Data Written to table : " + "qa_work.raptor_dataset_col_level_"+output_table_name_suffix)
  
  source.join(target,uniqueKeyColumns,"left").filter("Target_Record is null").write.mode("overwrite").option("mergeSchema",True).saveAsTable("qa_work.raptor_dataset_src_extra_"+output_table_name_suffix)
  print(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Data Written to table : " + "qa_work.raptor_dataset_src_extra_"+output_table_name_suffix)
  
  source.join(target,uniqueKeyColumns,"right").filter("Source_Record is null").write.mode("overwrite").option("mergeSchema",True).saveAsTable("qa_work.raptor_dataset_tgt_extra_"+output_table_name_suffix)
  print(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Data Written to table : " + "qa_work.raptor_dataset_tgt_extra_"+output_table_name_suffix)
  
  overall_summary_df = raptor_result_summary(validateData,source,target,uniqueKeyColumns,output_table_name_suffix)
  
  col_summary_df = raptor_column_summary(source,target,uniqueKeyColumns,col_mismatch_df,output_table_name_suffix)
  
  src_extra_df=spark.read.table("qa_work.raptor_dataset_src_extra_"+output_table_name_suffix).drop("Source_Record","Target_Record").limit(5)
  tgt_extra_df=spark.read.table("qa_work.raptor_dataset_tgt_extra_"+output_table_name_suffix).drop("Source_Record","Target_Record").limit(5)
  
  email_results(overall_summary_df,col_mismatch_df,col_summary_df,src_extra_df,tgt_extra_df,output_table_name_suffix,email)
#   return col_mismatch_df