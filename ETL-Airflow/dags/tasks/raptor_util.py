# Import Libraries and modules
from datetime import datetime,timezone
from email.mime.text import MIMEText
import pytz
from pyspark.sql.functions import row_number, col, concat_ws, regexp_replace, explode, expr, desc, concat, array, lit, element_at, split
import smtplib
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import logging
import psycopg2
from datetime import datetime

# Create the spark session
spark = SparkSession.builder.appName("GCS_to_Postgres") \
    .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar,/usr/local/airflow/jars/gcs-connector-hadoop3-latest.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .getOrCreate()
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/usr/local/airflow/jars/meta-morph-d-eng-pro-admin.json")
logging.info("Spark session Created")
today = datetime.now().strftime("%Y%m%d")

# Define a function to read the data from the Postgres Database specified the query and database
def read_data(username, password, spark, database, query_or_table=None, is_query=False) :

    try :
        logging.info("Connecting to PostgreSQL database using JDBC driver...")
        dbtable_value = f"({query_or_table}) as subquery" if is_query else query_or_table
        df = spark.read.format("jdbc")\
            .option("url", f"jdbc:postgresql://host.docker.internal:5432/{database}")\
            .option("user", username)\
            .option("password", password)\
            .option("driver", "org.postgresql.Driver")\
            .option("dbtable", dbtable_value)\
            .load()
        logging.info(f"Retrieved Data...")
    except Exception as e:
        logging.error("An Exception occurred")
        raise e
    return df


def ensure_schema_exists(username, password, database, schema):
    try:
        conn = psycopg2.connect(
            dbname=database,
            user=username,
            password=password,
            host="host.docker.internal",
            port="5432"
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.close()
        conn.close()
    except Exception as e:
        logging.error("Error creating schema", exc_info=True)
        raise e

# This function is defined to Write the data into the PG Database
def write_into_table(username, password, database, table, data_frame):

    try : 
        ensure_schema_exists(username, password, database, "work")
        logging.info("Connecting to PostgreSQL database using JDBC driver...")
        logging.info(f"Established connection. Writing into {table}")
        df = data_frame.write.format("jdbc")\
            .option("url", "jdbc:postgresql://host.docker.internal:5432/meta_morph") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", f"{table}") \
            .option("user", username) \
            .option("password", password) \
            .mode("overwrite") \
            .save()
        logging.info(f"Successfully Written {data_frame.count()} records into Table : {table}")
    except Exception as e:
        logging.error("An Exception occurred")
        raise e
    return df

def get_gcs_data(reporting_file, sql):
    logging.info("Connecting with reporting location..!")
    df = spark.read.format("parquet") \
                .option('header', 'True') \
                .option('inferSchema','True') \
                .load(f'gs://reporting-legacy/{reporting_file}')
    logging.info("Connection established with the reporting bucket..")
    df.createOrReplaceTempView(f"{reporting_file}")
    logging.info("Returned the Data from reporting..")
    return spark.sql(sql.replace(f'reporting.{reporting_file}',reporting_file))

def write_into_gcs_data(df, work_location):
    logging.info("Connecting to raptor-work...!")
    df.write.mode("overwrite").parquet(f"gs://raptor-work/{today}/{work_location}")
    logging.info(f"successfully written into raptor-work")

def raptor_data_fetch(username, password, source,source_db, sql):
  
    if source.lower().strip() == "pg_admin":
        dataframe  = read_data(username, password, spark, source_db, sql, True)
    
    elif source.lower().strip() == "reporting":
        try : 
            table_name = sql.split('reporting.')[1].split(' ')[0].lower()
            if 'reporting.' in sql:
                dataframe = get_gcs_data(table_name, sql)
            else : 
                raise Exception("Reporting data does not exist ..!")
        except Exception as e :
            logging.error(e)
        
    else : 
        raise Exception(f"Source ({source}) not Supported")
        
    return dataframe


def send_alert_emails_html(subject,user_email,body):

    sender_email = "ece.operations01@gmail.com"
    sender_password = "qtyl axzc zpcr naix"

    user_email = 'yateed1437@gmail.com,'+user_email
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
            background-color: #fffd75; /* Header background color */
            font-size: 16px;
            text-align: center;
            font-weight: bold;
        }
        td {
            background-color: #edffff;
        }
        tr:nth-child(even) {
            background-color: #edffff;
        }
        tr:nth-child(odd) {
            background-color: #edffff;
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
        pandas_df = pandas_df.reset_index(drop=True)
        return pandas_df.to_html(index=False)

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

    # HTML structure for the email body
    mail_body = f"""
    <html>
      <head>{email_styles}</head>
      <body>
        Hello,

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

  logging.info("Printing Summary ")
  
  source_count = source.count()
  target_count = target.count()
  compared_rec_count = source.join(target,uniqueKeyColumns).count()
  mismatch_rec_count = validateData.count()
  target_missing_rec_count= source.join(target,uniqueKeyColumns,"left").filter("Target_Record is null").count()
  source_missing_rec_count = source.join(target,uniqueKeyColumns,"right").filter("Source_Record is null").count()
  
  source_system=str(output_table_name_suffix.split("_",1)[0])
  target_system=str(output_table_name_suffix.split("_",3)[2])
  Dataset_Name=str(output_table_name_suffix)
  
  columns = ['Description', 'Value']
  data = []
  logging.info(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Matched on: " +str(uniqueKeyColumns) )
  
  data.append(("Source System Name  ",str(source_system)))
  data.append(("Target System Name  ",str(target_system)))
  data.append(("DataSet Compared b/w Source & Target  ",str(Dataset_Name)))
  data.append(("Primary Keys used to Compare b/w Source & Target  ",str(uniqueKeyColumns)))
  
  logging.info(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Number of rows in Source ["+source_system+"] "+str("{:,}".format(source_count)))
  data.append(("Number of rows in Source ["+source_system+"]",str("{:,}".format(source_count))))
  
  logging.info(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Number of rows in Target ["+target_system+"] "+str("{:,}".format(target_count)))
  data.append(("Number of rows in Target ["+target_system+"]",str("{:,}".format(target_count))))
  
  logging.info(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Number of rows in common "+str("{:,}".format(compared_rec_count)))
  data.append(("Number of rows in common ",str("{:,}".format(compared_rec_count))))
  
  logging.info(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Number of rows mismatch "+str("{:,}".format(mismatch_rec_count)))
  data.append(("Number of rows mismatch ",str("{:,}".format(mismatch_rec_count))))
  
  if(mismatch_rec_count != 0 ):
    logging.info(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Row Mismatch Percentage "+str("{:.2%}".format(((mismatch_rec_count/compared_rec_count)))))
    data.append(("Row Mismatch Percentage ",str("{:.2%}".format(((mismatch_rec_count/compared_rec_count))))))
    
  logging.info(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Number of rows in Source "+source_system+" but not in Target "+target_system+": "+str("{:,}".format(target_missing_rec_count)))
  data.append(("Number of rows in Source "+source_system+" but not in Target "+target_system,str("{:,}".format(target_missing_rec_count))))
  
  logging.info(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Number of rows in Target "+target_system+" but not in Source "+source_system+"        : "+str("{:,}".format(source_missing_rec_count)))
  data.append(("Number of rows in Target "+target_system+" but not in Source "+source_system,str("{:,}".format(source_missing_rec_count))))
        
  data.append(("Column Level Mismatch DataSet            ",str("work.raptor_dataset_col_level_"+output_table_name_suffix)))
  data.append(("Column Level Mismatch Percentage Summary ",str("work.raptor_dataset_col_level_smry_"+output_table_name_suffix)))
  data.append(("Source Extra DataSet                     ",str("work.raptor_dataset_src_extra_"+output_table_name_suffix)))
  data.append(("Target Extra DataSet                     ",str("work.raptor_dataset_tgt_extra_"+output_table_name_suffix)))
  
  summary_df = spark.createDataFrame(data=data, schema = columns)
  return summary_df

def raptor_column_summary(username, password, database, source,target,uniqueKeyColumns,df,sourcetablename):
  
  df.createOrReplaceTempView("mismatch_table_output")
  
  compared_rec_count = source.join(target,uniqueKeyColumns).count()
  
  columnwise_mismatch_count = spark.sql("""
  select Mismatch_Column_Name
  ,count(*) as Mismatch_Record_Count_Column_Level 
  from mismatch_table_output group by 1""").withColumn("Percentage_Of_Mismatch",concat((col("Mismatch_Record_Count_Column_Level")/lit(compared_rec_count) * 100).cast("decimal(10,2)"),lit('%'))).orderBy(desc("Percentage_Of_Mismatch"))
  
  write_into_gcs_data(columnwise_mismatch_count, "work.raptor_dataset_col_level_smry_"+sourcetablename)
  write_into_table(username, password, database, "work.raptor_dataset_col_level_smry_"+sourcetablename,columnwise_mismatch_count) 
  logging.info(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Column Level Summary Written to table                          : " + "work.raptor_dataset_col_level_smry_"+sourcetablename)
  return columnwise_mismatch_count

class Raptor:

    def __init__(self, username, password, email=None):
        self.username = username
        self.password = password
        self.email = email

    def submit_raptor_request(self,source_type,source_sql,target_type,target_sql,primary_key,source_db=None,target_db=None,email=None,output_table_name_suffix="test"):
        MST = pytz.timezone('US/Arizona')
        runDate = format(datetime.now(timezone.utc).astimezone(MST).strftime("%m%d%Y_%H%M%S"))
        
        output_table_name_suffix = source_type+ "_vs_" + target_type +  "_"+ output_table_name_suffix + "_"+ runDate
        
        sourceDF = raptor_data_fetch(self.username, self.password, source_type,source_db,source_sql)
        targetDF = raptor_data_fetch(self.username, self.password, target_type,target_db,target_sql)
        
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
        
        validateData = source.join(target, uniqueKeyColumns).where("Source_Record != Target_Record").select(*uniqueKeyColumns,split(col("Source_Record"),"\u0001").alias("_2"),split(col("Target_Record"),"\u0001").alias("_3"))

        transform_expr = "transform(_2, (x, i) -> struct(_2[i] as source_value, _3[i] as target_value, i+1 as index))"
        df=validateData.withColumn("merged_arrays", explode(expr(transform_expr))) \
                .withColumn("source_value", col("merged_arrays.source_value")) \
                .withColumn("target_value", col("merged_arrays.target_value")) \
                .withColumn("column_name_index", col("merged_arrays.index") ) \
            .drop("merged_arrays").drop("_3").drop("_2").filter("target_value!=source_value")

        df=df.withColumn("column_name",array([lit(i) for i in col_list]))
        col_mismatch_df=df.select(*uniqueKeyColumns,"source_value","target_value",element_at("column_name",col("column_name_index")).alias("mismatch_column_name"))

        write_into_gcs_data(col_mismatch_df, "work.raptor_dataset_col_level_"+output_table_name_suffix)
        write_into_table(self.username, self.password, source_db, "work.raptor_dataset_col_level_"+output_table_name_suffix, col_mismatch_df)
        logging.info(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Data Written to table : " + "work.raptor_dataset_col_level_"+output_table_name_suffix)

        write_into_gcs_data(source.join(target,uniqueKeyColumns,"left").filter("Target_Record is null"), "work.raptor_dataset_src_extra_"+output_table_name_suffix)
        write_into_table(self.username, self.password, source_db, "work.raptor_dataset_src_extra_"+output_table_name_suffix, source.join(target,uniqueKeyColumns,"left").filter("Target_Record is null"))
        logging.info(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Data Written to table : " + "work.raptor_dataset_src_extra_"+output_table_name_suffix)
        
        write_into_gcs_data(source.join(target,uniqueKeyColumns,"right").filter("Source_Record is null"), "work.raptor_dataset_tgt_extra_"+output_table_name_suffix)
        write_into_table(self.username, self.password, source_db, "work.raptor_dataset_tgt_extra_"+output_table_name_suffix, source.join(target,uniqueKeyColumns,"right").filter("Source_Record is null"))
        logging.info(str(current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))+" Data Written to table : " + "work.raptor_dataset_tgt_extra_"+output_table_name_suffix)
        
        overall_summary_df = raptor_result_summary(validateData,source,target,uniqueKeyColumns,output_table_name_suffix)
        
        col_summary_df = raptor_column_summary(self.username, self.password, source_db, source,target,uniqueKeyColumns,col_mismatch_df,output_table_name_suffix)

        src_extra_df=read_data(self.username, self.password,spark,source_db,"work.raptor_dataset_src_extra_"+output_table_name_suffix, False).drop("Source_Record","Target_Record").limit(5)
        tgt_extra_df=read_data(self.username, self.password,spark,source_db,"work.raptor_dataset_tgt_extra_"+output_table_name_suffix, False).drop("Source_Record","Target_Record").limit(5)
        
        email_results(overall_summary_df,col_mismatch_df,col_summary_df,src_extra_df,tgt_extra_df,output_table_name_suffix,email)