from utilities import *

class Raptor:

    def submit_raptor_request(self,source_type,source_sql,target_type,target_sql,primary_key,email=None,output_table_name_suffix="test"):

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
        