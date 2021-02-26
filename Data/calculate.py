import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pandas as pd
from pyspark.sql.functions import lit
from pyspark.sql import DataFrame
from functools import reduce
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import col
import os

file_path = "csv/"
spark = SparkSession.builder.appName('Banner_loader').getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 5)


def load_csvs(os_path):
    dataframe_list = []
    csv_paths = [f for f in os.listdir(os_path) if not f.startswith('.')]
    for folder in csv_paths:
        clicks_df = pd.read_csv(os_path+folder+'/'+'clicks'+'_'+folder+'.csv')
        cldf= spark.createDataFrame(clicks_df.astype(str))
        impressions_df = pd.read_csv(os_path+folder+'/'+'impressions'+'_'+folder+'.csv')
        idf= spark.createDataFrame(impressions_df.astype(str))
        conversions_df = pd.read_csv(os_path+folder+'/'+'conversions'+'_'+folder+'.csv')
        codf = spark.createDataFrame(conversions_df.astype(str))
        
        ##duplicate_values_check
        val_idf = idf.join(
            idf.groupBy(idf.columns).agg((
                f.count("*")>1).cast("int").alias("Duplicate_indicator")), on=idf.columns, how="inner")
        df1 = val_idf.where("Duplicate_indicator > 0")
        df1.toPandas().to_csv('duplicates/'+folder+'/'+'impressions_duplicates'+'_'+folder+'.csv')
        
        val_cldf = cldf.join(
            cldf.groupBy(cldf.columns).agg((
                f.count("*")>1).cast("int").alias("Duplicate_indicator")), on=cldf.columns, how="inner")
        df2 = val_cldf.where("Duplicate_indicator > 0")
        df2.toPandas().to_csv('duplicates/'+folder+'/'+'clicks_duplicates'+'_'+folder+'.csv')
        
        val_codf = codf.join(
            codf.groupBy(codf.columns).agg((
                f.count("*")>1).cast("int").alias("Duplicate_indicator")), on=codf.columns, how="inner")
        df3 = val_codf.where("Duplicate_indicator > 0")
        df3.toPandas().to_csv('duplicates/'+folder+'/'+'conversion_duplicates'+'_'+folder+'.csv')
        
        ##drop_duplicates
        cldf = cldf.dropDuplicates()
        idf = idf.dropDuplicates()
        codf = codf.dropDuplicates()
        
        ###
        clicks_and_impressions = idf.join(cldf,['banner_id','campaign_id'],"inner")
        merged_df = clicks_and_impressions.join(codf, ['click_id'], "left")
        merged_df = merged_df.withColumn('Time_Quarter', lit(int(folder)))
        dataframe_list.append(merged_df)
    
    df_complete = reduce(DataFrame.unionAll, dataframe_list)
    #df_complete.show()
    return df_complete



def create_banner_campaign_dataframe(merged_df, toq):
    campaigns = merged_df.select('campaign_id').distinct().rdd.map(lambda r: r[0]).collect()
    revenue_sum_banner_campaign = merged_df.groupby(
        'campaign_id','banner_id','Time_Quarter').agg(f.sum('revenue').alias('revenue_sums'))
    click_count_banner_campaign = merged_df.groupby(
        'campaign_id','banner_id','Time_Quarter').agg(countDistinct("click_id").alias('clicks_count'))
    resultant_df = pd.DataFrame(columns=('campaign_id','time_quarter', 'list_of_banners'))
    counter = 0
    for cmpgn in campaigns:
        cmpgn_revenue = revenue_sum_banner_campaign.filter(
            (revenue_sum_banner_campaign.campaign_id == cmpgn) & (
                revenue_sum_banner_campaign.Time_Quarter == toq) & (
            revenue_sum_banner_campaign.revenue_sums.isNotNull())).orderBy(
        ['revenue_sums'], ascending=False).select(col('banner_id')).rdd.map(lambda r: r[0]).collect()
        
        if len(cmpgn_revenue) >= 10:
            output_banners = cmpgn_revenue[:10]
            
        elif len(cmpgn_revenue) > 0 and len(cmpgn_revenue) < 10:
            output_banners = cmpgn_revenue
        
        else:
            cmpgn_clicks = click_count_banner_campaign.filter(
            (click_count_banner_campaign.campaign_id == cmpgn) & (
                click_count_banner_campaign.Time_Quarter == toq) & (
            revenue_sum_banner_campaign.revenue_sums.isNull())).orderBy(
        ['clicks_count'], ascending=False).select(col('banner_id')).rdd.map(lambda r: r[0]).collect()
            output_banners = cmpgn_clicks[:5]
        resultant_df.loc[counter]= [cmpgn, toq, output_banners]
        counter += 1
    return resultant_df

def main():
	merged_df = load_csvs(file_path)
	df_list = []
	for time_quarter in range(1,5):
		df_list.append(create_banner_campaign_dataframe(merged_df, str(time_quarter)))
	resultant_df = pd.concat(df_list)
	resultant_df.to_json('Load_in_database/results.json',orient ='records')

if __name__ == "__main__":
    main()
