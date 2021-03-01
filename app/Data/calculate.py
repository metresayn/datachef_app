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
import pymongo
import json

file_path = "csv/"
duplicate_file_path = 'duplicates/'
spark = SparkSession.builder.appName('Banner_loader').getOrCreate()
#spark.conf.set("spark.sql.shuffle.partitions", 5)


def load_csvs(os_path, duplicate_file_path):

	"""
		This function reads the data from the csv paths and puts it in a spark dataframe.
	"""
	
	dataframe_list = []
    csv_paths = [f for f in os.listdir(os_path) if not f.startswith('.')]
    
    # Iterate over folder path to get the predefined csv file locations
    for folder in csv_paths:
        clicks_df = pd.read_csv(os_path+folder+'/'+'clicks'+'_'+folder+'.csv')
        cldf= spark.createDataFrame(clicks_df.astype(str))
        impressions_df = pd.read_csv(os_path+folder+'/'+'impressions'+'_'+folder+'.csv')
        idf= spark.createDataFrame(impressions_df.astype(str))
        conversions_df = pd.read_csv(os_path+folder+'/'+'conversions'+'_'+folder+'.csv')
        codf = spark.createDataFrame(conversions_df.astype(str))
        
        # duplicate_values_check. If duplicates are found, log it in the duplicates folder.
        val_idf = idf.join(
            idf.groupBy(idf.columns).agg((
                f.count("*")>1).cast("int").alias("Duplicate_indicator")), on=idf.columns, how="inner")
        df1 = val_idf.where("Duplicate_indicator > 0")
        df1.toPandas().to_csv(duplicate_file_path+folder+'/'+'impressions_duplicates'+'_'+folder+'.csv')
        
        val_cldf = cldf.join(
            cldf.groupBy(cldf.columns).agg((
                f.count("*")>1).cast("int").alias("Duplicate_indicator")), on=cldf.columns, how="inner")
        df2 = val_cldf.where("Duplicate_indicator > 0")
        df2.toPandas().to_csv(duplicate_file_path+folder+'/'+'clicks_duplicates'+'_'+folder+'.csv')
        
        val_codf = codf.join(
            codf.groupBy(codf.columns).agg((
                f.count("*")>1).cast("int").alias("Duplicate_indicator")), on=codf.columns, how="inner")
        df3 = val_codf.where("Duplicate_indicator > 0")
        df3.toPandas().to_csv(duplicate_file_path+folder+'/'+'conversion_duplicates'+'_'+folder+'.csv')
        
        # drop_duplicates
        cldf = cldf.dropDuplicates()
        idf = idf.dropDuplicates()
        codf = codf.dropDuplicates()
        
        # merge the three dataframes together based on key columns
        clicks_and_impressions = idf.join(cldf,['banner_id','campaign_id'],"inner")
        merged_df = clicks_and_impressions.join(codf, ['click_id'], "left")
        merged_df = merged_df.withColumn('Time_Quarter', lit(int(folder)))
        dataframe_list.append(merged_df)
    
    #reduce by union over all rows in the dataframe
    df_complete = reduce(DataFrame.unionAll, dataframe_list)
    #df_complete.show()

    return df_complete



def create_banner_campaign_dataframe(merged_df, toq):


	"""
		This function performs the calculations and puts it in a dataframe,
		(campaign_id, time_of_hour, set_of_banners). The campaign_id is self explanatory,
		time_of_hour ranges from (1, 4) depending on when the user logged in to view (
		also the structure of the csv file directory gives the same information),
		set_of_banners refers to the output banner set to be shown to the user depending
		on clicks and revenues.
	"""
	
	campaigns = merged_df.select('campaign_id').distinct().rdd.map(lambda r: r[0]).collect()
    revenue_sum_banner_campaign = merged_df.groupby(
        'campaign_id','banner_id','Time_Quarter').agg(f.sum('revenue').alias(
        'revenue_sums'))
    click_count_banner_campaign = merged_df.groupby(
        'campaign_id','banner_id','Time_Quarter').agg(countDistinct("click_id").alias(
        'clicks_count'))
    resultant_df = pd.DataFrame(columns=('campaign_id','time_quarter', 'list_of_banners'))
    
    
    # for each unique campaign find the set of banners to be rendered
    counter = 0
    for cmpgn in campaigns:
        cmpgn_revenue = revenue_sum_banner_campaign.filter(
            (revenue_sum_banner_campaign.campaign_id == cmpgn) & (
                revenue_sum_banner_campaign.Time_Quarter == toq) & (
            revenue_sum_banner_campaign.revenue_sums.isNotNull())).orderBy(
        ['revenue_sums'], ascending=False).select(col('banner_id')).rdd.map(
        lambda r: r[0]).collect()
        
        if len(cmpgn_revenue) >= 10:
            output_banners = cmpgn_revenue[:10]
            
        elif len(cmpgn_revenue) > 0 and len(cmpgn_revenue) < 10:
            output_banners = cmpgn_revenue
        
        else:
            cmpgn_clicks = click_count_banner_campaign.filter(
            (click_count_banner_campaign.campaign_id == cmpgn) & (
                click_count_banner_campaign.Time_Quarter == toq) & (
            revenue_sum_banner_campaign.revenue_sums.isNull())).orderBy(
        ['clicks_count'], ascending=False).select(col('banner_id')).rdd.map(
        lambda r: r[0]).collect()
            output_banners = cmpgn_clicks[:5]
        resultant_df.loc[counter]= [cmpgn, toq, output_banners]
        counter += 1

    return resultant_df

def main():

	"""
		Main function to call above functions and generate a json with output data. The 
		json generated is loaded to a mongodb instance.
    """

    merged_df = load_csvs(file_path, duplicate_file_path)
    df_list = []
    for time_quarter in range(1,5):
        df_list.append(create_banner_campaign_dataframe(merged_df, str(time_quarter)))
    resultant_df = pd.concat(df_list)
    resultant_df.to_json('Load_in_database/results.json',orient ='records')
    
    connection = pymongo.MongoClient()
    db = connection['user_data']
    collection_banners = db['banners']

    with open('Load_in_database/results.json') as f:
        file_data = json.load(f)
    collection_banners.insert(file_data)

    return 1

if __name__ == "__main__":
    main()