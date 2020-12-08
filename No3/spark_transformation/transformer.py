from pyspark import SparkContext
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from kafka_writer import send_to_kafka


if __name__ == "__main__":
    sc = SparkContext()
    spark = SparkSession.builder.getOrCreate()
    donation = spark.read.csv('/resources/donations.csv', header=True)

    donation_cleaned = donation.withColumn('created', f.from_unixtime('created').cast('date'))
    donation_cleaned = donation_cleaned.withColumn('updated', f.from_unixtime('updated').cast('date'))
    donation_cleaned.createOrReplaceTempView("donation")

    campaign = spark.read.csv('/resources/campaign.csv', header=True)
    campaign_cleaned = campaign.withColumn('created', f.from_unixtime('created').cast('date'))
    campaign_cleaned = campaign_cleaned.withColumn('updated', f.from_unixtime('updated').cast('date'))
    campaign_cleaned.createOrReplaceTempView("campaign")

    ads_spent = spark.read.csv('/resources/ads_spent.csv', header=True)
    ads_spent.createOrReplaceTempView("ads_spent")

    visit = spark.read.csv('/resources/visit.csv', header=True)
    visit.createOrReplaceTempView("visit")

    final_calculation= spark.sql("SELECT created_dt,campaign_id,title,URL,donation_amount,total_donation,ads_spending,total_pageview,(total_donation/total_pageview)*100 AS convertion_rate,(ads_spending/donation_amount)* 100 AS spending_per_donation_rate FROM (SELECT don.created_dt,campaign_id,title,don.URL,donation_amount,total_donation, SUM(spend) OVER(PARTITION BY don.created_dt,campaign_id) AS ads_spending, SUM(V.pageview) OVER(PARTITION BY don.created_dt,campaign_id) AS total_pageview FROM (SELECT date_trunc('day', D.created) AS created_dt,D.campaign_id,C.title,C.URL, SUM(D.amount) OVER (PARTITION BY date_trunc('day', D.created),campaign_id) AS donation_amount, COUNT(D.user_id) OVER (PARTITION BY date_trunc('day', D.created),campaign_id) AS total_donation FROM donation D  INNER JOIN campaign C ON D.campaign_id = C.ID) don LEFT JOIN ads_spent ads ON don.created_dt = ads.date_id AND don.URL = ads.short_url LEFT JOIN visit V ON don.created_dt = V.date_id AND don.URL = V.campaign_url ORDER BY 1,2) FINAL GROUP BY 1,2,3,4,5,6,7,8 ORDER BY 9;")

    final_calculation.foreachPartition(send_to_kafka)
