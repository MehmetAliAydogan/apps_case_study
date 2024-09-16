# APPS CASE STUDY
For APPS Data Engineer Interview Case Study

- I have used api_events.py and spark to look and get the columns and example data.
From here we can see the common dimensions.

- We can see that all 3 of them has a date or a timestamp column which we can use to group. We will transform all to '%yyyy-%mm-%dd' format.

- Events table have 5 different event types which we only need the 'AdImpression' and 'GameStart' event names.

- Then we can see that costs and installs have campaign name and network name dimension in common. Those will be grouped and joined together.

- Also we can events and installs have user_id in common which means we can join them on user_id and have each users campaign and network name and install_date along with their impression and level events in a single table.

- Then we can aggregate using install_date, network_name, campaign_name and get the KPI's given in the Example-1 Table in Case Study pdf.

### Notes

-  For this test case considering the data size and ease of use I have used Python and pandas for the computing of intermediate dataframes/tables within Airflow DAG but it is not the best practive to use Airflow DAG's for computing instead using external computing is the best like spark/bigquery/postgres.

- In order to run the in Aİrflow DAG there are 2 requirements:

1.  You need requests and pandas libraries installed on the Airflow. If you are using Docker you can install requirements.txt using Dockerfile. 
2.  If you are running airflow within a container have '/output' folder available for final and intermediate tables. For the '/output' folder, I have attached local volume to the container using volumes section inside docker-compose file like "- ./output:/output"
  
   
# Detailed ETL Process

1. fetch installs events and cost data from the API

2. Change the date column type and change format to '%yyyy-%mm-%dd' for all data.

3. Rename the installed_at column to installed_date and Fix the network name spellings according to the cost table.

4. Filter 'AdImpression', 'GameStart' events from the events table.

5. Rename event_ts column to event_date.

6. Join the filtered events table with install table using user_id.With this we are able to know installed_date, campaign_name and network_name for each user in events table.

7. Calculate the install_count for each installed_date, network_name and campaign name using install data.

8. Group the joined table by installed_date, network_name and campaign_name. When grouping calculate the game_start_count by counting the rows with Game_Start event for each dimension. Calculate the banner, interstitial, rewarded revenue by summing the ad_revenue column when event_name is AdImpression and ad_type is  banner, interstitial, rewarded respectively.

9. Then to get the cost for each date, network_name and campaign_name use cost table and sum the cost column.

10. Use all install_grouped, events_joined_grouped and cost_grouped tables that we have grouped by date, network_name and campaign_name. Join the cost_grouped and install_grouped table to events_joined_grouped using installed_date-date, network_name, campaign_name columns and select the required columns for the final table.

11. Fill the null/Nan cost and install counts with 0.

12. Write the final table to '/output/final_table.csv'

