# apps_case_study
For APPS Data Engineer Interview Case Study

- I have used api_events.py and spark to look and get the columns and example data.
From here we can see the common dimensions.

- We can see that all 3 of them has a date or a timestamp column which we can use to group. We will transform all to '%yyyy-%mm-%dd' format.

- Events table have 5 different event types which we only need the 'AdImpression' and 'GameStart' event names.

- Then we can see that costs and installs have campaign name and network name dimension in common. Those will be grouped and joined together.

- Also we can events and installs have user_id in common which means we can join them on user_id and have each users campaign and network name and install_date along with their impression and level events in a single table.

- Then we can aggregate using install_date, network_name, campaign_name and get the KPI's given in the Example-1 Table in Case Study pdf.