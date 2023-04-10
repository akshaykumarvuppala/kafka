#Find all the code, text files and snapshot of metrics above.

description on files:
  mysqlconnector- In this code it connects to the the database and inserts the records to the table created as 'mytable'.
  producer_code- In this code It reads the data from the table 'mytable' on mysql database and publishes into the kafka topic 'people_details_new'.
  consumer_code- In this code it will subscribe to the topic 'people_details_new' and consumes the data and will perform minor transformations to the data and finally places it into the new table called 'yourtable'.
  
schema text file is posted to understand how schema's value and key is defined on topic 'people_details_new' on cofluent kafka for the reference. there are also snaphots available for production and comsumption metrics on confluent kafka along with all other files for reference.
