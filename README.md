# Tython
Motive behind this project is to generate a wear-house to performing analytics on entire dataset collected at a company. This helps us to merge all the different rds data (From many products) and make a conclusive decisions.

The way this works is it creates a replica/copy of data using the last snapshot at current time and then adds the data as differential sync so that the data is near to live with a configurable delays.

Below are the exact steps
- Create a rds instance using the last replica of mysql db
- Create a ec2 instace to configure a worker which will replicate the data after performing the requried hashing of PII(personal indentity information) data.
- kill the e2 instances
- Every designated time (preferably 1 hour) run a differential sync using `mysql binlogs`
