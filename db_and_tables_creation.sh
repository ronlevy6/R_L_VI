aws glue create-database --database-input "{\"Name\":\"ron_levy_assignment_db\"}"

aws glue create-crawler --name ron-levy-crawler-cli --database-name ron_levy_assignment_db --targets S3Targets=[{Path="s3://ron-levy-bucket-for-vi/output/"}] --table-prefix ron_levy_assignment --role AWSGlueGeneralRule

aws glue start-crawler --name ron-levy-crawler-cli

aws athena start-query-execution --query-string "create or replace view most_traded_stock as (select * from  (select * from ron_levy_assignmentq2 order by frequency desc) limit 1);" --query-execution-context Database=ron_levy_assignment_db --result-configuration OutputLocation="s3://ron-levy-bucket-for-vi/athena-outputs/"

aws athena start-query-execution --query-string "create or replace view most_volatile as (select * from  (select * from ron_levy_assignmentq3 order by standard_deviation desc) limit 1);" --query-execution-context Database=ron_levy_assignment_db --result-configuration OutputLocation="s3://ron-levy-bucket-for-vi/athena-outputs/"

aws athena start-query-execution --query-string "create or replace view top_30_days_return as (select * from  (select * from ron_levy_assignmentq4 order by return_30_days desc) limit 3);" --query-execution-context Database=ron_levy_assignment_db --result-configuration OutputLocation="s3://ron-levy-bucket-for-vi/athena-outputs/"
