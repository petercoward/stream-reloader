# stream-reloader
Replay data from storage onto Kafka

### Usage
``` 
Usage: stream-reloader [options]
  
    --startDate <dd/MM/yyyy>
                             Date (format dd/MM/yyyy) from which to start reloading. Required property.
    --endDate <dd/MM/yyyy>   Date (format dd/MM/yyyy) of the end of the period to start reloading. Optional property, if not provided will default to today's date
    --granularity <value>    The granularity of the folder structure to load from. Can be 'years', 'months', 'days' or 'hours'.
    --partitionNames true/false
                             Indicates whether the source folders have date partition names i.e. /year=yyyy/month=MM/day=dd/
    --mode s3/local          The mode to run stream-reloader in. Options are: 's3', 'local'. Default value is 'local'
    --gzipped true/false     Indicates whether the files to reload are gzipped
    --events event1=v1|v2,event2=v2|v3|v4...
                             Events and their schema versions to be reloaded. Must be in the format 'event1=v1|v2,event2=v2|v3|v4' etc
    --sourceBucket <value>   The s3 bucket from which to reload events.
    --sourcePrefix <value>   The prefix this event is stored under
    --destination <value>    The topic name to reload events onto
    --brokerList <value>     The list of IPs of the kafka brokers to write to
```
