# Week-01 Homework



## Question 1. Knowing docker tags



Which tag has the following text? - *Automatically remove the container when it exits* 

- `--delete`
- `--rc`
- `--rmc`
- `--rm`

>  Answer :  --rm

```bash
docker run --help
```


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

- 0.42.0
- 1.0.0
- 23.0.1
- 58.1.0

>  Answer :  0.42.0

```bash
docker run -it python:3.9 bash

pip list
```

## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 15767
- 15612
- 15859
- 89009

>  Answer :  15612

```sql
select count(*) 
from green_taxi_data 
where 
date_trunc('day',lpep_pickup_datetime)='2019-09-18' 
and 
date_trunc('day',lpep_dropoff_datetime)='2019-09-18' 
```


## Question 4. Largest trip for each day

Which was the pick up day with the largest trip distance
Use the pick up time for your calculations.

- 2019-09-18
- 2019-09-16
- 2019-09-26
- 2019-09-21

>  Answer :  2019-09-26

```sql
select 
date_trunc('day',lpep_pickup_datetime),
max(trip_distance) as Max_Trip_Distance 
from green_taxi_data 
group by date_trunc('day',lpep_pickup_datetime)
order by Max_Trip_Distance desc 
limit 1
```


## Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
 
- "Brooklyn" "Manhattan" "Queens"
- "Bronx" "Brooklyn" "Manhattan"
- "Bronx" "Manhattan" "Queens" 
- "Brooklyn" "Queens" "Staten Island"

>  Answer :  "Brooklyn" "Manhattan" "Queens"

```sql
select 
"Borough",
sum(total_amount) as amount  
from green_taxi_data as A 
left join taxi_zones as B on A."PULocationID"=B."LocationID" 
where date_trunc('day',lpep_pickup_datetime)='2019-09-18' 
group by "Borough" 
having sum(total_amount)>50000 
order by amount desc  
limit 3
```


## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- JFK Airport
- Long Island City/Queens Plaza

>  Answer :  JFK Airport

```sql
SELECT C."Zone" DOZone,
max(tip_amount) Tip
FROM public.green_taxi_data A 
left outer join public.taxi_zones B on a."PULocationID"=b."LocationID"
left outer join public.taxi_zones C on a."DOLocationID"=c."LocationID"
where B."Zone"='Astoria'
group by C."Zone"
order by Tip desc
limit 1
```



## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

>  Answer :  Command output below


```bash
Terraform used the selected providers to generate the following execution plan. Resource actions are indicated
with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.demo_dataset will be created
  + resource "google_bigquery_dataset" "demo_dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "demo_dataset"
      + default_collation          = (known after apply)
      + delete_contents_on_destroy = false
      + effective_labels           = (known after apply)
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + is_case_insensitive        = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "US"
      + max_time_travel_hours      = (known after apply)
      + project                    = "de-zoomcamp-2024-gcp-412014"
      + self_link                  = (known after apply)
      + storage_billing_model      = (known after apply)
      + terraform_labels           = (known after apply)
    }

  # google_storage_bucket.demo-bucket will be created
  + resource "google_storage_bucket" "demo-bucket" {
      + effective_labels            = (known after apply)
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "US"
      + name                        = "de-zoomcamp-2024-gcp-412014-terra-bucket"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + terraform_labels            = (known after apply)
      + uniform_bucket_level_access = (known after apply)
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "AbortIncompleteMultipartUpload"
            }
          + condition {
              + age                   = 1
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_bigquery_dataset.demo_dataset: Creation complete after 1s [id=projects/de-zoomcamp-2024-gcp-412014/datasets/demo_dataset]
google_storage_bucket.demo-bucket: Creation complete after 1s [id=de-zoomcamp-2024-gcp-412014-terra-bucket]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.

```
