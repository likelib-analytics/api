# Likelib API



## Explorer API 

Routes:
* `/api/v1/explorer/_transactions` 
* `/api/v1/explorer/_blocks` 

Optional parameters: 

* `limit` - define number of blocks/transactions
* `offset` - define records' shift 

## Metrics API

Routes:
* `/api/v1/analytics/_metric` 

Parameters: 

* `from_timestamp` 
* `to_timestamp` 
* `interval` - data granularity `1m` | `1h` | `1d` 
* `metric_name` - a metric from currently available mertics list
* `mode` - demo | live

Timestamps' format: `%Y-%m-%dT%H:%M:%S` 
Mertics:
* `transactions`
* `blocks`
* `active_addresses`
* `transaction_volume`
* `sending_addresses`
* `receiving_addresses` 

## Search API 

Routes:
* `/api/v1/_search` - get item by substring
* `/api/v1/_search_detailed` - get detailed information about current item

Parameters: 

* `search` - search query, string
* `search_type` - type of data: `address`, `transactions`, `blocks`
