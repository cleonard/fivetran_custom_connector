# fivetran_custom_connector

Cleaning up a first pass from last year to create a custom Fivetran connector.

(Based on:
https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/quickstart_examples/weather)

The data source is NewsAPI.org (same as the example code above, though I'm querying for
different topics).

For fun and extra credit, I also integrated cognitive linguistic analysis from Tabula
Lingua, the now-defunct startup that I co-founded, per news article. Adding these values
could allow for things like:

- Track the ebb and flow of cognitive attributes like clarity, disfunction, etc. over
  time per topic.
- Cluster similar articles based on language
- Topic modeling based on subsets of articles (high clarity, low involvement, etc.)

![Snowflake query screenshot](/imgs/query.png)


