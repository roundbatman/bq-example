## Testing with App Engine and Bigquery

Before starting add required stuff to src/main/java/org/github/roundbatman/bq/UsedToBeGuiceModule.java

    private static final String BQ_SERVICE_ACCOUNT = "";
    private static final String GCP_PROJECT = "";

And replace src/main/resources/key.p12 with a proper file.

Start App Engine on local machine and access http://localhost:8080 for sample query

    mvn appengine:devserver

Standalone test case can be found from file BigQueryClientTest.java