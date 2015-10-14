/*
 * Copyright (c) 2012 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.github.roundbatman.bq;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.logging.Logger;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs;
import com.google.api.services.bigquery.Bigquery.Jobs.GetQueryResults;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableRow;

/**
 * Example of authorizing with BigQuery and reading from a public dataset.
 */
public class BigQueryClient {
	private static final Logger LOG = Logger.getLogger(BigQueryClient.class.getName());

	private final String projectId;
	private Bigquery bigQuery;

	public BigQueryClient(Bigquery bigQuery, String projectId) {
		this.bigQuery = bigQuery;
		this.projectId = projectId;
	}


	public int asyncQueryAndFetchResults(String querySql) throws Exception {
		long queryStart = System.currentTimeMillis();
		List<TableRow> rows = null;
		try {
			JobReference job = startQuery(querySql);
			long queryEnd = System.currentTimeMillis();
			LOG.info("Query took (ms): " + (queryEnd - queryStart));
			Job completedJob = checkQueryResults(job);
			long jobEnd = System.currentTimeMillis();
			LOG.info("Job end took (ms): " + (jobEnd - queryStart));
			Jobs jobs = this.bigQuery.jobs();
			long jobsEnd = System.currentTimeMillis();
			LOG.info("Jobs end took (ms): " + (jobsEnd - queryStart));
			GetQueryResults results = jobs.getQueryResults(
					this.projectId, completedJob
					.getJobReference()
					.getJobId()
					);
			long resultsEnd = System.currentTimeMillis();
			LOG.info("Results end took (ms): " + (resultsEnd - queryStart));
			GetQueryResultsResponse response = results.execute();
			long fetchEnd = System.currentTimeMillis();
			LOG.info("Fetch took (ms): " + (fetchEnd - queryStart));
			return response.getRows().size();
		} catch (IOException e) {
			throw e;
		} catch (InterruptedException e) {
			throw e;
		}
	}

	/**
	 * Creates a Query Job for a particular query on a dataset
	 *
	 * @param bigquery  an authorized BigQuery client
	 * @param projectId a String containing the project ID
	 * @param querySql  the actual query string
	 * @return a reference to the inserted query job
	 * @throws IOException
	 */
	private JobReference startQuery(String querySql) throws IOException {
		Job job = new Job();
		JobConfiguration config = new JobConfiguration();
		JobConfigurationQuery queryConfig = new JobConfigurationQuery();
		config.setQuery(queryConfig);

		job.setConfiguration(config);
		queryConfig.setQuery(querySql);

		Insert insert = this.bigQuery.jobs().insert(this.projectId, job);
		insert.setProjectId(this.projectId);
		JobReference jobId = insert.execute().getJobReference();

		return jobId;
	}

	/**
	 * Polls the status of a BigQuery job, returns Job reference if "Done"
	 *
	 * @param bigquery  an authorized BigQuery client
	 * @param projectId a string containing the current project ID
	 * @param jobId     a reference to an inserted query Job
	 * @return a reference to the completed Job
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private Job checkQueryResults(JobReference jobId)
			throws IOException, InterruptedException {
		// Variables to keep track of total query time
		long startTime = System.currentTimeMillis();
		long elapsedTime;

		while (true) {
			Job pollJob = this.bigQuery.jobs().get(this.projectId, jobId.getJobId()).execute();
			elapsedTime = System.currentTimeMillis() - startTime;
			System.out.format("Job status (%dms) %s: %s\n", elapsedTime,
					jobId.getJobId(), pollJob.getStatus().getState());
			if (pollJob.getStatus().getState().equals("DONE")) {
				return pollJob;
			}
			// Pause execution before polling job status again, to
			// reduce unnecessary calls to the BigQUery API and lower overall
			// application bandwidth.
			Thread.sleep(500);
		}
	}
}
