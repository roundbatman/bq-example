package org.github.roundbatman.bq;

import org.junit.Ignore;
import org.junit.Test;

public class BigQueryClientTest {

	@Ignore
	@Test
	public void testBq() {
		UsedToBeGuiceModule bqFactory = new UsedToBeGuiceModule();
		BigQueryClient client = bqFactory.provideClient();
		int rows = -1;
		try {
			rows = client.asyncQueryAndFetchResults("SELECT * FROM [publicdata:samples.wikipedia] LIMIT 10000");
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Fetched " + rows + " rows");
	}

}
