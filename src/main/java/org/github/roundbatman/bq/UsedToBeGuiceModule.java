package org.github.roundbatman.bq;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;

public class UsedToBeGuiceModule {
	private static final Logger LOG = Logger.getLogger(UsedToBeGuiceModule.class.getName());
	private static final String BQ_SERVICE_ACCOUNT = "";
	private static final String GCP_PROJECT = "";
	private static final List<String> BIG_QUERY_SCOPES = Arrays.asList(BigqueryScopes.BIGQUERY);
	private static final URL SECRETS_URI = BigQueryClient.class.getResource("/key.p12");

	public BigQueryClient provideClient() {
		return new BigQueryClient(provideBigquery(), GCP_PROJECT);
	}

	public Bigquery provideBigquery() {
		HttpTransport transport = new NetHttpTransport();
		JsonFactory jsonFactory = new JacksonFactory();
		Bigquery bigQuery = null;
		try {
			bigQuery = createAuthorizedClient(transport, jsonFactory);
		} catch (IOException | InterruptedException | GeneralSecurityException
				| URISyntaxException e) {
			LOG.severe("Unable to construct Bigquery: " + e.getMessage());
		}

		return bigQuery;
	}

	private Bigquery createAuthorizedClient(HttpTransport transport, JsonFactory jsonFactory) throws IOException, InterruptedException, GeneralSecurityException, URISyntaxException {
		GoogleCredential credential = authorize(transport, jsonFactory);
		//		GoogleCredential credential = GoogleCredential.getApplicationDefault(httpTransport, jsonFactory).createScoped(scopes);
		System.out.println(credential.getAccessToken());
		System.out.println(credential.getRefreshToken());
		System.out.println(credential.getServiceAccountScopes());
		return new Bigquery.Builder(transport, jsonFactory, credential)
		.setApplicationName("telematics-data-api")
		.build();
	}

	/** Authorizes the installed application to access user's protected data. 
	 * @param jsonFactory 
	 * @param transport 
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws GeneralSecurityException 
	 * @throws URISyntaxException */
	private GoogleCredential authorize(HttpTransport transport, JsonFactory jsonFactory) throws IOException, InterruptedException, GeneralSecurityException, URISyntaxException  {
		LOG.info("Building Google credentials");
		
		GoogleCredential credential = new GoogleCredential.Builder()
		.setTransport(transport)
		.setJsonFactory(jsonFactory)
		.setServiceAccountId(BQ_SERVICE_ACCOUNT)
		.setServiceAccountScopes(BIG_QUERY_SCOPES)
		.setServiceAccountPrivateKeyFromP12File(new File(SECRETS_URI.toURI()))
		.build();
		LOG.info("Google credential building finished");
		return credential;
	}	
}

