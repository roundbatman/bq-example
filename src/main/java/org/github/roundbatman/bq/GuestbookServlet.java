/**
 * Copyright 2014-2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//[START all]
package org.github.roundbatman.bq;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class GuestbookServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5587173779695489932L;
	private final UsedToBeGuiceModule bqFactory = new UsedToBeGuiceModule();
	private final BigQueryClient client = bqFactory.provideClient();
		
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {

		int rows = -1;
		try {
			rows = client.asyncQueryAndFetchResults("SELECT * FROM [publicdata:samples.wikipedia] LIMIT 10000");
		} catch (Exception e) {
			e.printStackTrace();
		}
		resp.setContentType("text/plain");
		resp.getWriter().println("Fetched " + rows + " rows");
	}
}
