/*
 * Copyright 2017 BigIO.host. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package host.bigio.ml;

import java.io.IOException;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.google.api.services.bigquery.model.TableRow;

public class PredCount extends DoFn<KV<String,Iterable<Boolean>>,TableRow> { 
	private static final long serialVersionUID = -3373646006560761707L;

	@ProcessElement
	  public void processElement(ProcessContext c) throws IOException {
		  int succ_count=0;
		  int total_count=0;
		  for(Boolean d : c.element().getValue() ) {
			  if(d)
				  succ_count++;
			  total_count++;
		  }
		  
		  String[] temp = c.element().getKey().split("\\|");
		  String project = temp[0];
		  String event = temp[1];
		  
		  TableRow output = new TableRow();
		  output.set("project", project );
		  output.set("event", event );
		  output.set("success", succ_count);
		  output.set("total", total_count);

		  c.output(output);
		  
	  }
}
