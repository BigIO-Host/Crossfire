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
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.google.api.services.bigquery.model.TableRow;

public class SessionExtract extends DoFn<TableRow, KV<String,TableRow>> {
	private static final long serialVersionUID = 3287595076795743977L;
	Map<String, SessionConf> sessionTags;
	  
	  SessionExtract(Map<String, SessionConf> sessionTags) {
		  this.sessionTags = sessionTags;
	  }
	    	
	    @ProcessElement
	    public void processElement(ProcessContext c) throws IOException {
	      TableRow row = c.element();
	      String ssnId=null;
	      try {
	    	  //ssnId = (String) row.get("id"); //this match to the json input case-sensitive
	    	  ssnId = row.get("projectid").toString().concat("::").concat((String) row.get("id"));
	      } catch (Exception e) {
	      }
	      	      
	      String sessionConf;
	      try {
	    	  sessionConf = (String) row.get("sessionConf");
	      } catch (Exception e) {
	    	  sessionConf = null;
	      }
	      
	      // reduce log
	      //LOG.info("data:"+row.toPrettyString()+" timestamp:"+c.timestamp().toString()+" sessionWindow:"+sessionWindow);
	      
	      SessionConf tag = null;
	      
	      if (ssnId != null) {
	    	  if (sessionConf != null) {
	    		  tag = sessionTags.get(sessionConf);
	    	  }

  		  if (tag != null) {
  			  c.output(tag.tupleTag, KV.of(ssnId, row));
  		  } else {
  			  c.output(KV.of(ssnId, row));
  		  }
	    	  
	      }
	    }
}
