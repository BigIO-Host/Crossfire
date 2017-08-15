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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.DateTime;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.LongValue;

public class DatastoreWriter extends DoFn<TableRow,Void> {
	private static final long serialVersionUID = 5322666434840501993L;
	private static transient final Object lock = new Object();
	  private static transient Datastore datastore;
	  private static transient KeyFactory keyFactoryGeneric;
	  String namespace;
	  String table;
	  
	  DatastoreWriter(String namespace, String table) {
		  this.namespace=namespace;
		  this.table=table;
	  }
	  
	  @StartBundle
	  public void startBundle() {
	    synchronized (lock) {
	      if (datastore == null) {
	    	  datastore = DatastoreOptions.getDefaultInstance().getService();
	    	  keyFactoryGeneric = datastore.newKeyFactory();
	      }
	    }
	  }
	  
	  @ProcessElement
	  public void processElement(ProcessContext c, BoundedWindow window) throws IOException {  
			TableRow row = c.element();
			DateTime d = window.maxTimestamp().toDateTime();
			
			KeyFactory keyFactory = keyFactoryGeneric.setKind(this.table.concat("-").concat((String) row.get("project"))).setNamespace(this.namespace);
			//keyFactory = datastore.newKeyFactory().kind(this.table).namespace(this.namespace);
			  
			String datestamp = String.format("%04d%02d%02d", d.getYear(),d.getMonthOfYear(),d.getDayOfMonth() );
			String keyString = (String)row.get("event")+"::"+datestamp;
			Key key = keyFactory.newKey(keyString);
			Entity e = Entity.newBuilder(key)
					.set("Total", LongValue.newBuilder((Integer)row.get("total")).setExcludeFromIndexes(true).build() )
					.set("Success", LongValue.newBuilder((Integer)row.get("success")).setExcludeFromIndexes(true).build() )
					.build();
		
			datastore.put(e);
			//hack for getting distribution
			String[] keyElements = keyString.split("::");
			if (keyElements.length == 3 && keyElements[0].equals("[AL]")) {
				String RecordType = "[OTH-DIST]";
				if (keyElements[1].startsWith("EVENT#")) {
					RecordType = "[EVT-DIST]";
				} else if (keyElements[1].startsWith("ATR#")) {
					RecordType = "[ATR-DIST]";
				}
				keyString = RecordType.concat("::").concat(keyElements[2]).concat("::").concat(keyElements[1]);
				key = keyFactory.newKey(keyString);
				e = Entity.newBuilder(key)
						.set("Total", LongValue.newBuilder((Integer)row.get("total")).setExcludeFromIndexes(true).build() )
						.set("Success", LongValue.newBuilder((Integer)row.get("success")).setExcludeFromIndexes(true).build() )
						.build();
				datastore.put(e);
			}
			//end of hack
	  }
}
