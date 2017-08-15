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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.joda.time.Duration;

class SessionConf implements java.io.Serializable {
	
	private static final long serialVersionUID = 1L;
	
	TupleTag<KV<String,TableRow>> tupleTag;
	Duration windowSize;

	SessionConf(Duration windowSize) {
		this.windowSize = windowSize;
		this.tupleTag = new TupleTag<KV<String,TableRow>>() {
			private static final long serialVersionUID = 1L;
		};
	}
	
}

@SuppressWarnings("serial")
public class MainPipeline {

  private static Map<String, SessionConf> sessionConfs = new HashMap<>();
  private static Options options;
        
  public static void main(String[] args) {		
	    options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
	    parseSessionConf();
	    
		Pipeline p = Pipeline.create(options);
		
		
		sessionConversion(p, options);
			    
		p.run();
  }
  
  private static final ObjectMapper MAPPER =
		      new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
  
  private static void parseSessionConf() {
	    if( options.getSessionConfs() != null) {
	    	String[] tags = options.getSessionConfs().replaceAll("\\s+","").split(",");
	    	for (String tag : tags) {
	    		String[] parts = tag.split(":");
	    		if(parts.length == 2 ) {
		    		String name = parts[0];
		    		int windowSeconds = Integer.parseInt(parts[1]);
			    	sessionConfs.put(name, new SessionConf(Duration.standardSeconds(windowSeconds)) );
	    		}
	    	}
	    }	  
  }

  private static void sessionConversion(Pipeline p, Options options) {

		PCollection<String> pubsubRaw = p.apply(PubsubIO.readStrings().fromSubscription(options.getEventSubscription()).withTimestampAttribute("timestamp") );

	    // FIXME: Use Coder instead
	    PCollection<TableRow> streamData = pubsubRaw.apply("JSON decoder", ParDo.of(new DoFn<String, TableRow>() { 
	    		    	
	    	@ProcessElement
	    	public void processElement(ProcessContext c) throws IOException {
	    		String strJson = c.element();
	    		TableRow row = MAPPER.readValue(strJson, TableRow.class);
	    		c.output(row);
	    	}
	    	
	    } ) );
	    
	    String session_conf_default = options.getSessionConfDefault();

	    // Multiple Session window sizes
	    TupleTagList taglist = TupleTagList.empty();
	    
	    for(Entry<String, SessionConf> entry : sessionConfs.entrySet() )  {
	    	if(! entry.getKey().equals(session_conf_default)) {
		    	SessionConf tag = entry.getValue();
		    	taglist = taglist.and(tag.tupleTag);
	    	}
	    }
	    
	    PCollectionTuple mixedEvents = streamData.apply("extract ssn key", ParDo.of(new SessionExtract(sessionConfs)).withOutputTags( sessionConfs.get(session_conf_default).tupleTag , taglist) );
	    
	    PCollectionList<KV<String,TableRow>> windowList = PCollectionList.empty(p);
	    
	    for(Entry<String, SessionConf> entry : sessionConfs.entrySet() )  {
	    	String name = entry.getKey();
	    	SessionConf tag = entry.getValue();
	    	
		    PCollection<KV<String,TableRow>> eventsTmp = mixedEvents.get(tag.tupleTag);

		    PCollection<KV<String,TableRow>> windowedEventsTmp = eventsTmp.apply("SessionWindow: "+name,
		    		Window.<KV<String,TableRow>>into(
		    				Sessions.withGapDuration( tag.windowSize ) ) // same as google analytics 60 mins timeout
		    		.triggering(
		    				AfterWatermark.pastEndOfWindow()
		    					.withEarlyFirings(AfterPane.elementCountAtLeast(500))
		    				)
		    		.withAllowedLateness(Duration.standardMinutes(1) )
		    		.discardingFiredPanes()
		    );
		    
		    windowList = windowList.and(windowedEventsTmp);
	    	
	    }
	    
	    // merge from different window	    
	    PCollection<KV<String,TableRow>> mixedWindowedEvents = windowList.apply(Flatten.<KV<String,TableRow>>pCollections());
	    	    
	    PCollection<KV<String,Iterable<TableRow>>> sessionByUser = mixedWindowedEvents.apply("Group SSN by User", 
	    		GroupByKey.<String, TableRow>create() ); 
	    
	    PCollection<KV<String,Boolean>> eventConversion = sessionByUser.apply("event conversion", 
	    	    ParDo.of(new EventConversion()) );
	    	    
	    	    
	    // -- fixed window + trigger --
	    PCollection<KV<String,Boolean>> windowedEventConversion = eventConversion.apply("Pred Model Win",
	    		Window.<KV<String,Boolean>>into(
	    				FixedWindows.of(Duration.standardDays(1)))
	    		.triggering( Repeatedly.forever(
	    						AfterProcessingTime.pastFirstElementInPane()
	    						.plusDelayOf(Duration.standardMinutes(1)) ) )
	    		.accumulatingFiredPanes() // update
	    		.withAllowedLateness(Duration.standardHours(4))
	    );

	    PCollection<KV<String,Iterable<Boolean>>> eventConversionByName = windowedEventConversion.apply("Conv Group By",
	    		GroupByKey.<String,Boolean>create() );
	    
	    PCollection<TableRow> counting = eventConversionByName.apply("pred counting",ParDo.of(new PredCount() ));
	    
    	counting.apply("Write to Datastore",
			ParDo.of( new DatastoreWriter(options.getDatastoreNamespace(), options.getDatastoreTable() ) 
		));

	    	    
  }
  

}
