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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.google.api.services.bigquery.model.TableRow;

public class EventConversion extends DoFn<KV<String,Iterable<TableRow>>,KV<String,Boolean>> { 
	private static final long serialVersionUID = 6993862288668009727L;

	private boolean labelExists(String label) {
		  if (label != null && ! label.isEmpty()) {
			return true;
		  } 
		  	return false;
	  }
	  
	  private long hintExists(String hint) {
		  if (hint != null && ! hint.isEmpty()) {
			  if (hint.equals("local")) {
				  return 60000;
			  }
		  } 
		  return -1;
	  }
	  
	  @ProcessElement
	  public void processElement(ProcessContext c) throws IOException {
		  KV<String, Iterable<TableRow>> input = c.element();

		  String pid = "";
		  List<TableRow> actions = new ArrayList<>();
		  List<TableRow> goals = new ArrayList<>();
		  List<TableRow> events = new ArrayList<>();
		  List<TableRow> atrs = new ArrayList<>();
		  for(TableRow row : input.getValue()) {
			  
			  pid = (String) row.get("projectid");
			  String eventName = (String) row.get("event");
			  String eventType = (String) row.get("type");
			  
			  if (eventType.equals("ATR")) {
				  atrs.add(row);
			  } else {
				  events.add(row); //now events only have events, actions and goals
			  }
			  
			  c.output(KV.of(
					  String.format("%s|[AL]::%s#%s", pid, eventType, eventName)
					  , false)); // total evt cnt for each event
			  c.output(KV.of(
					  String.format("%s|[AL]::[TE]", pid)
					  , false));           //total evt cnt // optimize me 
			  
			  if (eventType.equals("GOAL")) {
				  goals.add(row);
			  } 
			  if (eventType.equals("ACT")) {
				  actions.add(row);
			  } 
			  
		  }
		  
		  // Classify approach
		  Set<String> fired_goals = new HashSet<>();
		  c.output(KV.of(pid.concat("|").concat("[AL]::[TS]"), false)); //building total ssn cnt (prior) // optimize me (prior denominator)
		  
		  for(int j = goals.size() - 1; j >= 0; j--) {
			  TableRow goal = goals.get(j);
			  String goal_name = String.format("%s#%s", goal.get("type"), goal.get("event"));
			  
			  String goal_label = (String) goal.get("label"); //new
			  String goal_hint = (String) goal.get("hint"); //new
			  boolean label_mode = labelExists(goal_label);     //new
			  long hint_mode = hintExists(goal_hint);     //new
			  
			  //FIXME: problem, what if one ssn has multiple of same goal? we should only fire once for each "type" of goal.
			  if (! fired_goals.contains(goal_name)) {
				  c.output(KV.of(pid.concat("|").concat(goal_name).concat("::[TS]"), true)); //total ssn cnt for each goal (prior numerator)
				  fired_goals.add(goal_name);
			  }
			  
			  //for(TableRow event: events) {
			  for(int k = events.size() - 1; k >= 0; k--) {
				  TableRow event = events.get(k);
				  if ((Long) goal.get("timestamp") > (Long) event.get("timestamp")) {
					  String event_name = String.format("%s#%s", event.get("type"), event.get("event"));
					  if (hint_mode == -1 || (Long) goal.get("timestamp") - (Long) event.get("timestamp") <= hint_mode) { // 60,000 ms
						  c.output(KV.of(pid.concat("|").concat(goal_name).concat("::").concat(event_name), true)); // each evt cnt for each goal
						  c.output(KV.of(pid.concat("|").concat(goal_name).concat("::[TE]"), true));  //total evt cnt for each goal // optimize me  
					  }
					  
					  // we break away when we see first matching event (EVENT#) label (marker) (inclusive)
					  if (label_mode && event_name.startsWith("EVENT#")) { //new
						  String event_label = (String) event.get("label");
						  if (goal_label.equals(event_label)) { 
							  break;                                   
						  }
					  } //end of new
				  }
			  }
			  
			  for(TableRow event: atrs) {
				  //atrs no time concept
				  String event_name = String.format("%s#%s", event.get("type"), event.get("event"));
				  c.output(KV.of(pid.concat("|").concat(goal_name).concat("::").concat(event_name), true)); // each evt cnt for each goal
				  c.output(KV.of(pid.concat("|").concat(goal_name).concat("::[TE]"), true));  //total evt cnt for each goal // optimize me
			  }
		  }
		  
		  // Action approach
		  //for(TableRow action: actions) {
		  for(int i = actions.size() - 1; i >= 0; i--) {
			  TableRow action = actions.get(i);
			  String action_name = String.format("%s#%s", action.get("type"), action.get("event"));
			  String action_label = action_name; //new
			  boolean act_label_mode = labelExists(action_label);  //new
			  
			  //for(TableRow event: events) {
			  for(int k = events.size() - 1; k >= 0; k--) {
				  TableRow event = events.get(k);
				  //action is after event
				  if ((Long) action.get("timestamp") > (Long) event.get("timestamp")) {
					  String event_name = String.format("%s#%s", event.get("type"), event.get("event"));
					  c.output(KV.of(pid.concat("|").concat("[AL]::").concat(action_name).concat("::").concat(event_name), false)); // total evt cnt for each event with action
					  c.output(KV.of(pid.concat("|").concat("[AL]::").concat(action_name).concat("::[TE]"), false)); //total evt cnt with action // optimize me 
					  
					  //we break away when we see first matching event (EVENT#) label (marker) (inclusive)
					  if (act_label_mode && event_name.startsWith("EVENT#")) {  //new
						  String event_label = (String) event.get("label");
						  if (action_label.equals(event_label)) {
							  break;
						  }
					  }  //end of new
				  }
			  } 

			  for(TableRow event: atrs) {
				  //atrs no time concept
				  String event_name = String.format("%s#%s", event.get("type"), event.get("event"));
				  c.output(KV.of(pid.concat("|").concat("[AL]::").concat(action_name).concat("::").concat(event_name), false)); // total evt cnt for each event with action
				  c.output(KV.of(pid.concat("|").concat("[AL]::").concat(action_name).concat("::[TE]"), false)); //total evt cnt with action // optimize me 
			  } 
			  
			  /* we can use this one to know how much explore has done for this action */
			  /* below line not use in the model */
			  Set<String> fired_action_goals = new HashSet<>();
			  c.output(KV.of(pid.concat("|").concat("[AL]::").concat(action_name).concat("::[TS]"), false)); //building total ssn cnt (prior denominator) // optimize me 
			  
			  //for(TableRow goal: goals) {
			  for(int j = goals.size() - 1; j >= 0; j--) {
				  TableRow goal = goals.get(j);
				  //goal is after action
				  if ((Long) goal.get("timestamp") > (Long) action.get("timestamp")) {
					  String goal_name = String.format("%s#%s", goal.get("type"), goal.get("event"));;
					  String goal_label = (String) goal.get("label");          //new
					  String goal_hint = (String) goal.get("hint");        //new
					  boolean goal_label_mode = labelExists(goal_label);         //new
					  long goal_hint_mode = hintExists(goal_hint);         //new
					  /* we can use this one to know the prior for this action */
					  /* below line not use in the model */
					  if (! fired_action_goals.contains(goal_name.concat("::").concat(action_name))) {
						  c.output(KV.of(pid.concat("|").concat(goal_name).concat("::").concat(action_name).concat("::[TS]"), true)); //total ssn cnt for each goal (prior numerator)
						  fired_action_goals.add(goal_name.concat("::").concat(action_name));
					  }
					  
					  //for(TableRow event: events) {
					  for(int k = events.size() - 1; k >= 0; k--) {
						  TableRow event = events.get(k);
						  //action is after event
						  if ((Long) action.get("timestamp") > (Long) event.get("timestamp")) {
							  String event_name = String.format("%s#%s", event.get("type"), event.get("event"));
							  if (goal_hint_mode == -1 || (Long) goal.get("timestamp") - (Long) event.get("timestamp") <= goal_hint_mode) { // 60,000 ms
								  c.output(KV.of(pid.concat("|").concat(goal_name).concat("::").concat(action_name).concat("::").concat(event_name), true)); // each evt cnt for each goal (likelihood top) 
								  c.output(KV.of(pid.concat("|").concat(goal_name).concat("::").concat(action_name).concat("::[TE]"), true));  //total evt cnt for each goal // optimize me (likelihood bottom)
							  }
							  
							  // we break away when we see first matching event "EVENT#" label (marker) (inclusive)
							  if (act_label_mode && goal_label_mode && event_name.startsWith("EVENT#")) { //new
								  String event_label = (String) event.get("label");
								  if (action_label.equals(event_label)) {
									  break;
								  }
							  } //end of new
						  }
					  }
					  
					  for(TableRow event: atrs) {
						  //atrs no time concept
						  String event_name = String.format("%s#%s", event.get("type"), event.get("event"));
						  c.output(KV.of(pid.concat("|").concat(goal_name).concat("::").concat(action_name).concat("::").concat(event_name), true)); // each evt cnt for each goal (likelihood top) 
						  c.output(KV.of(pid.concat("|").concat(goal_name).concat("::").concat(action_name).concat("::[TE]"), true));  //total evt cnt for each goal // optimize me (likelihood bottom)
					  }
					  
				  }
			  }
		  }
		  		  		  
	  }
}
