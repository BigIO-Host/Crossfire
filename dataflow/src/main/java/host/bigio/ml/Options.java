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

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface Options extends PipelineOptions {
	  
    @Description("Event PubSub subscription")
    @Required
    String getEventSubscription();
    void setEventSubscription(String topic);

    @Description("Datastore NameSpace")
    @Required
    String getDatastoreNamespace();
    void setDatastoreNamespace(String namespace);
    
    @Description("Datastore Table")
    @Required
    String getDatastoreTable();
    void setDatastoreTable(String table);
    
    @Description("Session Window Configurations")
    String getSessionConfs();
    void setSessionConfs(String confs);

    @Description("Session Window Default Conf")
    String getSessionConfDefault();
    void setSessionConfDefault(String defaultConf);
}