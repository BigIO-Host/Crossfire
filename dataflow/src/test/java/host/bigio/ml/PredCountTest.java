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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

import com.google.api.services.bigquery.model.TableRow;

public class PredCountTest {

	@Test
	public void SingleInputTest() throws Exception {
		DoFnTester<KV<String,Iterable<Boolean>>,TableRow> fnTest = DoFnTester.of( new PredCount() );
		
		String ProjectID = "projectXD";
		String Event = "evt001";
		String Key = ProjectID+"|"+Event;
		KV<String, Iterable<Boolean>> input = KV.of(Key, 
													new ArrayList<>(Arrays.asList( new Boolean[]{true,true,false}  )) );
		fnTest.processElement(input);
		
		for(TableRow row : fnTest.takeOutputElements() ) {
			assertEquals(row.get("project"), ProjectID);
			assertEquals(row.get("event"), Event);
			assertEquals(row.get("success"),2 );
			assertEquals(row.get("total"),3);
		}
	}

}
