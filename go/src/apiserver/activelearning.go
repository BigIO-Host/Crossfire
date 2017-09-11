// Copyright 2017 BigIO.host. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"math"
	"math/rand"
	"strings"
)

//Simplest epsilonGreedy
func epsilonGreedyPolicy(status []bool, values []float64, baselineValue float64, gainonly bool, eps float64) (int, string) {
	if rand.Float64() < eps {
		i := rand.Intn(len(status))
		return i, "explore"
	}

	maxValue := 0.0
	actCnt := len(status)
	for i := 0; i < actCnt; i++ {
		if maxValue < values[i] {
			maxValue = values[i]
		}
	}
	for i := 0; i < actCnt; i++ {
		if values[i] == maxValue {
			if gainonly == true && values[i] > baselineValue {
				//if gainonly and no gain in expected reward. we return suppress
				return -1, "suppress"
			}

			return i, "exploit"

		}
	}
	return -1, "error"
}

//foreverGreedy (100% active learning)
func alwaysExplorePolicy(status []bool, values []float64, baselineValue float64, gainonly bool) (int, string) {
	return epsilonGreedyPolicy(status, values, baselineValue, gainonly, 1.0)
}

//foreverGreedy (no active learning)
func alwaysGreedyPolicy(status []bool, values []float64, baselineValue float64, gainonly bool) (int, string) {
	return epsilonGreedyPolicy(status, values, baselineValue, gainonly, 0.0)
}

//Enhanced epsilonGreedy, agreesive learning for new actions (actions without information)
func curiousEpsilonGreedyPolicy(status []bool, values []float64, baselineValue float64, gainonly bool, eps float64) (int, string) {
	newCnt := 0
	newEps := 0.0
	//var newActions = make([]string, len(values))
	baseProb := 1.0 / float64(len(status))
	//log.Debugf(c, "%d %f", newActions, baseProb)
	for i := 0; i < len(status); i++ {
		if status[i] == false {
			newEps = newEps + baseProb
			//newActions[newCnt] = values[i]
			newCnt++
		}
	}
	//log.Debugf(c, "%f", newEps)

	if rand.Float64() < newEps {
		i := rand.Intn(newCnt)
		return i, "learn-action"
	}

	return epsilonGreedyPolicy(status, values, baselineValue, gainonly, eps)

}

//Softmax
//FIXME: Softmax does not support gainonly!
func softmaxPolicy(status []bool, values []float64, baselineValue float64, gainonly bool, temperature float64) (int, string, []float64) {
	//pick action based on expected reward distribution
	actCnt := len(status)
	sum := 0.0
	var expValues = make([]float64, len(status))
	for i := 0; i < actCnt; i++ {
		expValues[i] = math.Exp(values[i] / temperature)
		sum += expValues[i]
	}

	var probs = make([]float64, len(status))
	for i := 0; i < actCnt; i++ {
		probs[i] = expValues[i] / sum
	}

	// now we have prob distribution of each action
	cum := 0.0
	r := rand.Float64()
	for i := 0; i < actCnt; i++ {
		cum += probs[i]
		if cum > r {
			//FIXME: softmax do not support gainonly
			return i, "softmax", probs
		}
	}
	//something bad happened
	return -1, "error", probs
}

//curiosSoftmax
func curiousSoftmaxPolicy(status []bool, values []float64, baselineValue float64, gainonly bool, temperature float64) (int, string, []float64) {
	//find max value in values, replace the status failure with maxValue.
	//then new action will have same probability as the current best action.
	maxValue := 0.0
	actCnt := len(status)
	for i := 0; i < actCnt; i++ {
		if maxValue < values[i] && status[i] == true {
			maxValue = values[i]
		}
	}
	for i := 0; i < actCnt; i++ {
		if status[i] == false {
			values[i] = maxValue
		}
	}
	return softmaxPolicy(status, values, baselineValue, gainonly, temperature)
}

func activeLearning(exploreMode string, status []bool, values []float64, baselineValue float64, gainonly bool) (int, string) {
	switch strings.ToUpper(exploreMode) {
	case "E":
		return alwaysExplorePolicy(status, values, baselineValue, gainonly)
	case "G":
		return alwaysGreedyPolicy(status, values, baselineValue, gainonly)
	case "EG":
		return epsilonGreedyPolicy(status, values, baselineValue, gainonly, 0.05)
	case "S":
		pickedIndex, pickedMode, _ := softmaxPolicy(status, values, baselineValue, gainonly, 0.1)
		return pickedIndex, pickedMode
	case "CEG":
		return curiousEpsilonGreedyPolicy(status, values, baselineValue, gainonly, 0.05)
	case "CS":
		pickedIndex, pickedMode, _ := curiousSoftmaxPolicy(status, values, baselineValue, gainonly, 0.1)
		return pickedIndex, pickedMode
	default:
		return epsilonGreedyPolicy(status, values, baselineValue, gainonly, 0.05)
	}
}
