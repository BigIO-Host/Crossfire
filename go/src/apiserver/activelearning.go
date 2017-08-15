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
)

//Simplest epsilonGreedy
func epsilonGreedyAL(status []bool, values []float64, baselineReward float64, gainonly bool, eps float64) (int, string) {
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
			if gainonly == true && values[i] > baselineReward {
				//if gainonly and no gain in expected reward. we return no-action
				return -1, "suppress"
			}

			return i, "exploit"

		}
	}
	return -1, "error"
}

//foreverGreedy (100% active learning)
func alwaysExploreAL(status []bool, values []float64, baselineReward float64, gainonly bool) (int, string) {
	return epsilonGreedyAL(status, values, baselineReward, gainonly, 1.0)
}

//foreverGreedy (no active learning)
func alwaysGreedyAL(status []bool, values []float64, baselineReward float64, gainonly bool) (int, string) {
	return epsilonGreedyAL(status, values, baselineReward, gainonly, 0.0)
}

//Enhanced epsilonGreedy, agreesive learning for new actions (actions without information)
func curiousEpsilonGreedyAL(status []bool, values []float64, baselineReward float64, gainonly bool, eps float64) (int, string) {
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

	return epsilonGreedyAL(status, values, baselineReward, gainonly, eps)

}

//FIXME: Softmax does not support gainonly!
//Softmax
func softmaxAL(status []bool, values []float64, baselineReward float64, gainonly bool, temperature float64) (int, string, []float64) {
	//logic in python
	//z = sum([math.exp(v / temperature) for v in values])
	//probs = [math.exp(v / temperature) / z for v in values]
	//pick action based on probs
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
func curiousSoftmaxAL(status []bool, values []float64, baselineReward float64, gainonly bool, temperature float64) (int, string, []float64) {
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
	return softmaxAL(status, values, baselineReward, gainonly, temperature)
}

func activeLearning(Explore int64, status []bool, values []float64, baselineReward float64, gainonly bool) (int, string) {
	switch Explore {
	case 0:
		return curiousEpsilonGreedyAL(status, values, baselineReward, gainonly, 0.05)
		//bigioDebugf(c, "newEpsilonGreedy: %f default", 0.05)
	case 1:
		return alwaysExploreAL(status, values, baselineReward, gainonly)
		//bigioDebugf(c, "alwaysExplore")
	case 2:
		return alwaysGreedyAL(status, values, baselineReward, gainonly)
		//bigioDebugf(c, "alwaysGreedy")
	case 4:
		return epsilonGreedyAL(status, values, baselineReward, gainonly, 0.05)
		//bigioDebugf(c, "epsilonGreedy: %f", 0.05)
	case 8:
		pickedIndex, pickedMode, _ := softmaxAL(status, values, baselineReward, gainonly, 0.1)
		return pickedIndex, pickedMode
		//bigioDebugf(c, "softmax: %v", probArray)
	case 16:
		return curiousEpsilonGreedyAL(status, values, baselineReward, gainonly, 0.05)
		//bigioDebugf(c, "curiousEpsilonGreedy: %f", 0.05)
	case 32:
		pickedIndex, pickedMode, _ := curiousSoftmaxAL(status, values, baselineReward, gainonly, 0.1)
		return pickedIndex, pickedMode
		//bigioDebugf(c, "curiousSoftmax: %v", probArray)
	default:
		return epsilonGreedyAL(status, values, baselineReward, gainonly, 0.05)
		//bigioDebugf(c, "epsilonGreedy: %f", 0.05)
	}
}
