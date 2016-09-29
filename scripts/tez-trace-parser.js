const fs = require("fs");
const util = require('util');
var entities = [];
var totalMaps = 0;
var totalReduces = 0;
var mapTasks = [];
var reduceTasks = [];
var getTaskData = function(task, taskType) {
	//console.log(util.inspect(task, false, null));
	if(task.otherinfo.status != 'SUCCEEDED') {
		console.log("FAILURE");
	}
	return {
		taskType: taskType,
		taskStatus: task.otherinfo.status,
		startTime: task.otherinfo.startTime,
		finishTime: task.otherinfo.endTime
	};
}
var isMapTask = function(entity) {
	var counters = entity.otherinfo.counters.counterGroups;
	var isMap1 = false;
	var isMap2 = false;
	var isMap3 = false;
	var isMap4 = false;
	var isReduce1 = false;
	var isReduce2 = false;
	var isReduce3 = false;
	var isReduce4 = false;
	for(var i = 0 ; i < counters.length; i++) {
		if(counters[i].counterGroupName == 'org.apache.tez.common.counters.FileSystemCounter') {
			var fileCounters = counters[i].counters;
			for(var j = 0; j < fileCounters.length; j++) {
				if(fileCounters[j].counterName == 'HDFS_BYTES_READ') {
					isMap1 = true;
				}
			}
			for(var j = 0; j < fileCounters.length; j++) {
				if(fileCounters[j].counterName == 'HDFS_BYTES_WRITTEN') {
					isReduce1 = true;
				}
			}
			if(isMap1 == true && isReduce1 == true) {
				console.log("FAILURE...Both Map and reduce");
			}
		}
		if(counters[i].counterGroupName == 'org.apache.tez.common.counters.TaskCounter') {
			var taskCounters = counters[i].counters;
			for(var j = 0; j < taskCounters.length; j++) {
				if(taskCounters[j].counterName == 'REDUCE_INPUT_GROUPS') {
					isReduce2 = true;
				}
			}
		}
		if(counters[i].counterGroupName == 'HIVE') {
			var hiveCounters = counters[i].counters;
			for(var j = 0; j < hiveCounters.length; j++) {
				//console.log(hiveCounters[j].counterName);
				if(hiveCounters[j].counterName.startsWith('RECORDS_IN_Map_')) {
					isMap3 = true;
				}
				if(hiveCounters[j].counterName.startsWith('RECORDS_OUT_INTERMEDIATE_Map_')) {
					isMap4 = true;
				}
				if(hiveCounters[j].counterName.startsWith('CREATED_FILES')) {
					isReduce3 = true;
				}
				if(hiveCounters[j].counterName.startsWith('RECORDS_OUT_INTERMEDIATE_Reducer')) {
					isReduce4 = true;
				}
			}
		}
	}
	var isMap = isMap1 || isMap2 || isMap3 || isMap4;
	var isReduce = isReduce1 || isReduce2 || isReduce3 || isReduce4;
	if(isMap && isReduce) {
		console.log("WARNING: CONCLUDED TASK AS BOTH MAP AND REDUCE");
	}
	if(!isMap && !isReduce) {
		console.log("WARNING: CONCLUDED TASK AS NONE");
	}
	return isMap;
}

var init = function() {
	// Get content from file
	var contents = fs.readFileSync(process.argv[2], 'utf8').split("\n");
	

	for(var i = 0; i < contents.length-1; i++) {
		var content = contents[i].substring(0, contents[i].length - 1);
		entities[i] = JSON.parse(content);
		if(entities[i].entitytype == 'TEZ_TASK_ID') { 
			if(entities[i].events.length != 1) {
				console.log("WEIRD Events not equal to 1 for task id" + entities[i].entity);
			} else {
				var event = entities[i].events[0];
				if(event.eventtype == 'TASK_FINISHED') {
					//console.log(util.inspect(entities[i], false, null));
					if(isMapTask(entities[i])) {
						mapTasks.push(getTaskData(entities[i], 'MAP'));
						totalMaps++;
					} else {
						reduceTasks.push(getTaskData(entities[i], 'REDUCE'));
						totalReduces++;
					}
				}
			}
		}
		//console.log(util.inspect(entities[i].reduceTasks, false, null));
	}
	
}
function compare(a,b) {
	return a.value - b.value;
}
function logTasksOverTime(properTasks) {
	var tasks = [];
	for(var i = 0; i < properTasks.length; i++) {
		tasks.push({value: properTasks[i].startTime, type: 's'});
		tasks.push({value: properTasks[i].finishTime, type: 'e'});
	}
	tasks.sort(compare);
	var ctr = 0;
	var prevTime = -1;
	var logLast = true;
	for(var i = 0; i < tasks.length; i++) {
		if(tasks[i].type == 's') {
			ctr++;
		} else {
			ctr--;
		}
		var curTime = tasks[i].value;
		if(curTime != prevTime) {
			console.log(tasks[i].value, ctr);
			logLast = false;
		} else {
			logLast = true;
		}
		prevTime = curTime;
	}
	if(logLast) {
		console.log(tasks[tasks.length - 1].value);
	}
}
init();

console.log("MAP TASKS DISTRIBUTION(TIME, #TASKS RUNNING)");
logTasksOverTime(mapTasks);
console.log("REDUCE TASKS DISTRIBUTION(TIME, #TASKS RUNNING)");
logTasksOverTime(reduceTasks);
console.log("Total Maps: " + totalMaps);
console.log("Total Reduces: " + totalReduces);

