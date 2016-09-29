const fs = require("fs");
const util = require('util');
var trace = [];
var totalMaps = 0;
var totalReduces = 0;
var mapTasks = [];
var reduceTasks = [];
var getTaskData = function(task) {
	if(task.taskStatus != 'SUCCESS') {
		console.log("FAILURE");
	}
	return {
		taskType: task.taskType,
		taskStatus: task.taskStatus,
		startTime: task.startTime,
		finishTime: task.finishTime
	};
}
var init = function() {
	// Get content from file
	var contents = fs.readFileSync(process.argv[2], 'utf8').split("} {");
	if(contents.length > 1) {
		contents[0] = contents[0] + "}";
		for(var i = 1; i < contents.length - 1; i++) {
			contents[i] = "{" + contents[i] + "}";
		}
		contents[contents.length - 1] = "{" + contents[contents.length - 1];
	} else {
	}

	for(var i = 0; i < contents.length; i++) {
		trace[i] = JSON.parse(contents[i]);
		//console.log(trace[i].submitTime);
		//console.log(trace[i].launchTime);
		//console.log(trace[i].finishTime);

		totalMaps += trace[i].totalMaps;
		totalReduces += trace[i].totalReduces;
		for(var j = 0; j < trace[i].mapTasks.length; j++) {
			mapTasks.push(getTaskData(trace[i].mapTasks[j]));
		}
		for(var j = 0; j < trace[i].reduceTasks.length; j++) {
			reduceTasks.push(getTaskData(trace[i].reduceTasks[j]));
		}
		//console.log(util.inspect(trace[i].reduceTasks, false, null));
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
