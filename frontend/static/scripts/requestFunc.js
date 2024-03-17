/* 

This program contains the functions used to request new data from the server 

requestDataFromServer(selectedPeriod, newMap = false)
comparePeriods(baselinePeriod, comparisonPeriod, newMap = false)
syncDataRequest(period)
getShapesFile(layerNo, stops=false)
getLookupTable(layerNo)
getPeakDirections(layerNo)

*/

// Send ajax request to server for initial data or recalculated data
function requestDataFromServer(selectedPeriod, newMap = false) {
    // Call syncDataRequest and use .then() to handle the result asynchronously
    syncDataRequest(selectedPeriod).then(outputData => {
        // The actual data is now available here
        medianData = outputData.median;
        ninetyData = outputData.ninety;
        timepointLookup = outputData.timepoints;

        if (newMap) {
            // Create map
            createMap();
        } else {
            // Update map
            updateMetrics();
            updateMetricFilters();
            updateColorScheme();
            redrawShapes();
            updateLegend();
        }
    }).catch(error => {
        // Handle any errors that might occur during the data request
        console.error("Error requesting data from server:", error);
    });
}

// function requestDataFromServer(selectedPeriod, newMap = false){
	
// 	var outputData = syncDataRequest(selectedPeriod);
// 	medianData = outputData.median;
// 	ninetyData = outputData.ninety;
// 	timepointLookup = outputData.timepoints;
// 	if(newMap){
// 		// Create map
// 		createMap();
// 	} else {
// 		// Update map
// 		updateMetrics();
// 		updateMetricFilters();
// 		updateColorScheme();
// 		redrawShapes();
// 		updateLegend();
// 	};
// }

// Send ajax request to server for two sets of data and find difference
function comparePeriods(baselinePeriod, comparisonPeriod, newMap = false){

	medianData = [];
	ninetyData = [];
	baseMedianData = [];
	baseNinetyData = [];
	compMedianData = [];
	compNinetyData = [];

	// Get baseline and comparison data from static data files
	var baseOutput = syncDataRequest(baselinePeriod);
	baseMedianData = baseOutput.median;
	baseNinetyData = baseOutput.ninety;
	timepointLookup = baseOutput.timepoints;
	var compOutput = syncDataRequest(comparisonPeriod);
	compMedianData = compOutput.median;
	compNinetyData = compOutput.ninety;

	// Check if baseline data has corresponding record in comparison data - if so, take difference and append to global var
	for(var level in baseMedianData){
		var levelMetrics = {};
		for(var index in baseMedianData[level]){
			if(compMedianData[level].hasOwnProperty(index)){
				var baseMetrics = baseMedianData[level][index];
				var compMetrics = compMedianData[level][index];
				var newMetrics = {};
				for(var i in baseMetrics){
					if(compMetrics.hasOwnProperty(i)){
						newMetrics[i] = compMetrics[i] - baseMetrics[i];
					}
				}
				levelMetrics[index] = newMetrics;
			}
		}
		medianData.push(levelMetrics);
	}
	
	for(var level in baseNinetyData){
		var levelMetrics = {};
		for(var index in baseNinetyData[level]){
			if(compNinetyData[level].hasOwnProperty(index)){
				var baseMetrics = baseNinetyData[level][index];
				var compMetrics = compNinetyData[level][index];
				var newMetrics = {};
				for(var i in baseMetrics){
					if(compMetrics.hasOwnProperty(i)){
						newMetrics[i] = compMetrics[i] - baseMetrics[i];
					}
				}
				levelMetrics[index] = newMetrics;
			}
		}
		ninetyData.push(levelMetrics);
	}

	if(newMap){
		// Create map
		createMap();
	} else {
		// Update map
		updateMetrics();
		updateMetricFilters();
		updateColorScheme();
		redrawShapes();
		updateLegend();
	};
}

function syncDataRequest(period) {
    function ajaxCall(period) {
        // Return the $.ajax call which already returns a promise
        return $.ajax({
            type: "PUT",
            url: '/load/load_data',
            data: JSON.stringify(period, null, '\t'),
            contentType: 'application/json; charset=UTF-8',
            dataType: 'json'
        });
    }

    function parseData(data) {
		var segMedianTemp = JSON.parse(data['seg_median']);
		var segNinetyTemp = JSON.parse(data['seg_ninety']);
		var rteMedianTemp = JSON.parse(data['rte_median']);
		var rteNinetyTemp = JSON.parse(data['rte_ninety']);
		var corMedianTemp = JSON.parse(data['cor_median']);
		var corNinetyTemp = JSON.parse(data['cor_ninety']);
		var tpSegMedianTemp  = JSON.parse(data['tp_seg_median']);
		var tpSegNinetyTemp  = JSON.parse(data['tp_seg_ninety']);
		var tpCorMedianTemp  = JSON.parse(data['tp_cor_median']);
		var tpCorNinetyTemp  = JSON.parse(data['tp_cor_ninety']);
		var timepointLookup = data['timepoint_lookup'];
		
		// Get metrics from all different levels and combine
		var metricList = [];
		for(var i in data){
			if (!(i === 'timepoint_lookup')){
				var newMetrics = JSON.parse(data[i])['0'];
				if (!(newMetrics == null)){ // Check that this particular data is not empty (can happen if no timepoints defined)
					metricList.push(Object.getOwnPropertyNames(JSON.parse(data[i])['0']));
				}
			};
		};
		metrics = Array([...new Set(metricList.flat())])[0];

		// Get metrics by level and store in dict, unless no metrics for that level
		if (!(segMedianTemp['0'] == null)){
			levelMetrics['seg'] = Object.getOwnPropertyNames(segMedianTemp['0']);
		} else {
			levelMetrics['seg'] = [];
		}
		if (!(corMedianTemp['0'] == null)){
			levelMetrics['cor'] = Object.getOwnPropertyNames(corMedianTemp['0']);
		} else {
			levelMetrics['cor'] = [];
		}
		if (!(rteMedianTemp['0'] == null)){
			levelMetrics['rte'] = Object.getOwnPropertyNames(rteMedianTemp['0']);
		} else { 
			levelMetrics['rte'] = [];
		}
		if (!(tpSegMedianTemp['0'] == null)){
			levelMetrics['tpSeg'] = Object.getOwnPropertyNames(tpSegMedianTemp['0']);
		} else { 
			levelMetrics['tpSeg'] = [];
		}
		if (!(tpCorMedianTemp['0'] == null)){
			levelMetrics['tpCor'] = Object.getOwnPropertyNames(tpCorMedianTemp['0']);
		} else { 
			levelMetrics['tpCor'] = [];
		}

		// Remove properties that are not metrics from master list and individual lists
		var remove = ["route", "segment", "corridor", "index", "length", "direction", "time", "level_0"];
		metrics = metrics.filter(value => !remove.includes(value));
		for(var level in levelMetrics){
			tempMetrics = levelMetrics[level];
			levelMetrics[level] = tempMetrics.filter(value => !remove.includes(value));
		};

		// Sort metrics in the order specified in the config file (organizes the tables and dropdown menus)
		var unitsArray = Object.entries(units);
		var sortedUnits = unitsArray.sort((a, b) => parseFloat(a[1].order) - parseFloat(b[1].order));
		var sortedArray = [];
		for(var m in sortedUnits){
			sortedArray.push(sortedUnits[m][0])
		}
		metrics.sort((a, b) => sortedArray.indexOf(a) - sortedArray.indexOf(b));
		for(var level in levelMetrics){
			levelMetrics[level].sort((a, b) => sortedArray.indexOf(a) - sortedArray.indexOf(b));
		};

		// Function to take json data and store in dictionary with index as key
		function storeData(dataArray){
			var dataDict = {};
			for(var key of Object.keys(dataArray)){
				var entry = dataArray[key];
				dataDict[entry['index']] = entry;
			};	
			return dataDict
		}

		segMedianData = storeData(segMedianTemp);
		segNinetyData = storeData(segNinetyTemp);
		rteMedianData = storeData(rteMedianTemp);
		rteNinetyData = storeData(rteNinetyTemp);
		corMedianData = storeData(corMedianTemp);
		corNinetyData = storeData(corNinetyTemp);
		tpSegMedianData = storeData(tpSegMedianTemp);
		tpSegNinetyData = storeData(tpSegNinetyTemp);
		tpCorMedianData = storeData(tpCorMedianTemp);
		tpCorNinetyData = storeData(tpCorNinetyTemp);

		var outputMedianData = [segMedianData, rteMedianData, corMedianData, tpSegMedianData, tpCorMedianData];
		var outputNinetyData = [segNinetyData, rteNinetyData, corNinetyData, tpSegNinetyData, tpCorNinetyData];
        return {
            'median': outputMedianData,
            'ninety': outputNinetyData,
            'timepoints': timepointLookup
        };
    }

    // Call ajaxCall and handle the promise it returns
    return ajaxCall(period).then(response => {
        // The response here is the resolved value of the promise, which is responseJSON
        return parseData(response); // Now call parseData with the actual response
    }).catch(error => {
        // Handle any errors that occurred during the AJAX call
        console.error("Error in AJAX call:", error);
    });
}

// // Function to send synchronous request for metric data used in comparison -- need to find asynchronous solution
// function syncDataRequest(period){

// 	function ajaxCall(period){
// 		var response = $.ajax({
// 			type: "PUT",
// 			async: false,
// 			url: '/load/load_data',
// 			data: JSON.stringify(period, null, '\t'),
// 			contentType: 'application/json; charset=UTF-8',
// 			dataType: 'json'
// 		});
// 		return parseData(response.responseJSON)
// 	}

// 	function parseData(data) {
// 		var segMedianTemp = JSON.parse(data['seg_median']);
// 		var segNinetyTemp = JSON.parse(data['seg_ninety']);
// 		var rteMedianTemp = JSON.parse(data['rte_median']);
// 		var rteNinetyTemp = JSON.parse(data['rte_ninety']);
// 		var corMedianTemp = JSON.parse(data['cor_median']);
// 		var corNinetyTemp = JSON.parse(data['cor_ninety']);
// 		var tpSegMedianTemp  = JSON.parse(data['tp_seg_median']);
// 		var tpSegNinetyTemp  = JSON.parse(data['tp_seg_ninety']);
// 		var tpCorMedianTemp  = JSON.parse(data['tp_cor_median']);
// 		var tpCorNinetyTemp  = JSON.parse(data['tp_cor_ninety']);
// 		var timepointLookup = data['timepoint_lookup'];
		
// 		// Get metrics from all different levels and combine
// 		var metricList = [];
// 		for(var i in data){
// 			if (!(i === 'timepoint_lookup')){
// 				var newMetrics = JSON.parse(data[i])['0'];
// 				if (!(newMetrics == null)){ // Check that this particular data is not empty (can happen if no timepoints defined)
// 					metricList.push(Object.getOwnPropertyNames(JSON.parse(data[i])['0']));
// 				}
// 			};
// 		};
// 		metrics = Array([...new Set(metricList.flat())])[0];

// 		// Get metrics by level and store in dict, unless no metrics for that level
// 		if (!(segMedianTemp['0'] == null)){
// 			levelMetrics['seg'] = Object.getOwnPropertyNames(segMedianTemp['0']);
// 		} else {
// 			levelMetrics['seg'] = [];
// 		}
// 		if (!(corMedianTemp['0'] == null)){
// 			levelMetrics['cor'] = Object.getOwnPropertyNames(corMedianTemp['0']);
// 		} else {
// 			levelMetrics['cor'] = [];
// 		}
// 		if (!(rteMedianTemp['0'] == null)){
// 			levelMetrics['rte'] = Object.getOwnPropertyNames(rteMedianTemp['0']);
// 		} else { 
// 			levelMetrics['rte'] = [];
// 		}
// 		if (!(tpSegMedianTemp['0'] == null)){
// 			levelMetrics['tpSeg'] = Object.getOwnPropertyNames(tpSegMedianTemp['0']);
// 		} else { 
// 			levelMetrics['tpSeg'] = [];
// 		}
// 		if (!(tpCorMedianTemp['0'] == null)){
// 			levelMetrics['tpCor'] = Object.getOwnPropertyNames(tpCorMedianTemp['0']);
// 		} else { 
// 			levelMetrics['tpCor'] = [];
// 		}

// 		// Remove properties that are not metrics from master list and individual lists
// 		var remove = ["route", "segment", "corridor", "index", "length", "direction", "time", "level_0"];
// 		metrics = metrics.filter(value => !remove.includes(value));
// 		for(var level in levelMetrics){
// 			tempMetrics = levelMetrics[level];
// 			levelMetrics[level] = tempMetrics.filter(value => !remove.includes(value));
// 		};

// 		// Sort metrics in the order specified in the config file (organizes the tables and dropdown menus)
// 		var unitsArray = Object.entries(units);
// 		var sortedUnits = unitsArray.sort((a, b) => parseFloat(a[1].order) - parseFloat(b[1].order));
// 		var sortedArray = [];
// 		for(var m in sortedUnits){
// 			sortedArray.push(sortedUnits[m][0])
// 		}
// 		metrics.sort((a, b) => sortedArray.indexOf(a) - sortedArray.indexOf(b));
// 		for(var level in levelMetrics){
// 			levelMetrics[level].sort((a, b) => sortedArray.indexOf(a) - sortedArray.indexOf(b));
// 		};

// 		// Function to take json data and store in dictionary with index as key
// 		function storeData(dataArray){
// 			var dataDict = {};
// 			for(var key of Object.keys(dataArray)){
// 				var entry = dataArray[key];
// 				dataDict[entry['index']] = entry;
// 			};	
// 			return dataDict
// 		}

// 		segMedianData = storeData(segMedianTemp);
// 		segNinetyData = storeData(segNinetyTemp);
// 		rteMedianData = storeData(rteMedianTemp);
// 		rteNinetyData = storeData(rteNinetyTemp);
// 		corMedianData = storeData(corMedianTemp);
// 		corNinetyData = storeData(corNinetyTemp);
// 		tpSegMedianData = storeData(tpSegMedianTemp);
// 		tpSegNinetyData = storeData(tpSegNinetyTemp);
// 		tpCorMedianData = storeData(tpCorMedianTemp);
// 		tpCorNinetyData = storeData(tpCorNinetyTemp);

// 		var outputMedianData = [segMedianData, rteMedianData, corMedianData, tpSegMedianData, tpCorMedianData];
// 		var outputNinetyData = [segNinetyData, rteNinetyData, corNinetyData, tpSegNinetyData, tpCorNinetyData];
// 		return {'median': outputMedianData, 'ninety': outputNinetyData, 'timepoints': timepointLookup}
// 	}

// 	return ajaxCall(period)
// }

// Function to send request for shapes file to server
function getShapesFile(layerNo, stops = false) {
    if (selectLinkIndicator === 1) {
        var url = '/load/load_viz_shapes';
        if (stops === true) {
            layerNo['type'] = 'stops';
        } else {
            layerNo['type'] = 'shapes';
        }
    } else {
        var url = '/load/load_shapes';
    }

    // ajaxCall returns a promise because of the asynchronous nature of $.ajax
    function ajaxCall(layerNo) {
        // Return the promise directly from $.ajax
        return $.ajax({
            type: "PUT",
            url: url,
            data: JSON.stringify(layerNo, null, '\t'),
            contentType: 'application/json; charset=UTF-8',
            dataType: 'json'
        }).then(response => {
            // The response is the resolved value of the promise
            return response; // You can return response.responseJSON if you just want the JSON part
        }).fail(error => {
            // Handle errors here
            console.error('Error:', error);
            throw error; // Rethrow the error so it can be caught by the caller
        });
    };

    // Return the promise from ajaxCall
    return ajaxCall(layerNo);
}

// Function to send request for lookup table to server
function getLookupTable(layerNo){

	function ajaxCall(layerNo){
		var response = $.ajax({
			type: "PUT",
			async: false,
			url: '/load/load_lookup',
			data: JSON.stringify(layerNo, null, '\t'),
			contentType: 'application/json; charset=UTF-8',
			dataType: 'json',
		});
		return response.responseJSON
	};
	return ajaxCall(layerNo)
}

// Function to send request for peak direction table to server
function getPeakDirections(layerNo){

	function ajaxCall(layerNo){
		var response = $.ajax({
			type: "PUT",
			async: false,
			url: '/load/load_peak',
			data: JSON.stringify(layerNo, null, '\t'),
			contentType: 'application/json; charset=UTF-8',
			dataType: 'json'
		});
		return response.responseJSON
	};
	return ajaxCall(layerNo)
}