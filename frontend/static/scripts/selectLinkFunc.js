/* 

This program contains all of the functions that are unique to 
the journey visualization / select link mode:

requestSelectLinkData(requestInput, newMap = false)
sendSelectLinkDataRequest(requestInput)
createSelectLinkLegend()
setSelectLinkPopup(layer, periodData = false)
setSelectLinkStyle(layer)
getLayerWeight(layer)
drawLasso(layers)
drawStop(stopLayer)
multiSelectLegend(selectedIndices, selectedRoutes)
clearStopGeometry(layer)
setStopPopup(layer)
setLayerOrder(layer, min, max)
updateSelectLinkLegend(min, max, values)
redrawSelectLinkShapes()
updateSegText(layer)
loadPeriodData()
calculateMultiselect(layers, index, dataSource = false, period = false)

*/


// Send ajax request to server for initial data or recalculated data
function requestSelectLinkData(requestInput, newMap = false){
	
	segmentData = sendSelectLinkDataRequest(requestInput);
	
	if(newMap){
		// Create map
		createMap();
	} else {
		// Update map
		updateMetrics();
		updateMetricFilters();
		updateColorScheme();
		redrawSelectLinkShapes();
		updateLegend();
	};
}

function sendSelectLinkDataRequest(requestInput){

	if (comparisonIndicator === 1){
		var url = '/load/load_viz_data_comparison';
	} else {
		var url = '/load/load_viz_data';
	}

	// requestInput should be an object with 3 entries: 'period', 'file' and 'level'
	function ajaxCall(requestInput, url){
		var response = $.ajax({
			type: "PUT",
			async: false,
			url: url,
			data: JSON.stringify(requestInput, null, '\t'),
			contentType: 'application/json; charset=UTF-8',
			dataType: 'json',
		});
		return response.responseJSON
	}
	return ajaxCall(requestInput, url)
}

// Generates table of info which is displayed in the LH panel
function createSelectLinkLegend(){

	if (comparisonIndicator === 1){
		legendDef = d3.scaleQuantile().range(rangeGreen);
		var periodText = vizFileDescription[selectedFile[1]] + ' <i> minus </i> ' + vizFileDescription[selectedFile[0]] 
	} else {
		legendDef = d3.scaleQuantile().range(rangeRed);
		var periodText = vizFileDescription[selectedFile];
	}

	// Lookup metadata from config file
	var dataDescription = '<div id="segment-info" class="map-info"></br><p style="color:#9ec1bd; margin-bottom:0px"> Data From: <span id = "date-range" style="color:white">' 
		+ periodText + '<br> </span> </p>'
	
	// Get initial selection of routes, direction and level
	var numRoutes = $("#choose-routes option").length - Object.keys(garageAssignments).length - Object.keys(routeTypes).length - 1; // Subtract group options
	var initDirection = $("#direction option:selected").text();
	var initPeriod = $("#time-period option:selected").text();
	var levelSelect = $("#level option:selected").text();

	// Populate divs and text for presenting selection
	var metaText = dataDescription;
	metaText += '<p style="color:#9ec1bd; margin-bottom:0px"> Resolution Selected: <span id = "sel-level" style="color:white">' + levelSelect + '</span> </p>';
	metaText += '<p style="color:#9ec1bd; margin-bottom:0px"> Routes Selected: <span id = "sel-num" style="color:white">' + numRoutes + '</span> </p>';
	metaText += '<p style="color:#9ec1bd; margin-bottom:0px"> Direction Selected: <span id = "sel-dir" style="color:white">' + initDirection + '</span> </p>';
	metaText += '<p style="color:#9ec1bd"> Time Selected: <span id = "sel-time" style="color:white">' + initPeriod + '</span> </p>';
	metaText += '</div>';
	$("#segment-info").replaceWith(metaText);
}

// Function to add or change popups
function setSelectLinkPopup(layer, periodData = false){

	layer.unbindPopup();
	var route = layer.options.routeID;
	var popText = '<p><b> Route: '+ route +'</b></br>'
	+'</br> Direction: ' + directionLabels[layer.options.directionID]
	+'</br> First Stop: ' + layer.options.startStop
	+'</br> Last Stop: ' + layer.options.endStop
	if (comparisonIndicator === 1){
		popText += '</br> Difference in Trips: ' 
	} else {
		popText += '</br> Trips: ' 
	}
	popText += Math.round(layer.options.traversals);

	if (periodData){
		popText += '</br> Base Period Trips: ' 
		popText += Math.round(layer.options.baseTraversals)
		popText += '</br> Comp Period Trips: ' 
		popText += Math.round(layer.options.compTraversals)
	}
	popText += '</p>'	
	layer.bindPopup(popText, {maxWidth : 350});
}

function setSelectLinkStyle(layer){
	var globalColor = "rgb(111, 148, 220)"
	var layerWeight = getLayerWeight(layer);
	if (layer.options.mode === "bus"){ 
		layer.setStyle({
			weight: layerWeight,
			opacity: 1,
			offset: 3.5,
			interactive: true,
			color: globalColor
		})
	}
	else { 
		layer.setStyle({
			weight: layerWeight,
			opacity: 1,
			offset: 3.5,
			interactive: true,
			color: "rgb(51, 153, 102)"
		})
	}
}

function getLayerWeight(layer){
	var mode = layer.options.mode;
	if (mode === "bus"){
		var weight = 3;
		layer.bringToBack(); // Ensure that rail supercedes bus in draw order for aesthetic reasons
	} else {
		var weight = 6;
	}
	return weight
}

function drawLasso(layers){

	exportData = [];

	console.log("test"); 

	// Get combined data for selected segments
	var selectedLevel = $("#level option:selected").val();
	if (selectedLevel === 'stop' || selectedLevel == null){
		selectedLevel = 'stop';
		var index = ['segIndex'];
	} else if (selectedLevel === 'tp') {
		var index = ['tpIndex'];
	}

	var [combinedData, selectedLayers, selectedRoutes, layerGroups, allStops] = calculateMultiselect(layers, index[0]);

	// If no data available for this segment, inform user and reset selection
	if(Object.keys(combinedData).length === 0){

		var popText = '<p> No data available for these segments during the selected period. </p>';
		layers[0].bindPopup(popText, {maxWidth : 350});
		selectionIndicator = 0;

	} else {
		// Otherwise, use combined data to draw new shapes
		var segmentList = Object.keys(combinedData);
		var layerRange = Object.values(combinedData);

		var min = Math.min(...layerRange);
		var max = Math.max(...layerRange);

		updateSelectLinkLegend(min, max, layerRange);
		routesGeojson.eachLayer(function(drawLayer){

			var travSegmentIndex = drawLayer.options[index];
			if (segmentList.includes(travSegmentIndex)){
				
				var traversals = combinedData[travSegmentIndex];

				// If this is one of the selected layers, color according to max value of that section of the selection
				var stopID = drawLayer.options.endStop;
				var layerSelected = "No";
				if(selectedLayers.includes(travSegmentIndex)){
					layerSelected = "Yes";
					stopID = drawLayer.options.startStop;
					for(var group in layerGroups){
						if(travSegmentIndex in layerGroups[group]){
							traversals = sectionMax[group];
						}
					}
				}
				drawLayer.options.traversals = traversals;
				var layerWeight = getLayerWeight(drawLayer);					
				drawLayer.setStyle({
					weight: layerWeight,
					opacity: 1,
					offset: 3.5,
					color: legendDef(traversals)
				});

				if(selectedLevel === 'tp'){
					var layerData = [drawLayer.options.routeID, directionLabels[drawLayer.options.directionID], drawLayer.options.mode, drawLayer.options.startStop, drawLayer.options.endStop, traversals, layerSelected];
				} else {
					var layerData = [drawLayer.options.routeID, directionLabels[drawLayer.options.directionID], drawLayer.options.mode, stopID, traversals, layerSelected];
				}
				
				exportData.push(layerData);

				setSelectLinkPopup(drawLayer);
				setLayerOrder(drawLayer, min);

			} else {
				restyleShapeTransparentAndSendBack(drawLayer);
			}
		});
		multiSelectLegend(selectedLayers, selectedRoutes);
	}

	// Draw stops associated with selected layers
	stopsGeojson.eachLayer(function(stopLayer){
		stopID = stopLayer.options.stopID;
		if( allStops.includes(stopID) ){
			drawStop(stopLayer);
		};
	})
}

// Function to draw stop geometry
function drawStop(stopLayer){
	stopLayer.setStyle({
		weight: 2,
		opacity: 1,
		radius: 10,
		color: "rgb(13, 13, 13)", 
		fillColor: "rgb(230, 230, 230)",
		fillOpacity: 1,
		fill: true,
	});
	stopLayer.bringToFront();
	setStopPopup(stopLayer);
}

// Function to create table for all segments selected by lasso
function multiSelectLegend(selectedIndices, selectedRoutes){
	var selectedLevel = $("#level option:selected").val();
	if (selectedLevel === 'stop'){
		var levelText = 'Stops';
	} else if (selectedLevel === 'tp') {
		var levelText = "Timepoint";
	}
	var infoText = `<div id='selection-table'>
		<p> Selected Segments: </p>
		<table class = 'attr-table2' style='font-size:10pt;'><tr>
		<th align = 'left' style='color:#9ec1bd'>Route</th>
		<th align = 'left' style='color:#9ec1bd'>` + levelText + `</th></tr>`;

	for(var index in selectedIndices){
		infoText += "<tr><td style='width: 150px;''>" + selectedRoutes[index] + "</td><td>"	+ selectedIndices[index] +"</td></tr>";
	};
	infoText += "</table></div>";
	$("#selection-table").replaceWith(infoText);
}

function clearStopGeometry(layer){
	layer.unbindPopup();
	layer.setStyle({ 
		opacity: 0,
		radius: 0,
		weight: 0,
		fillOpacity: 0
	});
	layer.bringToBack();
}

function setStopPopup(layer){
	var popText = '<p><b> Stop ID: '+ layer.options.stopID +'</b></br></br> Name: ' + layer.options.stopName +'</p>';
	layer.bindPopup(popText, {maxWidth : 350});
}

function setLayerOrder(layer, min, max){

	var traversals = layer.options.traversals;
	if (traversals < legendDef.domain()[0]){ // If layer falls within lowest legend bin, move to back so it doesn't obstruct other flows
		layer.bringToBack();
	}
}

// Update legend whenever a new visualization metric is chosen
function updateSelectLinkLegend(min, max, values){
	
	var selectedLevel = $("#selmode").val();
	if(selectedLevel === 'all'){
		var newTitle = "Passenger flows upstream and downstream of selected segment";
	} else if(selectedLevel === 'upstream'){
		var newTitle = "Passenger flows upstream of selected segment";
	} else if(selectedLevel === 'downstream'){
		var newTitle = "Passenger flows downstream of selected segment";
	} else if(selectedLevel === 'origin'){
		var newTitle = "Passenger flows for journeys originating from selected segment";
	} else if(selectedLevel === 'transfer'){
		var newTitle = "Passenger flows for journeys that involve at least one transfer";
	} else {
		var newTitle = "Passenger flows for journeys ending at selected segment"; 
	}
		
	var domainRange = max - min;
	var legendSVG = d3.select("#legend-svg");

	if (comparisonIndicator === 1){

		legendDef.domain(values);
		legendSVG.append("g")
			.attr("id", "bus-legend")
			.attr("class", "legendQuant")
			.attr("transform", "translate(0,25)")
			.attr("fill","white");

		var legend = d3.legendColor()
			.labelFormat(d3.format(".1f"))
			.title(newTitle)
			.labels(d3.legendHelpers.thresholdLabels)
			.titleWidth(0.15*innerWidth)
			.scale(legendDef);

	} else {
		
		legendSVG.append("g")
			.attr("id", "bus-legend")
			.attr("class", "legendQuant")
			.attr("transform", "translate(0,25)")
			.attr("fill","white");
		
		var domain = [min + domainRange * 0.025, min + domainRange * 0.05, min + domainRange * 0.10, min + domainRange * 0.25];
		var threshold = d3.scaleThreshold()
			.domain(domain)
			.range(legendDef.range())

		var legend = d3.legendColor()
			.labelFormat(d3.format(".1f"))
			.title(newTitle)
			.labels(d3.legendHelpers.thresholdLabels)
			.titleWidth(0.15*innerWidth)
			.scale(threshold);
	
		legendDef = threshold;
	}

	legendSVG.select("#bus-legend").call(legend);
}

// Redraw shapes whenever a filter is changed -- also writes metrics for filtered shapes to array for export
function redrawSelectLinkShapes(){

	// If "Select All" is selected, include all (supercedes previous)
	if (selectedRoutes.includes("All")){
		selectedRoutes = $.map($("#choose-routes option"), function(option) { return option.value });
	}

	// Get direction filter value, include both
	var directionValue = $( "#direction" ).val();
	var selectedDirection = directionValue;
	if( directionValue === "both"){
		selectedDirection = [0, 1];
	}

	// Get selected time period (for peak direction filtering)
	var selectedPeriod = $('#time-period').val();

	// Redraw shapes if they are within all filter selections
	routesGeojson.eachLayer(function(layer){

		var layerRoute = layer.options.routeID;
		var layerDirection = layer.options.directionID;
		var layerInFilter = true;

		// Get route-specific direction from lookup table if "peak" direction is selected
		if (directionValue === "peak"){
			selectedDirection = [peakDirections[selectedPeriod][layerRoute]];
			if(selectedDirection === undefined){ // If the layer is not in lookup table, set to empty
				selectedDirection = [];
			}
		}

		// (1) Check if layer is outside current route selection
		if (!selectedRoutes.includes(layerRoute)){
			layerInFilter = false;
		} else 
		// (2) Check if layer is outside current direction selection
			if (!selectedDirection.includes(layerDirection)){
				layerInFilter = false;
		}

		// If segment selected, check if layer in the traversed segments
		if (selectionIndicator === 1) {
			var layerStart = layer.options.startStop;
			var layerEnd = layer.options.endStop;
			var layerCorIndex = "('" + layerStart + "', '" + layerEnd + "')";
			if (!(layerCorIndex in segmentData[selectedSegment])){
				layerInFilter = false;
			}
		}

		// Draw transparent shape
		if(layerInFilter === false){
			restyleShapeTransparentAndSendBack(layer);
		} else if(selectionIndicator === 1){
			var initColor = segmentData[selectedSegment][layerCorIndex];
			var layerWeight = getLayerWeight(layer);
			layer.setStyle({
				weight: layerWeight,
				opacity: 1,
				offset: 3.5,
				interactive: true,
				color: legendDef(initColor)
			});
		} else {
			setSelectLinkStyle(layer);
		}
	});
};

// Add information about selected segment
function updateSegText(layer){
	var selectedLevel = $("#level option:selected").val();
	var segText = '<div id="selection-table" class="map-info">';
	segText += '<p style="color:#9ec1bd; margin-bottom:0px"> Route ID: <span id = "sel-num" style="color:white">' + layer.options.routeID + '</span> </p>';

	if (selectedLevel === 'stop' || selectedLevel == null){
		segText += '<p style="color:#9ec1bd; margin-bottom:0px"> Stop ID: <span id = "sel-num" style="color:white">' + layer.options.startStop + '</span> </p>';
	} else if (selectedLevel === 'tp') {
		segText += '<p style="color:#9ec1bd; margin-bottom:0px"> First Stop: <span id = "sel-num" style="color:white">' + layer.options.startStop + '</span> </p>';
		segText += '<p style="color:#9ec1bd; margin-bottom:0px"> Second Stop: <span id = "sel-dir" style="color:white">' + layer.options.endStop + '</span> </p>';
	}
	segText += '<p style="color:#9ec1bd; margin-bottom:0px"> Direction: <span id = "sel-dir" style="color:white">' + directionLabels[layer.options.directionID] + '</span> </p>';
	
	if (comparisonIndicator === 1){
		segText += '<p style="color:#9ec1bd"> Difference in Trips: <span id = "sel-time" style="color:white">' + Math.round(layer.options.traversals) + '</span> </p> </div>';
	} else {
		segText += '<p style="color:#9ec1bd"> Total Trips: <span id = "sel-time" style="color:white">' + Math.round(layer.options.traversals) + '</span> </p> </div>';
	}
	$("#selection-table").replaceWith(segText);
}

function loadPeriodData(){

	// Return nothing if no segment is selected
	if (selectedSegment.length === 0){
		return
	}

	var selectedMode = $("#selmode").val();
	if(selectedMode == null){
		selectedMode = 'all';
	}

	// Get selected segment-specific data from back end
	requestInput = {'level': selectedMode, 'segment': selectedSegment};
	function ajaxCall(requestInput){
		var response = $.ajax({
			type: "PUT",
			async: false,
			url: '/load/load_period_data',
			data: JSON.stringify(requestInput, null, '\t'),
			contentType: 'application/json; charset=UTF-8',
			dataType: 'json',
		});
		return response.responseJSON
	}

	var periodData = ajaxCall(requestInput);
	var selectedLevel = $("#level option:selected").val();
	if (selectedLevel === 'stop'){
		var index = 'segIndex';
	} else {
		var index = 'tpIndex';
	}

	// Write base and comp traversals for each layer
	var baseData = calculateMultiselect(selectedLayer, index, periodData, 'base');
	var compData = calculateMultiselect(selectedLayer, index, periodData, 'comp');

	var popupLayer = null;
	routesGeojson.eachLayer(function(layer){
		var layerIndex = layer.options[index];
		layer.options.baseTraversals = baseData[layerIndex];
		layer.options.compTraversals = compData[layerIndex];
		setSelectLinkPopup(layer, true);

		if (selectedSegment.includes(layerIndex)){
			popupLayer = layer;
		}
	});

	// Open new popup for selected layer 
	map.closePopup();
	popupLayer.openPopup();
}

function calculateMultiselect(layers, index, dataSource = false, period = false){
	
	// Function to return layerData
	function getLayerData(layer, index, selectionMode){

		var baseIndex = layer.options[index];
		var allSegs = Object.keys(segmentData);

		if (allSegs.includes(baseIndex)){
			if(selectionMode === 'all'){ // Merge both upstream and downstream
				var upstreamData = segmentData[baseIndex]['upstream'];
				var downstreamData = segmentData[baseIndex]['downstream'];
				var layerTraversals = Object.assign(upstreamData, downstreamData);
			} else {
				var layerTraversals = segmentData[baseIndex][selectionMode];
			}
			return layerTraversals;
		} else {
			return [];
		}
	}	
	
	// Get selection mode
	var selectionMode = $("#selmode").val();
	var layerGroups = [];
	if (["all", "upstream", "downstream", "transfer"].includes(selectionMode)){

		// Need to ensure we're not double counting flows for adjacent segments when these selection modes are active
		// First: find segments that represent the endpoints of all continous lines in the selection
		var allStops = [];
		var endStops = [];
		var endLayers = [];
		var selectedLayers = [];
		var selectedRoutes = [];
		for(var layer in layers){
			var match = false;
			var startStop = layers[layer].options.startStop;
			var endStop = layers[layer].options.endStop;
			allStops.push(startStop)
			allStops.push(endStop)

			var segID = layers[layer].options[index];
			selectedLayers.push(segID);
			selectedRoutes.push(layers[layer].options.routeID)
			var pairIndex = 0;
			for(var pair in endStops){
				// If new segment is an extension of the existing segment
				var pairStart = endStops[pair][0];
				var pairEnd = endStops[pair][1];
				if(startStop === pairEnd){
					var newPair = [pairStart, endStop];
					var newLayers = [endLayers[pairIndex][0], layer];
					match = true;
				}

				// If new segment precedes existing segment
				if(endStop === pairStart){
					var newPair = [startStop, pairEnd];
					var newLayers = [layer, endLayers[pairIndex][1]];
					match = true;
				}
				if(match === true){
					endStops[pairIndex] = newPair;
					endLayers[pairIndex] = newLayers;
					layerGroups[pairIndex].push(segID);
				}
				pairIndex += 1;
			}

			if(match === false){
				endStops[endStops.length] = [startStop, endStop];
				endLayers[endLayers.length] = [layer, layer];
				layerGroups[layerGroups.length] = [segID];
			}
		}

		// Second, get the traversals for each pair. Take the max pax flow for any common traversed segments.
		var combinedData = {};
		var sectionMax = [];
		var maxFlow = 0;
		for(var pair in endLayers){
			var startLayer = layers[endLayers[pair][0]]
			var endLayer = layers[endLayers[pair][1]]
			if ( dataSource === false ){
				var startData = getLayerData(startLayer, index, selectionMode);
				var endData = getLayerData(endLayer, index, selectionMode);
			} else {
				var startData = dataSource[startLayer.options[index]][period];
				var endData = dataSource[endLayer.options[index]][period];
			}
			for(var travSegment in startData){
				var flow = startData[travSegment];
				if(travSegment in endData){
					flow = Math.max(flow, endData[travSegment]);
					if(flow > Math.abs(maxFlow)){
						maxFlow = flow;
					}
				}
				if(travSegment in combinedData){
					combinedData[travSegment] += flow;
				} else {
					combinedData[travSegment] = flow;
				}
			}

			for(var travSegment in endData){
				if(travSegment in startData){
					continue;
				}
				var flow = endData[travSegment];
				if(flow > Math.abs(maxFlow)){
					maxFlow = flow;
				}
				if(travSegment in combinedData){
					combinedData[travSegment] += flow;
				} else {
					combinedData[travSegment] = flow;
				}
			}
			sectionMax[pair] = maxFlow;
		}

	} else {
		// If origin or destination selection mode active, just take the sum across all selected segments
		var allStops = [];
		var endStops = [];
		var endLayers = [];
		var selectedLayers = [];
		var selectedRoutes = [];
		for(var layer in layers){
			var match = false;
			var startStop = layers[layer].options.startStop;
			var endStop = layers[layer].options.endStop;
			allStops.push(startStop)
			allStops.push(endStop)

			var segID = layers[layer].options[index];
			selectedLayers.push(segID);
			selectedRoutes.push(layers[layer].options.routeID)
		}

		var combinedData = {};
		for(var layer in layers){
			if (dataSource === false){
				var layerData = getLayerData(layers[layer], index, selectionMode);
			} else {
				var layerData = getLayerData(layers[layer], index, selectionMode, dataSource);
			}

			for(var travSegment in layerData){
				var flow = layerData[travSegment];
				if(travSegment in combinedData){
					combinedData[travSegment] += flow;
				} else {
					combinedData[travSegment] = flow;
				}
			}
		}
	}

	if (dataSource === false){
		return [combinedData, selectedLayers, selectedRoutes, layerGroups, allStops]
	} else {
		return combinedData
	}

}