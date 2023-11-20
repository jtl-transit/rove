/* 
This program contains all of the functions used to update the map when 
the filter states are changed:
updateMetrics()
updateMetricFilters()
restyleShapeTransparentAndSendBack(layer)
updateTime(minTime, maxTime, predefined = false)
customLegend(domain)
updateLayerData(inputData, prefix = '')
updateColorScheme()
updateLegend()
redrawShapes()
updateFiltersTable()
updateValues()
resetGlobalVars()
*/

// Function to update metrics given new server data
function updateMetrics(){
	var stat = $( "#statistic" ).val();
	if(stat === 'median'){ // Display median data
		updateLayerData(medianData);
		if(comparisonIndicator === 1){
			updateLayerData(compMedianData, "comp-");
			updateLayerData(baseMedianData, "base-");
		}

	} else { // Display 90th percentile data
		updateLayerData(ninetyData);
		if(comparisonIndicator === 1){
			updateLayerData(compNinetyData, "comp-");
			updateLayerData(baseNinetyData, "base-");
		}
	}
}

// Function to update the filters if the statistic is changed
function updateMetricFilters(){

	const units_reordered = {}
	for (var m in units) {
		units_reordered[units[m]['order']] = m
	}

	// Update filters
	var count = 0
	for (var i in metrics){
		if (i in units_reordered) {
			// var metricVal = metrics[i];
			var metricVal = units_reordered[i];
			var filterNum = count;
			var filterLevel = $( "#level-sel" + filterNum ).val();

			var metricRange = [];
			routesGeojson.eachLayer(function(layer) {
				metricRange.push(layer.options[filterLevel + '-' + metricVal])
			});

			var metricMin = Math.floor(d3.min(metricRange))
			var metricMax = Math.ceil(d3.max(metricRange))
			var step = getStep(metricMin, metricMax)

			// Set the domain of your scale to be [min, max]
			legendDef.domain([metricMin, metricMax]);

			// console.log(i, metricVal, units_reordered[i], filterLevel, metricMin, metricMax)
			$( "#metric-slider"+filterNum ).slider('setAttribute', 'max', metricMax);
			$( "#metric-slider"+filterNum ).slider('setAttribute', 'min', metricMin);
			$( "#metric-slider"+filterNum ).slider('setAttribute', 'step', step);
			$( "#metric-slider"+filterNum ).slider('refresh');
			$( "#metric-slider"+filterNum ).slider("setValue", [ metricMin, metricMax ])
		}
		
		count += 1;
	}
}

// Function to make shapes transparent and send to back of order for clicks
function restyleShapeTransparentAndSendBack(layer){
	layer.setStyle({ opacity: 0 });

	// Send to back so it doesn't appear in front of other layers
	layer.bringToBack();

	// Make layer not clickable
	layer.setStyle({interactive: false});
}

function updateTime(minTime, maxTime, predefined = false){

	// Delete previous data
	medianData = [];
	ninetyData = [];

	// If custom range selected
	if(predefined === false){
		// Replace text shown on left hand panel
		var minMinute = minTime[1]
		if (minMinute === 0){
			minMinute = '00'
		}
		var maxMinute = maxTime[1]
		if (maxMinute === 0){
			maxMinute = '00'
		}
		var newtext = "<span id='sel-time' style='color:white'>"+minTime[0]+":" + minMinute + " - " + maxTime[0] + ":"+ maxMinute + " </span>"
		$("#sel-time").replaceWith(newtext)

		// Delete peak option from direction selector
		if($.map($("#direction option"), function(option) { return option.value }).includes("peak")){
			$("#direction option[value='peak']").remove();
		}

		if(comparisonIndicator === 0){
			// Send time range to server and get updated data
			var selectedPeriod = {'custom_range': [minTime, maxTime], 'predefined': 0, 'file': selectedFile};
			requestDataFromServer(selectedPeriod, newMap=false)
		} else {
			var baselinePeriod = {'custom_range': [minTime, maxTime], 'predefined' : 0, 'file': selectedFile[0]};
			var comparisonPeriod = {'custom_range': [minTime, maxTime], 'predefined' : 0, 'file': selectedFile[1]};
			comparePeriods(baselinePeriod, comparisonPeriod, newMap=false);
		}

	} else {
		// Replace text shown on left hand panel
		var newtext = "<span id='sel-time' style='color:white'>"+periodNames[predefined]+"</span>"
		$("#sel-time").replaceWith(newtext);

		// Remove "peak" option from direction selector if already there and not available for this period
		if($.map($("#direction option"), function(option) { return option.value }).includes("peak")){
			if (peakDirections[predefined] === undefined){
				$("#direction option[value='peak']").remove();
			};
		// Otherwise add peak option if it is available and not already there
		} else if (!(peakDirections[predefined] === undefined)){
			{ 
				$('#direction').append($("<option></option>").attr("value", "peak").text("Peak"));
			}
		};

		if(comparisonIndicator === 0){
			// Send time range to server and get updated data
			var selectedPeriod = {'custom_range': null, 'predefined': predefined, 'file': selectedFile};
			requestDataFromServer(selectedPeriod, newMap=false);
		} else {
			var baselinePeriod = {'custom_range': null, 'predefined' : predefined, 'file': selectedFile[0]};
			var comparisonPeriod = {'custom_range': null, 'predefined' : predefined, 'file': selectedFile[1]};
			comparePeriods(baselinePeriod, comparisonPeriod, newMap=false);
		}
	}
}

// Function to update the legend and shape colors when new legend bins are specified
function customLegend(domain){
	if(domain[0] === undefined){ // If no entries in the custom legend inputs
		redrawShapes();
		updateLegend();
	} else {
		var newMetric = $( "#metric" ).val()
		// Add "Difference in ..." prefix to legend title if comparison selected
		if(comparisonIndicator === 1){
			var newTitle = "Difference in " + units[newMetric]["label"];
		} else {
			var newTitle = units[newMetric]["label"];
		}

		// Set up new color scheme using defined thresholds, existing colors
		var threshold = d3.scaleThreshold()
			.domain(domain)
			.range(legendDef.range())

		var legendSVG = d3.select("#legend-svg");

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
		.scale(threshold);

		legendDef = threshold;

		legendSVG.select("#bus-legend").call(legend);
		redrawShapes(domain);
	}
}

// Function to update shapes with new data
function updateLayerData(inputData, prefix = ''){
	routesGeojson.eachLayer(function(layer) {

		// Get indices
		var segIndex = layer.options['segIndex'];
		var rteIndex = layer.options['rteIndex'];
		var corIndex = layer.options['corIndex'];
		var tpIndex = layer.options['tpIndex'];
		
		// Function to append data to a line depending on the level
		function updateData(inputData, level, order, index, prefix, layer){
			var metricList = levelMetrics[level];
			if (typeof inputData[order][index] === 'undefined'){
				for(var j in metricList){
					var metricName = metricList[j];
					// Default to segment-level metric value if this segment is not part of a corridor
					if(level === 'cor'){
						layer.options[prefix + level + '-' + metricName] = layer.options[prefix + 'seg-' + metricName];
					} else if (level === 'tpCor'){
						layer.options[prefix + level + '-' + metricName] = layer.options[prefix + 'tpSeg-' + metricName];
					} else {
						layer.options[prefix + level + '-' + metricName] = null;
					}
				};
			} else {
				for(var j in metricList){
					var metricName = metricList[j];
					var metricValue = inputData[order][index][metricName];
					layer.options[prefix + level + '-' + metricName] = metricValue;
				};
			};
		};

		newLine = updateData(inputData, 'seg', 0, segIndex, prefix, layer);
		newLine = updateData(inputData, 'rte', 1, rteIndex, prefix, layer);
		newLine = updateData(inputData, 'cor', 2, corIndex, prefix, layer);
		newLine = updateData(inputData, 'tpSeg', 3, tpIndex, prefix, layer);
		newLine = updateData(inputData, 'tpCor', 4, tpIndex, prefix, layer);

		setPopup(layer);
	});
}

// Change to high-low or low-high 
function updateColorScheme(){
	
	// Get metric filter value
	var newMetric = $( "#metric" ).val();
	var newScheme = redValues[newMetric];

	if(newScheme == "High"){
		legendDef.range(rangeBlue)
	} else{
		legendDef.range(rangeGreen)
	};

	// Always show low values as red in comparison
	if(comparisonIndicator === 1){
		legendDef.range(rangeGreen)
	}
}

// Update legend whenever a new visualization metric is chosen
function updateLegend(){

	// Get metric filter value and text, level filter value
	var newMetric = $( "#metric" ).val()
	if(comparisonIndicator === 1){
		var newValue = $("input[name='comp-radio']:checked").val()
		if(newValue === 'absolute'){
			var newTitle = "Difference in " + units[newMetric]["label"];
		} else {
			var newTitle = "Percent Difference in " + units[newMetric]["label"];
		}
	
	} else {
		var newTitle = units[newMetric]["label"];
	}
	var newLevel = $( "#level" ).val()

	// Get range for new metric
	var newRange = [];
	routesGeojson.eachLayer(function(layer) {
		newRange.push(layer.options[newLevel + "-" + newMetric])
	});

	// Check if metric is currently being filtered; trim range if true
	if (activeFilters.includes(newMetric)){
		var filterNum = metrics.findIndex(x => x === newMetric);
		var filterRange = $( "#metric-slider" + filterNum ).val();
		function isInRange(value) {
			return value >= this[0] && value <= this[1];
		};
		newRange = newRange.filter(isInRange, filterRange);
	};

	var legendSVG = d3.select("#legend-svg");

	// Add new metric range to color scale
	legendDef = d3.scaleQuantile().range(rangeGreen)

	if (newMetric == "boardings") {

		// Calculate min and max of newRange
		var min = d3.min(newRange);
		var max = d3.max(newRange);
	
		// Set the domain of your scale to be [min, max]
		legendDef.domain([min, max]);
	} else {legendDef.domain(newRange)}

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

	legendSVG.select("#bus-legend").call(legend);
}

// Redraw shapes whenever a filter is changed -- also writes metrics for filtered shapes to array for export
function redrawShapes(domain = 0){
	
	// Send old segments to the back regardless of filter status
	routesGeojson.eachLayer(function(layer){
		if (layer.options['newSeg'] === false){
			layer.bringToBack();
		}
	});

	// Clear previous export data
	exportData.length = 0

	// Get metric, level and route filter values
	var newMetric = $( "#metric" ).val();
	var newLevel = $( "#level" ).val();

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

	// Get range for new metric
	if(domain === 0){
		domain = [];
		routesGeojson.eachLayer(function(layer) {
			domain.push(layer.options[newLevel + '-' + newMetric]);
		});
		var initScheme = redValues[newMetric];

		// Add new metric range to color scale
		legendDef = d3.scaleQuantile().range(rangeGreen)

		if (initScheme == "High" && comparisonIndicator == 0){
			legendDef.range(rangeBlue);
		} 

		if (newMetric == "boardings") {
			// Calculate min and max of newRange
			var min = d3.min(domain);
			var max = d3.max(domain);
	
			// Set the domain of your scale to be [min, max]
			legendDef.domain([min, max]);
		} else {legendDef.domain(domain)} 
	}	

	// Get selected time period (for peak direction filtering)
	var selectedPeriod = $('#time-period').val();

	// Range filter event handle
	var filterID = [];
	var filterMin = [];
	var filterMax = [];
	var filterLevel = [];
	$('.range-slider').each(function(){
		filterID.push(metrics[filterID.length])
		filterMin.push($(this).val()[0])
		filterMax.push($(this).val()[1])
	});

	$('.level-select').each(function(){
		filterLevel.push($(this).val())
	});

	var corridorList = [];
	var newRouteList = [];

	// Redraw shapes if they are within all filter selections
	routesGeojson.eachLayer(function(layer){

		var layerRoute = layer.options.routeID;
		var layerDirection = layer.options.directionID;

		// Get route-specific direction from lookup table if "peak" direction is selected
		if (directionValue === "peak"){
			selectedDirection = [peakDirections[selectedPeriod][layerRoute]];
			if(selectedDirection === undefined){ // If the layer is not in lookup table, set to empty
				selectedDirection = [];
			}
		}
		var layerInFilter = true;
		// (1) Check if layer is outside current route selection
		if (!selectedRoutes.includes(layerRoute)){
			layerInFilter = false;
			// If geographic comparison with multiple routes
			if((typeof layerRoute === 'object') && (layerRoute !== null)){
				if('comp' in layerRoute){
					for(var i in layerRoute['comp']){
						if(selectedRoutes.includes(layerRoute['comp'][i])){
							layerInFilter = true;
							break
						}
					}
				}
				else if('base' in layerRoute){
					for(var i in layerRoute['base']){
						if(selectedRoutes.includes(layerRoute['base'][i])){
							layerInFilter = true;
							break
						}
					}
				} 
			}
		} 
		// (2) Check if layer is outside current direction selection
		if ((!selectedDirection.includes(layerDirection)) && (layerInFilter == true)){
			layerInFilter = false;

			// If geographic comparison with multiple directions
			if((typeof layerDirection === 'object') && (layerDirection !== null)){
				if('comp' in layerDirection){
					for(var i in layerDirection['comp']){
						if(selectedDirection.includes(layerDirection['comp'][i])){
							layerInFilter = true;
							break
						}
					}
				} 
				if(('base' in layerDirection) && (layerInFilter == false)){
					for(var i in layerDirection['base']){
						if(selectedDirection.includes(layerDirection['base'][i])){
							layerInFilter = true;
							break
						}
					}
				} 
			}
		} 
		// (3) Check if layer is null for current visualiation metric
		if ((layer.options[newLevel + '-' + newMetric] === null) && (layerInFilter == true)){
			layerInFilter = false;
		} 
		// (4) Check if layer is outside current filter ranges
		if (layerInFilter == true){
			
			for(var filter in filterID){

				var layerValue = layer.options[filterLevel[filter] + '-' + filterID[filter]]
				
				// If the layer is null for any active filters
				if (activeFilters.includes(filterID[filter]) && layerValue === null && filterLevel[filter] === 'seg'){
					layerInFilter = false;
					break
					
				// Or if it falls outside of any filter
				} else if(layerValue < filterMin[filter] || layerValue > filterMax[filter]){
					if(layerValue === null){
						continue
					}
					layerInFilter = false;
					break
				};
			};
		};

		// Draw either color or transparent shape
		if(layerInFilter == true){
			var newColor = layer.options[newLevel + '-' + newMetric];
			// Catch null values
			if (layer.options['newSeg'] === true) {

				if(typeof newColor === "undefined" ){
					restyleShapeTransparentAndSendBack(layer);
				} else {
					// Color according to selection
					layer.setStyle({
						weight: 2.5,
						opacity: 1,
						interactive: true,
						color: legendDef(newColor)
					});

					if(layerDirection === 0){
						layer.bringToFront();
					};
				};
			};

			// If corridor level selected, don't write duplicate rows for corridors or route-directions
			if (!(newLevel === 'cor' && corridorList.includes(layer.options.corIndex)) && !(newLevel === 'rte' && newRouteList.includes(layer.options.rteIndex))){
				// Add identifying information to the columns of the export data				
				var layerData = [layer.options.routeID, directionLabels[layer.options.directionID]];
				if (!(newLevel === 'rte')){ // If not route level, add information about the stops too
					layerData.push(layer.options.startStop);
					layerData.push(layer.options.endStop);
					layerData.push(layer.options.signal);
				};

				for(var index in levelMetrics[newLevel]){
					layerData.push(layer.options[newLevel + '-' + levelMetrics[newLevel][index]]);
				};
				// In comparison mode, add baseline and comparison metrics
				if(comparisonIndicator === 1){
					for(var index in levelMetrics[newLevel]){
						layerData.push(layer.options['base-' + newLevel + '-' + levelMetrics[newLevel][index]]);
					};
					for(var index in levelMetrics[newLevel]){
						layerData.push(layer.options['comp-' + newLevel + '-' + levelMetrics[newLevel][index]]);
					};
				}
				exportData.push(layerData);
				corridorList.push(layer.options.corIndex);
				newRouteList.push(layer.options.rteIndex);
			};
		} else
		if ((layer.options['newSeg'] === true) || (activeFilters.length > 0)) {
			restyleShapeTransparentAndSendBack(layer);
		} else { // If no filters are active, show old segments
			// Draw old shapes
			if (activeFilters.length === 0) {
				layer.setStyle({
					weight:1,
					opacity: 1,
					offset: 3.5,
					interactive: true,
					color: color="rgb(184, 122, 168)" // Assign pink color if old segment
				});
			};
		};
	});
};

// Update information in Left panel below legend - to be renamed
function updateFiltersTable(){
	
	// Populate filter state information in the table below legend
	var infoText = `<div id='filter-table'><table class = 'attr-table2' style='font-size:10pt;'><tr>
		<th align = 'left' style='color:#9ec1bd'>Metric</th>
		<th align = 'left' style='color:#9ec1bd'>Range</th></tr>`;
	
	var count = 0;
	$('.range-slider').each(function(){
		var metricMin = $(this).val()[0];
		var metricMax = $(this).val()[1];
		if (metrics[count] in units) {
			var metricName = units[metrics[count]]["label"]; // Get metric name w/ index

			if( isNaN(metricMin) ) { metricMin = 0; }
			if( isNaN(metricMax) ) { metricMax = 0;	}
	
			infoText = infoText + "<tr><td style='width: 150px;''>"
				+ metricName + "</td><td>"
				+ Number(metricMin).toFixed(0) + " - " + Number(metricMax).toFixed(0)
				+"</td></tr>";
		}

		count += 1;
	});
	infoText += "</table></div>";
	$("#filter-table").replaceWith(infoText);

};

// Function to toggle between absolute values and percentages in comparison mode
function updateValues(){
	
	// Define calculation based on radio button selection
	var newValue = $("input[name='comp-radio']:checked").val()
	if(newValue === 'absolute'){
		var calculateValue = function (base, comp) {
			return comp - base
		};
	} else {
		var calculateValue = function (base, comp) {
			if(base < 0.0001){
				return null
			} else {
				return Math.round(100 * (comp - base) / base)
			}
		}
	}

	medianData = []
	ninetyData = []
	for(var level in baseMedianData){
		var levelMetrics = {};
		for(var index in baseMedianData[level]){
			if(compMedianData[level].hasOwnProperty(index)){
				var baseMetrics = baseMedianData[level][index];
				var compMetrics = compMedianData[level][index];
				var newMetrics = {};
				for(var i in baseMetrics){
					if(compMetrics.hasOwnProperty(i)){
						newMetrics[i] = calculateValue(baseMetrics[i], compMetrics[i]);
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
						newMetrics[i] = calculateValue(baseMetrics[i], compMetrics[i]);
					}
				}
				levelMetrics[index] = newMetrics;
			}
		}
		ninetyData.push(levelMetrics);
	}

	var stat = $( "#statistic" ).val();
	if(stat === 'median'){ 
		updateLayerData(medianData);
	} else { 	
		updateLayerData(ninetyData);
	}
}

// Function to reset global variables
function resetGlobalVars(){
	medianData = [];
	ninetyData = [];
	metrics = [];
	segmentData = [];
	selectedSegment = null;
	selectedLayer = null;
	levelMetrics = {};
	routesGeojson = [];
	stopsGeojson = [];
	backgroundLayer = [];
	lassoControl = [];
	exportData = [];
	mapCenter = [];
	minTime = 0;
	maxTime = 24;
	selectedFile = null;
	selectedRoutes = [];
	comparisonIndicator = 0;
	selectLinkIndicator = 0;
	selectionIndicator = 0;
	corridorRoutes = {};
	peakDirections = null;
	timepointLookup = {};
	routeList = [];
	activeFilters.length = 0;
}