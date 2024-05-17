/* 
This file contains functions used to prepare the map once new data is loaded.

populateFilters()
addEventHandlers()
createLegend()
getStep(min, max)
setPopup(layer)
sortRoutes(routeList)
recenterView()
zoomToExtents()
goToPreset(presetNumber)
clearExisting()

Note: All functions are called by createMap() in setupFunc.js, which is called in the body of index.html

*/

// Populates filter options once data is loaded
function populateFilters(){

	if(selectLinkIndicator === 1){
		var uniqueRoutes = sortRoutes(routeList);

		// Add predefined selection modes
		$('#selmode').empty();
		$('#selmode').append($("<option></option>").attr("value", "all").text("All flows"));
		$('#selmode').append($("<option></option>").attr("value", "upstream").text("Upstream flows"));
		$('#selmode').append($("<option></option>").attr("value", "downstream").text("Downstream flows"));
		$('#selmode').append($("<option></option>").attr("value", "destination").text("Origins of Alighting Pax"));
		$('#selmode').append($("<option></option>").attr("value", "origin").text("Destinations of Boarding Pax"));
		$('#selmode').append($("<option></option>").attr("value", "transfer").text("Transfer Journeys Only"));
		$('#selmode').val("all");

		if ($("#level option:selected").val() == null){
			var initValue = "stop";
		} else {
			var initValue = $("#level option:selected").val();
		}
		$('#level').empty();
		$('#level').append($("<option></option>").attr("value", "stop").text("Stop"));
		$('#level').append($("<option></option>").attr("value", "tp").text("Timepoint"));
		$("#level").val(initValue);

	} else {
		// Get list of routes in geojson that are also in data 
		var allRoutes = []
		if (comparisonIndicator === 1){
			routeList = Object.keys(compMedianData[1]);
		} else {
			routeList = Object.keys(medianData[1]);
		}
		routesGeojson.eachLayer(function(layer) {
			var layerRoute = layer.options.routeID
			if(routeList.includes(layerRoute +"-0")  || routeList.includes(layerRoute +"-1")){
				allRoutes.push(layerRoute)
			}

			// Add routes from geographic comparison if needed
			if ((typeof layerRoute === 'object') && (layerRoute !== null)){
				if('comp' in layerRoute){
					for(var i in layerRoute['comp']){
						allRoutes.push(layerRoute['comp'][i]);
					}
				} 
				if('base' in layerRoute){
					for(var i in layerRoute['base']){
						allRoutes.push(layerRoute['base'][i]);
					}
				} 
			}
		});
		var uniqueRoutes = [... new Set(allRoutes)] // get only unique values

		// Implement custom sort function to account for possible chars in route ids
		uniqueRoutes = sortRoutes(uniqueRoutes);

		// Add metrics to dropdown menu
		var currentMetrics = levelMetrics['seg'];
		$('#metric').empty();
		for(var i in currentMetrics){
			if (currentMetrics[i] != 'route_id') {
				// console.log(currentMetrics[i])
				$('#metric').append($("<option></option>").attr("value", currentMetrics[i]).text(units[currentMetrics[i]]["label"]));
			}
		}

		// Add pre-defined options to statistic selector
		$('#statistic').empty();
		$('#statistic').append($("<option></option>").attr("value","median").text("Median"));
		$('#statistic').append($("<option></option>").attr("value","ninety").text("Worst Decile"));
		$('#statistic').value = "median";

		// Add pre-defined options to level selector
		$('#level').empty();
		$('#level').append($("<option></option>").attr("value", "seg").text("Stop"));
		$('#level').append($("<option></option>").attr("value", "cor").text("Stop Aggregated"));
		$('#level').append($("<option></option>").attr("value", "rte").text("Route"));
		$('#level').append($("<option></option>").attr("value", "tpSeg").text("Timepoint"));
		$('#level').append($("<option></option>").attr("value", "tpCor").text("Timepoint Aggregated"));
		$('#level').value = "seg";
		
		// Populate individual metric filters
		$("#range-filters").replaceWith('<div id="range-filters"></div>');

		for (var i in metrics){
			var metricVal = metrics[i];
			var metricName = units[metricVal]["label"]
			// var filterNum = $('.filter').length;
			var filterNum = units[metricVal]["order"];

			var metricRange = [];
			routesGeojson.eachLayer(function(layer) {
				if (metricVal == "on_time_performance_perc") {
					metricRange.push(layer.options['rte-' + metricVal]);
				} else {
				metricRange.push(layer.options['seg-' + metricVal]);
				}
			});

			var metricMin = d3.min(metricRange)
			var metricMax = d3.max(metricRange)
			var step = getStep(metricMin, metricMax)
			// console.log(i, metricVal, filterNum, metricName, metricMin, metricMax)
			var rangeFilterText = `<div id = "jfilter${filterNum}" class = "filter">
				<div class="filt-title">
						<div> <span id="js${filterNum}" style="font-weight:bold"> ${metricName} </span> </div>
						<div> <select id="level-sel${filterNum}" class="dropbtn level-select">
							<option value="seg"> By Segment </option>
							<option value="cor"> By Segment (Agg) </option>
							<option value="rte"> By Route </option>
							<option value="tpSeg"> By Timepoint </option>
							<option value="tpCor"> By Timepoint (Agg) </option>
							</select>
						</div>
					</div> <br/>
					<div id = "metric-slider${filterNum}" class= "range-slider"></div> <br/>
				</div>`

			$("#range-filters").append(rangeFilterText);

			// Add slider to metric filter div
			$( "#metric-slider" + filterNum ).slider({
				range: true,
				min: Math.floor(metricMin),
				max: Math.ceil(metricMax),
				step: step,
				tooltip: 'always',
				tooltip_split: true
			});
			$( "#metric-slider" + filterNum ).slider("setValue", [ Math.floor(metricMin), Math.ceil(metricMax) ]);

			// Add metric as option to "visualize by" dropdown menu
			$('#viz-by')
				.append($("<option></option>")
				.attr("value",i)
				.text(metricName));

			// Add metric filter to custom filter step panel
			var filterStepInput = 
				`<label for="${filterNum}-step-input"> ${metricName} Step </label> <br>
				<input type="text" id="${filterNum}-step-input" class="form-control-xs" style="width: 100px;"> <br>`
			$("#filter-ctrl").append(filterStepInput);
		};

		// Changes text of preset buttons if preset file exists
		if (!(presets === 0)){
			var name0 = presets[0]['name']
			var name1 = presets[1]['name']
			var name2 = presets[2]['name']
			if (!(name0 == null)){
				$('#button-preset-0').text(presets[0]['name'])
			}
			if (!(name1 == null)){
				$('#button-preset-1').text(presets[1]['name'])
			}
			if (!(name2 == null)){
				$('#button-preset-2').text(presets[2]['name'])
			}

			// Defines action when preset scenario button 0 clicked
			$('#button-preset-0').click(function(){
				goToPreset(0)
			});

			// Defines action when preset scenario button 1 clicked
			$('#button-preset-1').click(function(){
				goToPreset(1)
			});

			// Defines action when preset scenario button 2 clicked
			$('#button-preset-2').click(function(){
				goToPreset(2)
			});
		};
	
		// Add radio buttons for absolute values and percentages
		if(comparisonIndicator === 1){

			var comparisonButton = `
			<div id="radio-container" class="metrics-element dropdown-container">	
			<div class="form-check">
				<input class="radio-button" type="radio" name="comp-radio" id="radio-absolute" value="absolute" checked>
				<label class="radio-label" for="radio-absolute">  Absolute Values  </label>
			</div>
			<div class="form-check">
				<input class="radio-button" type="radio" name="comp-radio" id="radio-percent" value="percent">
				<label class="radio-label" for="radio-percent">  Percentages </label>
			</div>
			</div>`
			$("#radio-container").replaceWith(comparisonButton);
		}
	}
	
	// Default to all routes selected
	selectedRoutes = 'All'

	// Add routes to route selection dropdown and default to all selected
	$('#choose-routes').empty();
	$('#choose-groups').empty();
	$('#choose-routes').append($("<option></option>").attr("value", 'All').attr('selected', "selected").text("Select All  "));

	// Add option to select routes by garage and by route type, then by individual route
	for(var i in garageAssignments){
		$('#choose-groups').append($("<option></option>").attr("value", i).text(i));
	}
	for(var i in routeTypes){
		$('#choose-groups').append($("<option></option>").attr("value", i).text(i));
	}
	for(var i in uniqueRoutes){
		$('#choose-routes').append($("<option></option>").attr("value", uniqueRoutes[i]).text(uniqueRoutes[i]));
	}

	// Add pre-defined options to direction selector
	$('#direction').empty();
	$('#direction').append($("<option></option>").attr("value", "both").text("Both"));
	$('#direction').append($("<option></option>").attr("value", 0).text(directionLabels[0]));
	$('#direction').append($("<option></option>").attr("value", 1).text(directionLabels[1]));
	if(!(peakDirections['full'] === undefined)){ // If peak direction available for full day, add it
		$('#direction').append($("<option></option>").attr("value", "peak").text("Peak"));
	};
	$('#direction').val("both");

	// Add time periods defined in config file to the options for the period dropdown
	$('#time-period').empty();
	for(var i in timePeriods){
		var periodID = timePeriods[i];
		$('#time-period').append($("<option></option>").attr("value", periodID).text(periodNames[periodID]));
	}
}

function populateEquityBackgroundFilters(){
	// console.log(backgroundLayer)
	var firstKey = Object.keys(backgroundLayer._layers)[0]
	var firstObjProperties = Object.keys(backgroundLayer._layers[firstKey].feature.properties)
	// console.log(firstKey)
	// console.log(firstObjProperties)
	$("#range-toggle-bkgrd").show()
	$("#bkgrd-select").show()
	$("#range-filters-bkgrd").replaceWith('<div id="range-filters-bkgrd"></div>');
	var bkgrd_metrics = ['pct_dis', 'pct_poc', 'pct_pov']
	var bkgrd_metrics_names = ['% Disability', '% People of Color', '% Poverty']

	const includeall = bkgrd_metrics.every(value => {
		return firstObjProperties.includes(value);
	});
	// console.log('includeall ' + includeall)
	if (includeall){

		$('#bkgrd-filter').empty();
		$('#bkgrd-filter').append($("<option></option>").attr("value", 0).text(bkgrd_metrics_names[0]));
		$('#bkgrd-filter').append($("<option></option>").attr("value", 1).text(bkgrd_metrics_names[1]));
		$('#bkgrd-filter').append($("<option></option>").attr("value", 2).text(bkgrd_metrics_names[2]));
		$('#bkgrd-filter').append($("<option></option>").attr("value", 3).text('None'));

		$("#bkgrd-filter").val('3');
		
		// Defines action for direction dropdown
		$( '#bkgrd-filter' ).change(function() {
			var selected = parseInt($("#bkgrd-filter option:selected").val())
			// console.log(selected)

			var colorQuantile = d3.scaleQuantile()
            	.domain([0, 1])
            	.range(rangeBlue);

			
			if (selected < 3){
				$('#legends-2').show()
				// $("#legend-svg-bkgrd").show()
				legendDef.range(rangeBlue)
				legendDef.domain([0, 1])

				var threshold = d3.scaleThreshold()
						.domain([0, 1])
						.range([0, 1]);
					
				var bkgrdLegendSVG = d3.select("#legend-svg-bkgrd");
				bkgrdLegendSVG.append("g")
					.attr("id", "bus-legend-bkgrd")
					.attr("class", "legendQuant")
					.attr("transform", "translate(0,25)")
					.attr("fill","white");
				var legend = d3.legendColor()
					.title(bkgrd_metrics_names[selected])
					.labels(d3.legendHelpers.thresholdLabels)
					.titleWidth(0.15*innerWidth)
					.scale(legendDef);
					bkgrdLegendSVG.select("#bus-legend-bkgrd").call(legend);
		
				backgroundLayer.eachLayer(function(layer){
					var prop = bkgrd_metrics[selected]
					var propValue = layer.feature.properties[prop]
					// console.log(propValue, colorQuantile(propValue))
					layer.setStyle({
						weight: 0.7,
						opacity: 1,
						fillOpacity : 0.3,
						color: colorQuantile(propValue)
					});
				});
			} else {
				var polyStyle = {
					"color": "#ff00ff", // pick a color not part of metrics color scheme
					"weight": 1,
					"opacity": 0.7,
					"fillOpacity" : 0.3
				};

				backgroundLayer.eachLayer(function(layer){
					layer.setStyle(polyStyle)
				});
			}
		});

		// if (firstObjProperties.includes(metric)){
		// 	console.log('has ' + metric)
		// 	var rangeFilterText = `<div id = "jfilter_${metric}" class = "filter">
		// 		<div class="filt-title">
		// 				<div> <span id="js_${metric}" style="font-weight:bold"> ${bkgrd_metrics_names[i]} </span> </div>
		// 			</div> <br/>
		// 			<div id = "metric-slider_${metric}" class= "range-slider-bkgrd"></div> <br/>
		// 		</div>`

		// 	$("#range-filters-bkgrd").append(rangeFilterText);

		// 	// Add slider to metric filter div
		// 	$( "#metric-slider_" + metric).slider({
		// 		range: true,
		// 		min: 0,
		// 		max: 1,
		// 		step: 0.1,
		// 		tooltip: 'always',
		// 		tooltip_split: true
		// 	});
		// 	$( "#metric-slider_" + metric ).slider("setValue", [ 0, 1 ]);

		// };
	};

	
}

// Defines the actions for the hotkeys and buttons once data is loaded
function addEventHandlers(){
	
	// Hotkey functions
	$(document).keypress(function(e){
		if (e.keyCode == 67){ // c= center
			recenterView();
		}
		if (e.keyCode == 90){// z = zoom to extents
			zoomToExtents();
		}
	});

	// Define zoom to extents button action
	$('#button-extent').click(function(){
		// Zoom map to extents of geojson
		zoomToExtents();
	})

	// Define recenter view button action
	$('#button-center').click(function(){
		recenterView();
	})

	// Defines action for level dropdown
	$( '#level' ).change(function() {
		if(selectLinkIndicator === 1){
			var selectedLevel = $("#level option:selected").val();
			var selectedPeriod = $("#time-period option:selected").val();
			var requestInput = {'period' : selectedPeriod, 'file': selectedFile, 'level':selectedLevel};
			map.removeLayer(routesGeojson); // Delete current shapes
			map.removeControl(lassoControl); // Remove current lasso (new one will be drawn)
			var newText = "<span id = 'sel-level' style='color:white'>"+$("#level option:selected").text()+"</span>";
			requestSelectLinkData(requestInput, true); // newMap=true because we need new shapes drawn
			$("#sel-level").replaceWith(newText);
		} else {
				
			// Set all of the filter level dropdowns to the newly selected level
			var count = 0;
			var newLevel = $( "#level" ).val();
			for (var i in metrics){
				$( "#level-sel" + count ).val( newLevel );
				count += 1;
			}
			activeFilters.length = 0;

			// Update the list of metrics available for visualization
			$('#metric').empty();
			var currentMetrics = levelMetrics[newLevel];
			for(var i in currentMetrics){
				$('#metric').append($("<option></option>").attr("value", currentMetrics[i]).text(units[currentMetrics[i]]["label"]));
			}

			updateMetricFilters();
			redrawShapes();
			updateFiltersTable();
			updateLegend();
			var newtext = "<span id='sel-lev' style='color:white'>"+$("#level option:selected").text()+"</span>"
			$("#sel-lev").replaceWith(newtext);
			routesGeojson.eachLayer(function(layer) {
				setPopup(layer);
			});
		}
	});

	// Time period select event handle
	$("#time-period").on('change', function() {
		var selectedPeriod = $("#time-period option:selected").val();
		if(selectedPeriod != 'custom'){

			$("#time-period option[value='custom']").remove(); // remove custom option
			// Update time based on selected period, not custom time range
			updateTime(0, 0, selectedPeriod);
		
			if(selectLinkIndicator === 0){
				// Time period select event handle
				if(selectedPeriod != 'custom'){
					var selectedRange = periodRanges[selectedPeriod];
					$("#time-slider").slider('setValue', selectedRange); // reset the time slider to avoid confusion

				}
			}
		}
	});

	if(selectLinkIndicator === 0){
		// Defines action for metric dropdown
		$( '#metric' ).change(function() {
			updateColorScheme();
			redrawShapes();
			updateLegend();
		});

		// Defines action for statistic dropdown
		$( '#statistic' ).change(function() {

			// Update the metric data, then draw shapes and legend accordingly
			activeFilters.length = 0;
			updateMetrics();
			updateMetricFilters();
			redrawShapes();
			updateFiltersTable();
			updateLegend();

			// Replace text shown in the left hand panel
			var newtext = "<span id='sel-stat' style='color:white'>"+$("#statistic option:selected").text()+"</span>";
			$("#sel-stat").replaceWith(newtext);
		});

		// Reset filter button action
		$("#button-reset").click(function(){
			$('.range-slider').each(function(){
				$(this).slider("setValue", [$(this).data("slider").getAttribute("min"), $(this).data("slider").getAttribute("max")]);
			});
			activeFilters.length = 0;
			redrawShapes();
			updateFiltersTable();
		});

		// Time filter calculate button event handle
		$("#time-calc").click(function() {

			// Create new option "custom" for the time period dropdown menu to blank to avoid confusion
			$('#time-period').append($("<option></option>").attr("value", "custom").text("Custom")); 
			$('#time-period').val("custom");

			// Update time based on custom time range
			var minHour = Math.floor($(".time-slider").val()[0]);
			var minMinute = Math.floor(($(".time-slider").val()[0] % 1) * 60);
			var maxHour = Math.floor($(".time-slider").val()[1]);
			var maxMinute = Math.floor(($(".time-slider").val()[1] % 1) * 60);
			if (maxHour === 24){ // Time must be between 0:00 - 23:59
				maxHour = 23;
				maxMinute = 59;
			} ;
			updateTime([minHour, minMinute], [maxHour, maxMinute]);
			// updateFiltersTable();
		});

		// Range filter event handle
		$('.range-slider').each(function(){
			$(this).slider().on('slideStop', function(){
				var filterNum = this.id.substring(this.id.length - 1, this.id.length);
				// Get metric value and add to active filters
				var metricName = metrics[filterNum]; 
				// console.log(filterNum, metricName)
				if (!activeFilters.includes(metricName)){
					activeFilters.push(metricName);
				}
				redrawShapes();
				updateFiltersTable();

				// If this is the currently visualized metric, update the legend
				if (metricName === $( "#metric" ).val()){
					updateLegend();
				};
			});
		});

		// Defines what happens when dropdown in metric filter is changed -- updates all filters
		$('.level-select').each(function(){
			$(this).on('change', function() {
				updateMetricFilters();
				updateFiltersTable();
			});
		});

		// When the custom legend bins buttom is clicked, call function to update legend
		$('#button-legend-update').on('click', function(){

			var firstMax = $('#first-bin-input').val();
			var secondMax = $('#second-bin-input').val();
			var thirdMax = $('#third-bin-input').val();
			var fourthMax = $('#fourth-bin-input').val();

			var domain = [firstMax, secondMax, thirdMax, fourthMax];

			// Check that entries are valid, ascending numbers
			for(var i = 0; i < domain.length; i++){
				if(isNaN(parseFloat(domain[i]))){
					domain[i] = domain[i-1];
				} else if(parseFloat(domain[i]) < domain[i-1]){
					domain[i] = domain[i-1];
				} else {
					domain[i] = parseFloat(domain[i]);
				}			
			}
			customLegend(domain);
		});

		// Legend reset button
		$('#button-legend-reset').on('click', function(){
			redrawShapes();
			updateLegend();
			
			$('.form-control-sm').each(function(){
				$(this).val('');
			});
		});

		// Custom filter steps button
		$('#button-filter-update').on('click', function(){
			for (var i in metrics){
				var customStep = parseInt($('#'+i+"-step-input").val());
				if(customStep === undefined){
					continue
				} else if(customStep < $('#metric-slider' + i).data('slider').options.max){ // Step shouldn't be > entire range
					$( "#metric-slider" + i ).slider({step:customStep});
				}
			}
		});

		// Filter reset button
		$('#button-filter-reset').on('click', function(){
			updateMetricFilters();
			$('.form-control-xs').each(function(){
				$(this).val('');
			});
		});

		// Radio button for comparison mode
		$('[id^="radio"]').on('change', function () {
			updateValues();
			updateMetricFilters();
			redrawShapes();
			updateFiltersTable();
			updateLegend();
		});

		// When range slider level is changed, update the range of that filter
		var count = 0;
		for (var i in metrics){
			$( "#level-sel" + count ).on('change', function () {
				updateMetricFilters();
			});
			count += 1;
		}
	}
	
	// Defines action for direction dropdown
	$( '#direction' ).change(function() {
		var newText = "<span id='sel-dir' style='color:white'>"+$("#direction option:selected").text()+"</span>";
		$("#sel-dir").replaceWith(newText);
		if(selectLinkIndicator === 1){
			redrawSelectLinkShapes();
		} else {
			redrawShapes();
			updateLegend();
		}
		activeFilters.push(0);
	});

	// Defines action for route select dropdown
	$('#choose-routes').on('click', function() {

		var selectedList = $("#choose-routes").val();
		selectedRoutes = selectedList;

		// Change the visualization to reflect new selection
		if(selectLinkIndicator === 1){
			redrawSelectLinkShapes();
		} else {
			redrawShapes();
		}

		// Update "selected route" text in the LH panel
		var selectedNum = $("#choose-routes option:selected").length;

		// Count all routes if "select all" is chosen (supercedes previous)
		if($("#choose-routes option:selected").val().includes("All")){
			selectedNum = $("#choose-routes option").length - 1;
		} else {
			activeFilters.push(0);
		}
		
		// Update text
		var newText = "<span id='sel-num' style='color:white'>" + selectedNum + "</span>";
		$("#sel-num").replaceWith(newText);
	});

	// Defines action for route group select dropdown
	$('#choose-groups').on('click', function() {

		var selectedNum = $("#choose-groups option:selected").length;
		var selectedList = $.map($("#choose-groups :selected"), function(option) { return option.value });

		// If a garage or route type is selected, add any unselected routes to count
		for(var garage in garageAssignments){	
			if(selectedList.includes(garage)){
				selectedNum -= 1; // Subtract the garage group item
				for(var i in garageAssignments[garage]){
					if(!selectedList.includes(garageAssignments[garage][i])){ // Only add if not already selected
						selectedNum += 1;
						selectedList.push(garageAssignments[garage][i]);
					};
				};
			};	
		};
		for(var type in routeTypes){	
			if(selectedList.includes(type)){
				selectedNum -= 1; // Subtract the route type group item
				for(var i in routeTypes[type]){
					if(!selectedList.includes(routeTypes[type][i])){ // Only add if not already selected
						selectedNum += 1;
						selectedList.push(routeTypes[type][i]);
					};
				};
			};		
		};

		selectedRoutes = selectedList;
		if(selectLinkIndicator === 1){
			redrawSelectLinkShapes();
		} else {
			redrawShapes();
		}

		// Update text
		var newText = "<span id='sel-num' style='color:white'>" + selectedNum + "</span>";
		$("#sel-num").replaceWith(newText);
	});


	// Defines action for the data export button
	$('#export-btn').each(function(){
		$(this).on('click', function() {

			// Sort export data so selected segments appear first
			function Comparator(a, b) {
				if (a[a.length-1] < b[b.length-1]) return 1;
				if (a[a.length-1] > b[b.length-1]) return -1;
				return 0;
			}
			var sortedData = exportData.sort(Comparator);
			console.log("Data:", sortedData)

			if(selectLinkIndicator === 1){
				// Get GTFS lookup table for stop names and municipalities (if available)
				var lookupTable = getLookupTable(selectedFile[0]);
				var selectedLevel = $("#selmode").val();

				// Write attributes to csv header
				if (lookupTable === 0){
					var lookupIndicator = 0;
					if(selectedLevel === 'tp'){
						var csv = 'Route,Direction,Mode,First Stop ID,Last Stop ID,Passenger Flow,Selected';
					} else {
						var csv = 'Route,Direction,Mode,Stop ID,Passenger Flow,Selected';
					}
				} else {
					var lookupIndicator = 1;
					if(selectedLevel === 'tp'){
						var	csv = 'Route,Direction,Mode,First Stop ID,First Stop Name,Last Stop ID,Last Stop Name,Passenger Flow,Selected';
					} else {
						var	csv = 'Route,Direction,Mode,Stop ID,Stop Name,Passenger Flow,Selected';
					}
				}
				csv += "\n";

				// Write values to the rows of the CSV export
				sortedData.forEach(function(row) {

					if (lookupIndicator === 1){
						var firstStopID = row[3];
						try {
							var firstStopName = lookupTable[firstStopID]['stop_name'];
						} catch(err) {
							var firstStopName = 'N/A';
						}
						row.splice(4, 0, firstStopName);

						if(selectedLevel === 'tp'){
							var lastStopID = row[4];
							try {
								var lastStopName = lookupTable[lastStopID]['stop_name'];
							} catch(err) {
								var lastStopName = 'N/A';
							}
							row.splice(6, 0, lastStopName);
						}
					};

					csv += row.join(',');
					csv += "\n";
				});
			} else {
				// Get value of level selector
				var newLevel = $( "#level" ).val();

				// Get GTFS lookup table for stop names and municipalities (if available)
				var lookupTable = getLookupTable(selectedFile[0]);
				
				// Write attributes to csv header
				if (lookupTable === 0){
					var csv = 'Route,Direction,'
					if(!(newLevel === 'rte')){
						csv += 'First Stop ID,Last Stop ID,Signal,';
					};
					var lookupIndicator = 0;
				} else {
					var csv = 'Route,Direction,'
					if(!(newLevel === 'rte')){
						csv += 'First Stop ID,First Stop Name,First Stop City,Last Stop ID,Last Stop Name,Last Stop City,Signal,';
					};
					var lookupIndicator = 1;
				}

				// Write metric names to csv header
				Object.values(levelMetrics[newLevel]).forEach(function(row) {
					csv += units[row]["label"];
					csv += ',';
				});

				// If in comparison mode, also add baseline and comparison period metrics to the header
				if (comparisonIndicator === 1){
					Object.values(levelMetrics[newLevel]).forEach(function(row) {
						csv += 'Baseline ';
						csv += units[row]["label"];
						csv += ',';
					});
					Object.values(levelMetrics[newLevel]).forEach(function(row) {
						csv += 'Comparison ';
						csv += units[row]["label"];
						csv += ',';
					});
				}
				csv += "\n";
				
				// Write values to the rows of the CSV export
				sortedData.forEach(function(row) {
					// If lookup table exists, add stop details to export file
					if (lookupIndicator === 1 && !(newLevel === 'rte')){
						var firstStopID = row[2];
						var lastStopID = row[3];
						try {
							var firstStopName = lookupTable[firstStopID]['stop_name'];
							var lastStopName = lookupTable[lastStopID]['stop_name'];
							var firstStopCity = lookupTable[firstStopID]['municipality'];
							var lastStopCity = lookupTable[lastStopID]['municipality'];
						} catch(err) {
							var firstStopName = 'N/A';
							var lastStopName = 'N/A';
							var firstStopCity = 'N/A';
							var lastStopCity = 'N/A';
						}

						row.splice(3, 0, firstStopName);
						row.splice(4, 0, firstStopCity);
						row.splice(6, 0, lastStopName);
						row.splice(7, 0, lastStopCity);
					};
					// If corridor mode, add all routes
					if (newLevel === 'cor'){
						row[0] = corridorRoutes[row[2]+'-'+row[5]].join("; "); 
					};

					csv += row.join(',');
					csv += "\n";
				});
			}

			// Export to CSV
			var hiddenElement = document.createElement('a');			
			var csvData = new Blob([csv], { type : "application/csv;charset=utf-8;" }); 
			var csvUrl = URL.createObjectURL(csvData);
			hiddenElement.href = csvUrl;
			hiddenElement.download = 'ROVE_export.csv';
			hiddenElement.click();

		});
	});

	// Defines action for the image export button
	$('#export-img-btn').each(function(){
		$(this).on('click', function() {
			leafletImage(map, prepareImage);
		});
	});
}

// Generates legend and other info which is displayed in the LH panel once data is loaded
function createLegend(){

	// Lookup metadata from config file
	if(comparisonIndicator === 0){
		var dataDescription = '<div id="selection-table"><p style="color:#9ec1bd; margin-bottom:0px"> Data From: <span id = "date-range" style="color:white">' 
			+ transitFileDescription[selectedFile] + '<br> </span> </p>'
	} else {
		var dataDescription = '<div id="selection-table"><span id = "date-range" style="color:white">' + transitFileDescription[selectedFile[1]] 
			+ " <i> minus </i> " + transitFileDescription[selectedFile[0]] + '<br> </span>'
	}

	// Get initial selection of routes, direction and statistic
	var numRoutes = Math.max($("#choose-routes option").length - Object.keys(garageAssignments).length - Object.keys(routeTypes).length - 1, 0); // Subtract group options unless < 0
	var initDirection = $("#direction option:selected").text();
	var initStat = $("#statistic option:selected").text();
	var initLevel = $("#level option:selected").text();
	var initPeriod = $("#time-period option:selected").text();

	// Populate divs and text for presenting selection
	var metaText = dataDescription;
	metaText += '<p style="color:#9ec1bd; margin-bottom:0px"> Routes Selected: <span id = "sel-num" style="color:white">' + numRoutes + '</span> </p>';
	metaText += '<p style="color:#9ec1bd; margin-bottom:0px"> Level Selected: <span id = "sel-lev" style="color:white">' + initLevel + '</span> </p>';
	metaText += '<p style="color:#9ec1bd; margin-bottom:0px"> Direction Selected: <span id = "sel-dir" style="color:white">' + initDirection + '</span> </p>';
	metaText += '<p style="color:#9ec1bd; margin-bottom:0px"> Statistic Selected: <span id = "sel-stat" style="color:white">' + initStat + '</span> </p>';
	metaText += '<p style="color:#9ec1bd"> Time Selected: <span id = "sel-time" style="color:white">' + initPeriod + '</span> </p>';
	metaText += '<p style="color:#9ec1bd; margin-bottom:0px"> Filters: </p> <div id="view-table" ></div>';
	metaText += '<div id="filter-table" ></div></div>';
	$("#selection-table").replaceWith(metaText);

	// Populate network-wide metrics
	// var congestionDelay = ;
	// var avgCrowding = ;
	// var avgSpeed = ;
	// $("#network-metrics").append('<p style="color:#9ec1bd; margin-bottom:0px"> Total Congestion Delay: <span id = "date-range" style="color:white">' + congestionDelay + '<br> </span> </p>');
	// $("#network-metrics").append('<p style="color:#9ec1bd; margin-bottom:0px"> Average Crowding: <span id = "date-range" style="color:white">' + avgCrowding + '<br> </span> </p>');
	// $("#network-metrics").append('<p style="color:#9ec1bd; margin-bottom:0px"> Average Speed: <span id = "date-range" style="color:white">' + avgSpeed + '<br> </span> </p>');

	// Get first metric from bus data array for legend initialization
	var initMetric = levelMetrics[$("#level option:selected").val()][0];
	var initRange = [];
	routesGeojson.eachLayer(function(layer) {
		initRange.push(layer.options['seg-'+initMetric])
	});

	// Init legend, ensure scale has correct domain + range
	var legendSVG = d3.select("#legend-svg");

	// Add "Difference in ..." prefix to legend title if comparison selected
	if(comparisonIndicator === 1){
		var metricName = "Difference in " + units[initMetric]["label"];
	} else {
		var metricName = units[initMetric]["label"];
	}

	legendDef.domain(initRange);
	updateColorScheme();

	// Define legend attributes and call
	legendSVG.append("g")
		.attr("id", "bus-legend")
		.attr("class", "legendQuant")
		.attr("transform", "translate(0,25)")
		.attr("fill","white");
	var legend = d3.legendColor()
		.title(metricName)
		.labels(d3.legendHelpers.thresholdLabels)
		.titleWidth(0.15*innerWidth)
		.scale(legendDef);
	legendSVG.select("#bus-legend").call(legend);

	// Populate filter state information in the table below legend
	var infoText = `<div id='filter-table'><table class = 'attr-table2' style='font-size:10pt;'><tr>
		<th align = 'left' style='color:#9ec1bd'>Metric</th>
		<th align = 'left' style='color:#9ec1bd'>Range</th></tr>`;

	for (var i in metrics){
		var metricValue = metrics[i];
		var metricName = units[metricValue]["label"];

		var metricRange = [];
		routesGeojson.eachLayer(function(layer) {
			if (metricValue == "on_time_performance_perc") {
				metricRange.push(layer.options['rte-' + metricValue]);
			} else {
			metricRange.push(layer.options['seg-' + metricValue]);
			}
		});

		var metricMin = d3.min(metricRange);
		var metricMax = d3.max(metricRange);

		if( isNaN(metricMin) ) { metricMin = 0; };
		if( isNaN(metricMax) ) { metricMax = 0;	};

		infoText = infoText + "<tr><td style='width: 185px;''>"
			+ metricName + "</td><td>"
			+ Number(metricMin).toFixed(0) + " - " + Number(metricMax).toFixed(0)
			+ "</td></tr>";
	}
	
	infoText += "</table></div>";
	$("#filter-table").replaceWith(infoText);
}

// Get an appropriate default step value for the slider given a range
// Used personal judgement for these definitions -- might benefit from some better algorithm
function getStep(min, max){
	var spread = max - min;
	var step = 0.1;
	if(spread > 5000){
		step = 500;
	} else if(spread > 2000){
		step = 250;
	} else if(spread > 1000){
		step = 100;
	} else if(spread > 500){
		step = 50;
	} else if(spread > 200){
		step = 25;
	} else if(spread > 100){
		step = 10;
	} else if(spread > 40){
		step = 5;
	} else if(spread > 20){
		step = 2;
	} else if(spread > 10){
		step = 1;
	} else if(spread > 2){
		step = 0.5;
	}
	return step
}

// Function to add or change popups
function setPopup(layer) {
    layer.unbindPopup();

    let level = $("#level").val() || 'seg';
    let levelText = $("#level option:selected").text() || "Segment";
    let route = layer.options.routeID;
    let popText = '';

    function formatRoutes(routeData, type) {
        let routes = routeData.map(r => r.toString()).join(', ');
        return `</br> ${type}:</b> ${routes}<b>`;
    }

    function formatDirection(directions, type) {
        return `</br> ${type} Direction: ${[...new Set(directions)].toString()}`;
    }

    function formatStopPairs(stops, type) {
        let stopPairs = stops.map(s => `(${s})`).join(', ');
        return `</br> ${type} Stop Pairs: ${stopPairs}`;
    }

    function formatMetrics(metrics, options, level) {
        let text = '';
        metrics.forEach(m => {
            let metricValue = levelMetrics[level][m];
				if ((comparisonIndicator == 0 && (((metricValue != 'route_id') && (metricValue in units)))) || comparisonIndicator == 1) {
            		let metricName = units[metricValue]["label"];
				}
            let value = options[level + '-' + metricValue];
            if (typeof value !== 'undefined') {
                value = Math.abs(value) < 100 && value % 1 > 0 ? value.toFixed(1) : value.toFixed(0);
                text += `${metricName}: ${value}</br>`;
            }
        });
        return text;
    }

    if ((typeof route === 'object') && (route !== null)) {
        let routeText = '', dirText = '', segText = '';

        if (route.base) {
            routeText += formatRoutes(route.base, 'Base');
            dirText += formatDirection(layer.options.directionID.base, 'Base');
            segText += formatStopPairs(layer.options.startStop, 'Base');
        }

        if (route.comp) {
            routeText += formatRoutes(route.comp, 'Comp');
            dirText += formatDirection(layer.options.directionID.comp, 'Comp');
            segText += formatStopPairs(layer.options.endStop, 'Comp');
        }

        popText = `<p><b> Routes: ${routeText}</b>${dirText}${segText}</br> Note: ${levelText} Level Metrics Listed</br></br>`;
        popText += formatMetrics(levelMetrics[level], layer.options, level);
    } else {
        // Simplified corridor mode and default popup text generation
        if (level === 'cor' && corridorRoutes[layer.options.corIndex]) {
            route = corridorRoutes[layer.options.corIndex].toString();
        }

        popText = `<p><b> Route: ${route}</b></br>Direction: ${directionLabels[layer.options.directionID]}</br>First Stop: ${layer.options.startStop}</br>Last Stop: ${layer.options.endStop}</br>Note: ${levelText} Level Metrics Listed</br></br>`;
        popText += formatMetrics(levelMetrics[level], layer.options, level);
    }

    layer.bindPopup(popText, { maxWidth: 350 });
}

function zoomToExtents(){
	map.flyTo(mapCenter, 11);
}

function recenterView(){
	map.flyTo(mapCenter, map.getZoom());
}

// Function to sort route IDs that may or may not contain characters
function sortRoutes(routeList){

	// Function to find the closest value to input in an array
	function closest(searchValue, searchList) {
		return searchList.reduce((a, b) => {
			let aDiff = Math.abs(a - searchValue);
			let bDiff = Math.abs(b - searchValue);
	
			if (aDiff == bDiff) {
				return a < b ? a : b;
			} else {
				return bDiff < aDiff ? b : a;
			}
		});
	}

	// Split into 3 lists: 1) routes with leading characters, 2) with trailing characters, 3) entirely numeric
	var leadingCharRoutes = [];
	var trailingCharRoutes = [];
	var numericalRoutes = [];
	for(var i in routeList){
		var routeID = routeList[i];
		var routeNumber = Number(routeID);
		if(!Number.isNaN(routeNumber)){
			numericalRoutes.push(routeID);
		} else {
			if(Number.isNaN(Number(routeID[0]))){
				leadingCharRoutes.push(routeID);
			} else {
				trailingCharRoutes.push(routeID);
			}
		}
	}

	// Simple sort for numerical routeIDs, add to output list
	var sortedList = numericalRoutes.sort((a, b) => a - b);

	// For trailing char routeIDs, strip trailing chars then insert at appropriate position in sortedList
	// for(var i in trailingCharRoutes){
	// 	var origRouteID = trailingCharRoutes[i];
	// 	var numRouteID = origRouteID;
	// 	while(Number.isNaN(Number(numRouteID))){
	// 		numRouteID = numRouteID.slice(0, -1);
	// 	}
	// 	var closestValue = closest(numRouteID, sortedList);
	// 	var closestIndex = sortedList.indexOf(closestValue) + 1; // insert after closest value
	// 	sortedList.splice(closestIndex, 0, origRouteID);
	// }

	// Append leading char routes to the end of the list
	sortedList.push(...leadingCharRoutes.sort());
	sortedList.push(...trailingCharRoutes.sort());
	return sortedList
}

function goToPreset(buttonNumber){
	var scenarioSettings = presets[buttonNumber];
	if (!(scenarioSettings == null)){
		// If preset value is different from current value, then change
		if (!($('#choose-groups').val() === scenarioSettings['routes'])){
			$('#choose-groups').val(scenarioSettings['routes']).change();
		};
		if (!($('#level').val() === scenarioSettings['level'])){
			$('#level').val(scenarioSettings['level']).change();
		};
		if (!($('#statistic').val() === scenarioSettings['statistic'])){
			$('#statistic').val(scenarioSettings['statistic']).change();
		};
		if (!($('#direction').val() === scenarioSettings['direction'])){
			$('#direction').val(scenarioSettings['direction']).change();
		};
		if (!($('#time-period').val() === scenarioSettings['time_period'])){
			$('#time-period').val(scenarioSettings['time_period']).change();
		};
		if (!($('#metric').val() === scenarioSettings['metric'])){
			$('#metric').val(scenarioSettings['metric']).change();
		};
	};
	$("#button-tool").trigger("click");
}

function clearExisting(){
	map.removeLayer(backgroundLayer);
	map.removeLayer(routesGeojson);
	map.removeLayer(stopsGeojson);
	if(lassoControl._map != null){
		map.removeControl(lassoControl);
	}
	d3.selectAll("svg > *").remove();
	$("#segment-info").empty();
	$("#selection-table").empty();
	resetGlobalVars();

	$('#metric').empty();
	$('#direction').empty();
	$('#statistic').empty();
	$('#level').empty();
	$('#choose-groups').empty();
}