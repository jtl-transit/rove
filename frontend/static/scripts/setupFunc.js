/* Initialization functions:

initializeDataPanel()
createMap()
prepareImage()
appendMetricsToLine()
displaySegments(e)

Notes:
InitializeDataPanel() is called directly from index.html
createMap() is called when 'visualize' button is clicked -- this action defined in initializeDataPanel()

*/

// Main function called in index.html to set up right hand side panel. Is called when dashboard is first initialized.
// Adds static elements to the map that are not dependent on any metric data being loaded.
function initializeDataPanel(){
	console.log('initializeDataPanel')
	// Drop journey visualization buttons if WMATA instance (not supported)
	if(transitFileDescription[0].slice(0,5) === 'WMATA'){
		$('#button-viz').hide();
		$('#button-viz-compare').hide();
		map.setView([38.889362, -77.035246], 12); // Set to show DC upon loading
	}

	// Defines action when performance metrics - single period button is clicked
	$('#select-transit-button').click(function(){
		
		// If file already selected, reset global variables and remove any transit layers
		if(selectedFile !== null){
			clearExisting();
		};

		// Get metric data from the server - full day period as default on startup
		selectedFile = $('#select-transit').val();
		var selectedPeriod = {'custom_range': null, 'predefined': 'full', 'file': selectedFile};
		requestDataFromServer(selectedPeriod, newMap = true);

		// Remove and restore elements to get metrics-single period UI
		$('.journey-element').hide();
		$('.metrics-element').show();
		$("#button-tool").trigger("click");
		$( ".time-slider" ).slider('setValue', [0, 24]);
	});

	// Defines action when period comparison is selected
	$('#comparison-button').click(function(){

		// If file already selected, reset global variables and remove any transit layers
		if(selectedFile !== null){
			clearExisting();
		};

		comparisonIndicator = 1;

		var baselineFile = $('#select-baseline').val();
		var comparisonFile = $('#select-comparison').val();
		selectedFile = [baselineFile, comparisonFile];

		var baselinePeriod = {'custom_range': null, 'predefined' : 'full', 'file': baselineFile};
		var comparisonPeriod = {'custom_range': null, 'predefined' : 'full', 'file': comparisonFile};
		comparePeriods(baselinePeriod, comparisonPeriod, newMap = true);

		// Remove and restore elements to get metrics-comparison UI
		$('.journey-element').hide();
		$('.metrics-element').show();
		$("#button-tool").trigger("click");
		$( ".time-slider" ).slider('setValue', [0, 24]);
	});

	// Defines action when journey visualization button is clicked
	$('#select-viz-button').click(function(){

		// If file already selected, reset global variables and remove any transit layers
		if(selectedFile !== null){
			clearExisting();
		};
		selectLinkIndicator = 1;
		selectedFile = $('#select-viz').val();

		// Get metric data from the server - full day period and stop resolution are defaults on startup
		var selectedPeriod = {'period' : 'full', 'file': selectedFile, 'level': 'stop'};
		requestSelectLinkData(selectedPeriod, newMap = true);

		// Remove and restore elements to get journey-single period UI
		$('.metrics-element').hide();
		$('.journey-element').show();
		$('#load-period-data').hide();
		$("#button-tool").trigger("click");
	});

	// Defines action when journey visualization button is clicked
	$('#viz-comparison-button').click(function(){

		// If file already selected, reset global variables and remove any transit layers
		if(selectedFile !== null){
			clearExisting();
		};
		selectLinkIndicator = 1;
		comparisonIndicator = 1;
		var baseFile = $('#viz-baseline').val();
		var compFile = $('#viz-comparison').val();
		selectedFile = [baseFile, compFile];

		// Get metric data from the server - full day period and stop resolution are defaults on startup
		var selectedPeriod = {'base_period' : baseFile, 'comp_period': compFile, 'level': 'stop'};
		requestSelectLinkData(selectedPeriod, newMap = true);

		// Remove and restore elements to get journey-comparison UI
		$('.metrics-element').hide();
		$('.journey-element').show();
		$("#button-tool").trigger("click");
	});

	// Groups the components of the three RH panels and defines how/when they are shown
	$(".button-nav-div").each(function(){
		$(this).click(function(){
			var panelList = ['#tool-panel', '#about-panel', '#options-panel', '#settings-panel'];
			var textList = ['#tool-text', '#about-text', '#options-text', '#settings-text'];
			var btnList = ['#button-tool', '#button-about', '#button-options', '#button-settings'];
			var divList = ['#button-tool-div', '#button-about-div', '#button-options-div', '#button-settings-div'];
			var switchID = "#"+this.id.split("-")[1]+"-panel";
			var textID = "#"+this.id.split("-")[1]+"-text";
			var btnID = "#button-"+this.id.split("-")[1];
			$(switchID).show();
			for (var i in panelList) {
				if (panelList[i] != switchID){
					$(panelList[i]).hide();
					$(textList[i]).hide();
					$(divList[i]).attr('class', 'button-nav-div');
					$(btnList[i]).attr('class', 'btn tab');
				}
			}
			$(this).attr('class',"button-nav-div-click");
			$(btnID).attr('class',"btn tab-click");
			$(textID).show();
		})
	})
	
	// Defines the hide/show function of each subpanel in the RH menu
    $(".toggle").each(function(){
		$(this).click(function(){
			var toggle = this.id.split("-")[0];
			$("#"+toggle+"-panel-more").text("")

			var display = $("#"+toggle+"-panel").css('display')
			if (toggle != "range"){
				rangeHeight = (display == "" || display == "block")? rangeHeight + panelHeight[toggle]:rangeHeight - panelHeight[toggle]
				$("#range-panel").animate({height: rangeHeight}, 500)
			}
			$("#"+toggle+"-panel").slideToggle();
			$("#"+toggle+"-arrow").attr("class", (display == "" || display == "block")? "arrow-down":"arrow-up")
		})
	});
	$(".toggle2").each(function(){
		$(this).click(function(){
			var toggle = this.id.split("-")[0];
			$("#"+toggle+"-panel-more").text("")
			var display = $("#"+toggle+"-panel").css('display')
			$("#"+toggle+"-panel").slideToggle();
			$("#"+toggle+"-arrow").attr("class", (display == "" || display == "block")? "arrow-down":"arrow-up")
		})
	});

	// Defines which of the 3 panels is shown upon loading
	$("#button-options").trigger("click");

	// Defines action when "Import Background Layer" button is clicked
	$('#select-background-button').click(function(){
		
		var selectedBackground = $('#select-background').val();
		// Add new background layer if none exists
		if (!map.hasLayer(backgroundLayer) && selectedBackground != "None"){
			$.ajax({
				type: "PUT",
				url: '/load/load_sublayer',
				contentType: 'application/json; charset=UTF-8',
				dataType: 'json',
				data: JSON.stringify(selectedBackground, null, '\t'),
				success: function(data) {
					
					// Add to map
					var polyStyle = {
						"color": "#ff00ff", // pick a color not part of metrics color scheme
						"weight": 1,
						"opacity": 0.7,
						"fillOpacity" : 0.3
					};

					var markerStyle = {
						"radius": 5,
						"fillColor": "#265c3f",
						"color": "#ca8aff",
						"weight": 1.5,
						"opacity": 0.5,
						"fillOpacity": 0
					};

					backgroundLayer = L.geoJSON(data, {
						pointToLayer: function (feature, latlng) {
							if(feature.geometry.type == 'Point'){
							  marker = L.circleMarker(latlng, markerStyle);
							  return marker
							}
						  },
						onEachFeature: function (features, layer){
							if(layer.feature.geometry.type != 'Point'){                                            
								layer.setStyle(polyStyle)
							}
						}
					}).addTo(map);

					backgroundLayer.bringToBack();
					backgroundLayer.eachLayer(function(layer) {
						var popText = '<p>'
						popText+= 'ANNABEL </br>'
						var layerProps = Object.keys(layer.feature.properties)
						for(property in layerProps){
							popText += '<b> '
							popText += layerProps[property]
							popText += ':</b> '
							popText += layer.feature.properties[layerProps[property]]
							popText += '</br>'
						}
						popText += '</p>'
						layer.bindPopup(popText)
					});
					backgroundLayer.on('click', function(e) {
						var marker1 = e.layer._leaflet_id;
						// console.log(marker1, backgroundLayer._layers[marker1])
						createRoutePolygonMap(backgroundLayer._layers[marker1], routesGeojson)
					  });
					populateEquityBackgroundFilters();
				}
			});
		}

	});
	
	$('.range-slider-bkgrd').each(function(){
		$(this).slider().on('slideStop', function(){
			console.log(this)
		});
	});

	// Defines action when clear geometry button is clicked
	$('#clear-geom-btn').click(function(){
		map.removeLayer(backgroundLayer);
		backgroundLayer = [];
		$('#range-toggle-bkgrd').hide();
		$('#bkgrd-select').hide();
		$('#legends-2').hide()
		// $("#legend-svg-bkgrd").hide();
	});

	// Add time range slider
	$( ".time-slider" ).slider({
		range: true,
		min: 0,
		max: 24,
		step: 0.1666666667,
		tooltip: 'always',
		tooltip_split: true,
		formatter: function(value) {
			var hour = Math.floor(value);
			var minutes = ('0'+Math.round((value - hour) * 60)).slice(-2) // place zero in front of minutes if <10
			return hour + ':' + minutes;
		}
	});
	$( ".time-slider" ).slider('setValue', [0, 24]);

	// Defines action for clear selection button
	$('#clear-selection').click(function(){
		selectionIndicator = 0
		selectedSegment = [];
		selectedLayer = [];
		routesGeojson.eachLayer(function(layer){
			setSelectLinkStyle(layer);
		});
		stopsGeojson.eachLayer(function(layer){
			clearStopGeometry(layer);
		});
		d3.selectAll("svg > *").remove();
		//$("#segment-info").empty();
		$("#selection-table").empty();
		map.closePopup(); 
	});

	// Defines action for load period data button
	$('#load-period-data').click(function(){
		loadPeriodData();
	});
	
	// Default to journey elements hidden
	$('.journey-element').hide();
}

function createMap(){
	// $("#loader").show()
	document.getElementById("loader").style.display = "block";

	requestAnimationFrame(() =>
    requestAnimationFrame(function(){

	// Create new custom polyline - needed for decoding.
	customPolyline = L.Polyline.extend({});

	// Get shapes data
	if (selectLinkIndicator === 0){
		if (comparisonIndicator === 1){
			var baselineFile = $('#select-baseline').val();
			var comparisonFile = $('#select-comparison').val();
			var busShapes = getShapesFile(comparisonFile)
			var oldShapes = getShapesFile(baselineFile)
		} else {
			var busShapes = getShapesFile(selectedFile);
		}
	} else {
		var selectedLevel = $("#level option:selected").val();
		if (selectedLevel == null){
			selectedLevel = 'stop';
		} 
		
		if (comparisonIndicator === 1){
			var busShapes = getShapesFile({'file': selectedFile[1], 'level': selectedLevel});
		} else {
			var busShapes = getShapesFile({'file': selectedFile, 'level': selectedLevel});
		}
	} 

	// Get peak directions
	peakDirections = getPeakDirections(selectedFile);
	// Loop to create geoJSON linestring from each encoded polyline in shape file and append properties from server data
	var lineFeatures = [];
	
	if(selectLinkIndicator === 1){
		for (var i in busShapes){
			// Get shape information from shapes file
			var routeID = busShapes[i].route_id
	
			// If config file specifies an alternative route id, use it
			if (routeID in altRouteIDs) {
				routeID = altRouteIDs[routeID]
			}
	
			if (!(routeList.includes(routeID))){
				routeList.push(routeID)
			}
	
			var startStop = busShapes[i].stop_pair[0];
			var endStop = busShapes[i].stop_pair[1];
			var segIndex = startStop + '-' + endStop
			var tpIndex = busShapes[i].timepoint_index;
			var coords =  L.PolylineUtil.decode(busShapes[i].geometry, 6);
	
			var newLine = new customPolyline(coords, {
				segIndex: segIndex,
				tpIndex: tpIndex,
				routeID: routeID,
				directionID: busShapes[i].direction,
				startStop: startStop,
				endStop: endStop,
				mode: busShapes[i].mode,
				traversals: 0,
			});
	
			lineFeatures.push(newLine);
		}
	
		// Combine all linestrings into Leaflet FeatureGroup and define popups
		routesGeojson = L.featureGroup(lineFeatures);
	
		// Zoom map to extents of geojson and restrict map bounds to transit service region w/ some padding
		map.fitBounds(routesGeojson.getBounds());
		map.setZoom(11);
		map.setMaxBounds(routesGeojson.getBounds().pad(0.5));

		var selectedLevel = $("#level option:selected").val();
		if(selectedLevel == null){
			selectedLevel = 'stop';
		}

		if(comparisonIndicator === 1){
			var busStops = getShapesFile({'file': selectedFile[1], 'level': selectedLevel}, true);
		} else {
			var busStops = getShapesFile({'file': selectedFile, 'level': selectedLevel}, true);
		}

		// Loop to create geoJSON linestring from each encoded polyline in shape file and append properties from server data
		var stopList = [];
		for (var i in busStops){
			
			var stopID = busStops[i].stop_id;
			var stopName = busStops[i].stop_name;
			var coords =  [busStops[i].stop_lat, busStops[i].stop_lon];
	
			var newStop = L.circle(coords, {
				stopID: stopID,
				stopName: stopName
			});
			stopList.push(newStop);
		}
	
		// Combine all linestrings into Leaflet FeatureGroup and define popups
		stopsGeojson = L.featureGroup(stopList);
	
		map.on('mousedown', () => {
			resetSelectedState();
		});
	
		map.on('lasso.finished', event => {
			setSelectedLayers(event.layers);
		});
		map.on('lasso.enabled', () => {
			resetSelectedState();
		});
	
		function resetSelectedState() {
			var lassoResult = '';
		}
		function setSelectedLayers(layers) {

			var selectedLevel = $("#level option:selected").val();
			if(selectedLevel == null){
				selectedLevel = 'stop';
			}
			if(selectedLevel === 'stop'){
				var selectedIndex = 'segIndex';
			} else {
				var selectedIndex = 'tpIndex';
			}

			resetSelectedState();
			if(layers.length > 0){
				selectionIndicator = 1;
				var lassoResult = [];
				var layerIndices = [];
				for(var layer in layers){
					// Use opacity to filter out transit stops and include only line segments
					if(layers[layer].options.opacity === 1){
						lassoResult.push(layers[layer]);
						layerIndices.push(layers[layer].options[selectedIndex]);
					}
				}
				if(lassoResult.length > 0){
					selectedLayer = lassoResult;
					selectedSegment = layerIndices;
					drawLasso(lassoResult);
				}
			}
		}
	
		// Style shapes with first metric
		routesGeojson.eachLayer(function(layer){
			setSelectLinkStyle(layer);
			setSelectLinkPopup(layer);
		});
	
		// Style stops as transparent to begin
		stopsGeojson.eachLayer(function(layer){
			clearStopGeometry(layer);
		});
	
		// Add shapes to map and store parameters
		routesGeojson.addTo(map);
		stopsGeojson.addTo(map);
		mapCenter = routesGeojson.getBounds().getCenter();
	
		// Add lasso control to map (global var)
		lassoControl = L.control.lasso().addTo(map);
		$('#toggle-lasso').click(function(){
			if(lassoControl.enabled()){
				lassoControl.disable();
			} else {
				lassoControl.enable();
			}
		});

		// Set up event handling for clicking on segment
		routesGeojson.on("click", displaySegments);
		$('#clear-selection').trigger("click");

	} else {
		var segmentList = [];
		legendDef = d3.scaleQuantile().range(rangeGreen);;

		for (var i in busShapes){
			// Get shape information from shapes file
			var routeID = busShapes[i].route_id

			// If config file specifies an alternative route id, use it
			if (routeID in altRouteIDs) {
				routeID = altRouteIDs[routeID]
			}

			var startStop = busShapes[i].stop_pair[0];
			var endStop = busShapes[i].stop_pair[1];

			// If geographic comparison with multiple stop pairs
			if ('base' in busShapes[i].stop_pair) {
				startStop = busShapes[i].stop_pair['base'];
			}

			if ('comp' in busShapes[i].stop_pair) {
				endStop = busShapes[i].stop_pair['comp'];
			}
			
			var rteIndex = routeID + '-' + busShapes[i].direction;
			var segIndex = routeID + '-' + startStop + '-' + endStop;

			if(busShapes[i].cor_id == null){
				var corIndex = startStop + '-' + endStop;
			} else { // Handle the geographic decomposition shapes
				var corIndex = busShapes[i].cor_id;
				var segIndex = busShapes[i].cor_id;
			}
			
			var tpIndex = timepointLookup[segIndex];

			if(tpIndex == null){
				var tpIndex = '0';
			} else {
				var tpIndex = routeID + '-' + tpIndex[0] + '-' + tpIndex[1];
			};

			var signal = busShapes[i].intersect;
			if(signal == null){
				signal = 'Unknown';
			};

			var coords =  L.PolylineUtil.decode(busShapes[i].geometry, 6);
			var newLine = new customPolyline(coords, {
				segIndex: segIndex,
				rteIndex: rteIndex,
				corIndex: corIndex,
				tpIndex: tpIndex,
				routeID: routeID,
				directionID: busShapes[i].direction,
				startStop: startStop,
				endStop: endStop,
				signal: signal,
				newSeg: true, // Indicates whether part of comparison period data
			});

			// Use segment index to get segment metric information from server data -- median is default on loading
			newLine = appendMetricsToLine(medianData, newLine);

			// If in comparison mode, also add both periods' data to the line properties
			if(comparisonIndicator === 1){
				newLine = appendMetricsToLine(compMedianData, newLine, "comp-");
				newLine = appendMetricsToLine(baseMedianData, newLine, "base-");

				// Set newSeg parameter to false if abandoned segment
				if(newLine.options['base-rte-sample_size'] > 0 && newLine.options['comp-rte-sample_size'] === 0){
					newLine.options['newSeg'] = false;
				} else if(newLine.options['base-rte-sample_size'] > 0 && newLine.options['comp-rte-sample_size'] == null){
					newLine.options['newSeg'] = false;
				}
			};

			if (corIndex in corridorRoutes){
				existingRoutes = corridorRoutes[corIndex];
				existingRoutes.push(routeID);
				corridorRoutes[corIndex] = existingRoutes;
			} else {
				corridorRoutes[corIndex] = [routeID];
			}

			lineFeatures.push(newLine);
			segmentList.push(segIndex);
		}

		// Add grey lines for any old segments that are no longer served
		for (var i in oldShapes){
			// Get shape information from shapes file
			var routeID = oldShapes[i].route_id

			// If config file specifies an alternative route id, use it
			if (routeID in altRouteIDs) {
				routeID = altRouteIDs[routeID]
			}

			var startStop = oldShapes[i].stop_pair[0]
			var endStop = oldShapes[i].stop_pair[1]
			var segIndex = routeID + '-' + startStop + '-' + endStop

			if(segmentList.includes(segIndex)){
				continue
			}
			
			var rteIndex = routeID + '-' + oldShapes[i].direction
			if(oldShapes[i].cor_id == null){
				var corIndex = startStop + '-' + endStop;
			} else {
				var corIndex = oldShapes[i].cor_id;
			}
			var coords =  L.PolylineUtil.decode(oldShapes[i].geometry, 6);
			var newLine = new customPolyline(coords, {
				segIndex: segIndex,
				rteIndex: rteIndex,
				corIndex: corIndex,
				routeID: routeID,
				directionID: oldShapes[i].direction,
				startStop: startStop,
				endStop: endStop,
				newSeg: false,
			});	
			
			newLine = appendMetricsToLine(medianData, newLine);
			newLine = appendMetricsToLine(baseMedianData, newLine, "base-");
			lineFeatures.push(newLine);
		}

		// Combine all linestrings into Leaflet FeatureGroup and define popups
		routesGeojson = L.featureGroup(lineFeatures);

		// Zoom map to extents of geojson and restrict map bounds to transit service region w/ some padding
		map.fitBounds(routesGeojson.getBounds());
		map.setZoom(11);
		map.setMaxBounds(routesGeojson.getBounds().pad(0.5));

		// Get the first segment metric to use as default for visualization
		var initMetric = levelMetrics['seg'][0];
		var initRange = [];
		routesGeojson.eachLayer(function(layer) {
			initRange.push(layer.options['seg-'+initMetric]);
		});

		// Add default metric range to color scale
		legendDef.domain(initRange);

		// Lookup whether that range should have red high or low
		var initScheme = redValues[initMetric];
		// Always show low values as red in comparison
		if(comparisonIndicator === 1){
			legendDef.range(rangeGreen);
		} else 
		if (initScheme == "high"){
			legendDef.range(rangeBlue);
		};

		// Style shapes with first metric and save data for export
		routesGeojson.eachLayer(function(layer){

			var initColor = layer.options[ 'seg-' + initMetric ]
			if (initColor === null && layer.options['newSeg']){
					restyleShapeTransparentAndSendBack(layer);
			} else if(layer.options['newSeg'] === false) {
				layer.setStyle({
					weight: 1,
					opacity: 1,
					offset: 3.5,
					interactive: true,
					color: color="rgb(184, 122, 168)" // Assign pink color if old segment
				});
			} else {
				layer.setStyle({
					weight: 2.5,
					opacity: 1,
					offset: 3.5,
					interactive: true,
					color: legendDef(initColor)
				});
			};

			// Add popup information through function in toolFunc.js
			setPopup(layer);

			// Add layer data to shape for possible export
			var layerData = [layer.options.routeID, directionLabels[layer.options.directionID], layer.options.startStop, layer.options.endStop, layer.options.signal]
			for(var index in levelMetrics['seg']){
				layerData.push(layer.options['seg-' + levelMetrics['seg'][index]]);
			}
			// In comparison mode, add baseline and comparison metrics
			if(comparisonIndicator === 1){
				for(var index in levelMetrics['seg']){
					layerData.push(layer.options['base-seg-' + levelMetrics['seg'][index]]);
				}
				for(var index in levelMetrics['seg']){
					layerData.push(layer.options['comp-seg-' + levelMetrics['seg'][index]]);
				}
			}
			exportData.push(layerData);
		});
	}

	// console.log(lineFeatures, 'lineFeatures')
	// var point = turf.point([38.795961, -77.075091])
	// var line = turf.lineString(makeLatLongArray(lineFeatures[0]._latlngs))
	// console.log( 'shape of bus line in lat/long array', line)
	// console.log( 'test', point)
	// console.log('busShapes', turf.booleanIntersects(line, point))
	// console.log('backgroundLayer', backgroundLayer)
	// Add shapes to map and store parameters
	routesGeojson.addTo(map);
	mapCenter = routesGeojson.getBounds().getCenter();

	routesGeojson.eachLayer(function(layer){
		if (layer.options['newSeg'] === false){
			layer.bringToBack();
		}
	});

	// Call functions in toolFunc.js
	populateFilters();
	addEventHandlers();
	if(selectLinkIndicator === 1){
		createSelectLinkLegend();
	} else {
		createLegend();
	}
	// Defines which of the 3 panels is shown upon loading and hide loading window
	$("#button-tool").trigger("click");
			$("#loader").hide()
			//blocks render
	}))
}

// Function for exporting map images
function prepareImage(err, canvas) {
    var link = document.createElement("a");
	link.download = "ROVE_Map.png";
	link.href = canvas.toDataURL('image/png');
	link.click();
}

// Takes an input data file and polyline item and returns polyline with metrics appended
function appendMetricsToLine(inputData, newLine, prefix = ''){

	var segIndex = newLine.options['segIndex'];
	var rteIndex = newLine.options['rteIndex'];
	var corIndex = newLine.options['corIndex'];
	var tpIndex = newLine.options['tpIndex'];

	// Function to append data to a line depending on the level
	function append(inputData, level, order, index, prefix, newLine){
		var metricList = levelMetrics[level];
		if (typeof inputData[order][index] === 'undefined'){
			for(var j in metricList){
				var metricName = metricList[j];
				// Default to segment-level metric value if this segment is not part of a corridor
				if(level === 'cor'){
					newLine.options[prefix + level + '-' + metricName] = newLine.options[prefix + 'seg-' + metricName];
				} else if (level === 'tpCor'){
					newLine.options[prefix + level + '-' + metricName] = newLine.options[prefix + 'tpSeg-' + metricName];
				} else {
					newLine.options[prefix + level + '-' + metricName] = null;
				}
			};
		} else {
			for(var j in metricList){
				var metricName = metricList[j];
				var metricValue = inputData[order][index][metricName];

				if(metricName === 'scheduled_frequency' && (metricValue > 18 || metricValue < -18)){
					if(metricValue > 18){
						metricValue = 18;
					} else {
						metricValue = -18;
					};
					
				} else if (metricName === 'observed_frequency' && (metricValue > 18 || metricValue < -18)) {
					if(metricValue > 18){
						metricValue = 18;
					} else {
						metricValue = -18;
					};
				} else if ((metricName === 'passenger_congestion_delay') && (metricValue < 0) && (comparisonIndicator === 0)){
					metricValue = 0;
				} else if ((metricName === 'vehicle_congestion_delay') && (metricValue < 0) && (comparisonIndicator === 0)){
					metricValue = 0;
				}
				newLine.options[prefix + level + '-' + metricName] = metricValue;

			};
		};
		return newLine
	};

	newLine = append(inputData, 'seg', 0, segIndex, prefix, newLine);
	newLine = append(inputData, 'rte', 1, rteIndex, prefix, newLine);
	newLine = append(inputData, 'cor', 2, corIndex, prefix, newLine);
	newLine = append(inputData, 'tpSeg', 3, tpIndex, prefix, newLine);
	newLine = append(inputData, 'tpCor', 3, tpIndex, prefix, newLine);
	return newLine
}

function displaySegments(e) {

	// Only display new segments if no segments currently selected
	if(selectionIndicator === 0){

		exportData = [];

		// Find out whether to append segment or timepoint data
		var selectedLevel = $("#level option:selected").val();
		if (selectedLevel === 'stop' || selectedLevel == null){
			var baseIndex = e.layer.options.segIndex;
			var index = ['segIndex'];
		} else if (selectedLevel === 'tp') {
			var baseIndex = e.layer.options.tpIndex;
			var index = ['tpIndex'];
		}

		// Determine selection mode
		var selectionMode = $("#selmode").val();
		var allSegs = Object.keys(segmentData);

		if (allSegs.includes(baseIndex)){
			selectedSegment = baseIndex;
			selectedLayer = e.layer;
			selectionIndicator = 1;

			if(selectionMode === 'all'){ // Merge both upstream and downstream
				var upstreamData = segmentData[baseIndex]['upstream'];
				var downstreamData = segmentData[baseIndex]['downstream'];
				var layerTraversals = Object.assign(upstreamData, downstreamData);
			} else {
				var layerTraversals = segmentData[baseIndex][selectionMode];
			}

			var segmentList = Object.keys(layerTraversals);
			var layerRange = Object.values(layerTraversals);
		
			var min = Math.min(...layerRange);
			var max = Math.max(...layerRange);
			e.layer.options.traversals = layerTraversals[baseIndex];

			var layerData = [e.layer.options.routeID, directionLabels[e.layer.options.directionID], e.layer.options.mode, e.layer.options.startStop, e.layer.options.endStop, e.layer.options.traversals, "Yes"]
			exportData.push(layerData);

			updateSelectLinkLegend(min, max, layerRange);
			updateSegText(e.layer);
			routesGeojson.eachLayer(function(drawLayer){

				var travSegmentIndex = drawLayer.options[index];
				drawLayer.options.baseTraversals = null;
				drawLayer.options.baseTraversals = null;

				if (segmentList.includes(travSegmentIndex)){
					var traversals = layerTraversals[travSegmentIndex];
					drawLayer.options.traversals = traversals;
					var layerWeight = getLayerWeight(drawLayer);					
					drawLayer.setStyle({
						weight: layerWeight,
						opacity: 1,
						offset: 3.5,
						interactive: true,
						color: legendDef(traversals)
					});
					setSelectLinkPopup(drawLayer);
					setLayerOrder(drawLayer, min);

					var layerData = [drawLayer.options.routeID, directionLabels[drawLayer.options.directionID], drawLayer.options.mode, drawLayer.options.startStop, drawLayer.options.endStop, traversals, "No"]
					exportData.push(layerData);

				} else {
					restyleShapeTransparentAndSendBack(drawLayer);
				}
			});

			stopsGeojson.eachLayer(function(stopLayer){
				stopID = stopLayer.options.stopID;
				if( e.layer.options.startStop === stopID || e.layer.options.endStop === stopID ){
					drawStop(stopLayer);
				};
			})

		} else {
			// If no data availble for this segment, inform user and reset selection
			var popText = '<p> No data available for this segment during the selected period. </p>';
			e.layer.bindPopup(popText, {maxWidth : 350});
		}
		e.layer.openPopup();
	}
}

function makeLatLongArray(coordinateArray) {
	if(coordinateArray.length === 1){
		makeLatLongArray(coordinateArray[0])
	}
	return coordinateArray.map(coordinates => Object.values(coordinates))
}

function createRoutePolygonMap(polygon, routes){
	// takes background polygons and bus routes and returns a object in the shape
	// {polygonID: [{segments: [segment-indexes]}]}
	// what does the data actually look like - we have data at each stop
	// polygons._layers.forEach(polygon => {
	// 	console.log(polygon.feature.geometry.coordinates)
	// })

	// console.log('routes', routes)
	// console.log('polygon', polygon)

	// make polygons into turf polygons
	// var turfPolygons = {};
	// console.log('pre turf polygon', polygon)
	// var polygonCoordinates = []
	// for (let i = 0; i < polygon.feature.geometry.coordinates.length ; i++){
	// 	// console.log(polygon[1]._latlngs.length)
	// 	var coordinates = polygon.feature.geometry.coordinates[i];
	// 	if(coordinates.length === 1){
	// 		coordinates=coordinates[0]	
	// 	}
	// 	var temp = coordinates.concat([coordinates[0]]);
	// 	polygonCoordinates = polygonCoordinates.concat(temp)
	// }
	// 	turfPolygons[polygon[1]._leaflet_id] = turf.multiPolygon([polygonCoordinates])
	// // })
	// console.log(turfPolygons)


	// console.log(routes._layers)

	//make route segments into turf lines
	// var polygonSegmentIntersections = {}
	// var lineSegments = {}

	// console.log('segs', Object.values(routes._layers).length)
	// console.log('polys', Object.entries(turfPolygons).length)
	// segment.options.segIndex
	// var testPolycoords = polygon.feature.geometry.coordinates[0].concat(polygon.feature.geometry.coordinates[0][0])
	// console.log(testPolycoords)
	// console.log(polygonCoordinates)
	var turfPoly = turf.multiPolygon([[makeLatLongArray(polygon._latlngs[0])]])
	console.log('turfPoly', turfPoly)
	// var segmentsArr = []
	Object.values(routes._layers).forEach(segment => {
		// console.log('segment pre turf', segment)
		var turfLine = turf.lineString(makeLatLongArray(segment._latlngs)) 
		// console.log('turfLine', turfLine)
		if (turf.booleanIntersects(turfLine, turfPoly)){
			console.log('match')
			// segmentsArr = segmentsArr.concat(segment)
		}

		// Object.entries(turfPolygons).forEach(poly => {
			// linesefturf = turf.lineString(makeLatLongArray(segment._latlngs)) 
			// console.log('seg')
			// if(turf.booleanIntersects(poly[1], linesefturf)){
				// console.log('intersect')
				// lineSegments[segment.options.segIndex] = {
				// 	'latlng': turf.lineString(makeLatLongArray(segment._latlngs)),
				// 	'seg-observed_speed_without_dwell': segment.options['seg-observed_speed_without_dwell'],
				// 	'seg-boardings': segment.options['seg-boardings'],
				// 	'seg-crowding': segment.options['seg-crowding'],
				// };
			// }
		// })
	})

	// console.log('lineSegments',segmentsArr)

	// Object.entries(turfPolygons).forEach(poly => {
	// 	console.log('test poly', poly)
	// 	turfPolygons[poly[0]]['lineData'] = Object.values(lineSegments).filter(line => turf.booleanIntersects(poly[1], line.latlng))
	// })
	// console.log('final:',turfPolygons)

	// determine which lines intersect with which polygons
	// var polygonSegmentIntersections = {}
	// turfPolygons.forEach(poly => {
	// 	Object.entries(lineSegments).forEach(seg => {
	// 		console.log(turf.booleanIntersects(poly, seg[1].latlng))
	// 	})
	// })

	// return map of polygons: line segments
	
}