# OPTIMIZATIONS 

The key optimizations involved converting several functions from synchronous to asynchronous, allowing the application to handle multiple requests simultaneously without blocking the main application or becoming unresponsive. Asynchronous functions differ from synchronous ones in that they do not immediately require the necessary data to start processing. Instead, while the async functions await data, the application remains responsive, allowing user interaction with the UI and further data requests. Another advantage of this approach is that if data processing fails, it doesn’t negatively affect the user experience. Overall, these changes should lead to a reduction in wait times, shaving off a few seconds when loading and processing data.

Below are the specific files/functions that were modified:

## Modified Functions: 

Note that these files all live under the frontend/static/scripts/ path

* requestFunc.js 
    * syncDataRequest(): Converted this function from synchronous to asynchronous, enhancing its ability to handle data. Now, the function only calls parseData (for processing data) once the server response is fully received (fetching data). Additionally, this asynchronous method includes error handling. If an error occurs during data fetching, an error message is printed, preventing the entire application from crashing. This not only improves reliability but also enhances the user experience by maintaining application stability and responsiveness. 

    * getShapesFile(): This function, like the previous one, has been converted from synchronous to asynchronous. Now, it waits for the AJAX call to complete successfully before it processes and returns the shapes file. This change ensures that if an error occurs during the AJAX call, the error will be displayed in the console rather than impacting the entire application.

    * requestDataFromServer(): This function has been converted from synchronous to asynchronous. Now, it first fetches the data from the server and only after successfully receiving the data does it proceed to process the data to create or update the map. This change allows for more efficient data handling and a smoother user experience by ensuring that the map updates only occur with the latest available data, preventing any unresponsiveness or delays in the interface.

* toolFunc.js:
    * setPopUp(): Added helper functions to avoid repetition in code and allow for easier readability. This will also allow future users to directly modify/reuse specific modules (i.e. formatRoutes, formatDirection) without having to worry about affecting the overall code/logic.

* setupFunc.js:
    * createMap(): Added ‘Promise.all()’ to efficiently manage asynchronous operations when creating the map. By using this method, multiple data files—such as comparisonFile, baselineFile, and selectedFile—can be loaded in parallel. This approach ensures that the application does not have to wait for one request to complete before starting another, significantly speeding up the overall data loading process and enhancing the responsiveness of the application.

## Further Optimizations:
* There are currently discrepancies in how certain metrics are processed and transferred from the backend to the frontend. For example, metrics like "boardings" and "on_time_performance_perc" are handled differently compared to other metrics. As a result, specific conditional ('if') statements are necessary within the updateFunc.js and toolFunc.js files respectively, to ensure these metrics are managed correctly. Otherwise, the frontend exhibits issues, such as displaying inaccurate values like all zeroes for the on-time performance metric. Reviewing and possibly revising the backend's data processing and transmission methods will be crucial to ensure that users are working with accurate data.