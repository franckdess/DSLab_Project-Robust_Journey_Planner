# Robust-Journey-Planning

Lab in Data Science - Final Project

This repository contains the following files:

- Robust_Journey_Planner-Graph_Construction.ipynb: A jupyter notebook used to construct the graph represented as follows:
    - Each node has as attributes : stop_name, day_of_week, time, line_id, line_text, transport_type
    - Each edge has as attributes : from_node, to_node, trip_duration, wait_duration, walk_duration

- Robust_Journey_Planner-Results_Vizualisation.ipynb: A jupyter notebook used to compute and display the resulting itinerary of a query. It displays the itinerary corresponding to the query, all the distinct lines on the transportation network of Zürich and the isochronous map given a source. Given the request of the user, it starts by computing a subgraph depending on time constraints and then finds the itinerary.

- helper_functions.py: A python file that contains helper functions. In both of notebooks above, the functions that we considered to be meaningful were kept on the notebooks while functions that we considered as less meaningful were placed on this file in order to avoid overloading the notebooks.

- Ressources: A folder that contains the final graph, and other dataframe that were computed during the graph construction and needed for the vizualisation.

- Folium: A folder that contains pictures of the maps since Folium maps are not shown on github.

In order to avoid running the entire jupyter notebook to build the graph, this latter is available here for download:

https://drive.google.com/file/d/1PWNiVt7pdIB5jmkwGw8Z-JPqt_PyQlYb/view?usp=sharing

Note that it has to be located in the folder 'Ressources'.