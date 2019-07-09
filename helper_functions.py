import folium
import pyspark
import matplotlib
import numpy as np
from pyspark.sql.types import *
from datetime import datetime, date
import pyspark.sql.functions as functions


#--------------------------------------------- Prepocessing ---------------------------------------------#


def rename_columns(df_sbb):
    df = df_sbb.withColumnRenamed('BETRIEBSTAG', 'OPERATING_DAY') \
               .withColumnRenamed('FAHRT_BEZEICHNER', 'TRIP_ID') \
               .withColumnRenamed('BETREIBER_ID', 'OPERATOR_ID') \
               .withColumnRenamed('BETREIBER_ABK', 'OPERATOR_ABK') \
               .withColumnRenamed('BETREIBER_NAME', 'OPERATOR_NAME') \
               .withColumnRenamed('PRODUKT_ID', 'PRODUCT_ID') \
               .withColumnRenamed('LINIEN_ID', 'LINE_ID') \
               .withColumnRenamed('LINIEN_TEXT', 'LINE_TEXT') \
               .withColumnRenamed('UMLAUF_ID', 'RUN_ID') \
               .withColumnRenamed('VERKEHRSMITTEL_TEXT', 'TRANSPORTATION_TEXT') \
               .withColumnRenamed('ZUSATZFAHRT_TF', 'ADDITIONAL_TRIP') \
               .withColumnRenamed('FAELLT_AUS_TF', 'CANCELLED_TRIP') \
               .withColumnRenamed('BPUIC', 'BPUIC') \
               .withColumnRenamed('HALTESTELLEN_NAME', 'STOP_NAME') \
               .withColumnRenamed('ANKUNFTSZEIT', 'ARRIVAL_TIME') \
               .withColumnRenamed('AN_PROGNOSE', 'AT_FORECAST') \
               .withColumnRenamed('AN_PROGNOSE_STATUS', 'AT_FORECAST_STATUS') \
               .withColumnRenamed('ABFAHRTSZEIT', 'DEPARTURE_TIME') \
               .withColumnRenamed('AB_PROGNOSE', 'DT_FORECAST') \
               .withColumnRenamed('AB_PROGNOSE_STATUS', 'DT_FORECAST_STATUS') \
               .withColumnRenamed('DURCHFAHRT_TF', 'DRIVE_THROUGH')
    return df

def format_columns(df_sbb):
    # Format the dates as timestamp
    df = df_sbb
    df = df.withColumn('OPERATING_DAY', functions.to_timestamp('OPERATING_DAY', 'dd.MM.yyyy'))
    df = df.withColumn('ARRIVAL_TIME', functions.to_timestamp('ARRIVAL_TIME', 'dd.MM.yyyy HH:mm'))
    df = df.withColumn('AT_FORECAST', functions.to_timestamp('AT_FORECAST', 'dd.MM.yyyy HH:mm:ss'))
    df = df.withColumn('DEPARTURE_TIME', functions.to_timestamp('DEPARTURE_TIME', 'dd.MM.yyyy HH:mm'))
    df = df.withColumn('DT_FORECAST', functions.to_timestamp('DT_FORECAST', 'dd.MM.yyyy HH:mm'))
    # Format columns
    df = df.withColumn('PRODUCT_ID', functions.initcap(functions.col('PRODUCT_ID')))
    df = df.withColumn('DRIVE_THROUGH', functions.col('DRIVE_THROUGH').cast("boolean"))
    df = df.withColumn('ADDITIONAL_TRIP', functions.col('ADDITIONAL_TRIP').cast("boolean"))
    df = df.withColumn('CANCELLED_TRIP', functions.col('CANCELLED_TRIP').cast("boolean"))
    return df

def get_metadata(df_metadata):
    df_met = df_metadata
    # Negative look-behind to split on spaces not preceded by characters
    split_col = pyspark.sql.functions.split(df_met['value'], "(?<![a-zA-Z,()-])\s+")
    df_met = df_met.withColumn('id', split_col.getItem(0))
    df_met = df_met.withColumn('x', split_col.getItem(1).cast(FloatType()))
    df_met = df_met.withColumn('y', split_col.getItem(2).cast(FloatType()))
    df_met = df_met.withColumn('z', split_col.getItem(3).cast(FloatType()))
    df_met = df_met.withColumn('STOP_NAME', split_col.getItem(5))
    df_met = df_met.drop('value')
    return df_met


#--------------------------------------------- Datetime functions ---------------------------------------------#


def to_time(str_time):
    """ This function converts string to Time object. """
    return datetime.strptime(str_time, '%H:%M').time()

def add_timedelta(time, to_add):
    """ This function adds the timedelta to_substract
        from the time, returns Time object. """
    return (datetime.combine(datetime.today(), time) + to_add).time()

def substract_times(time1, time2):
    """ Substracts 2 datetime.time objects, return a Timedelta. """
    return datetime.combine(date.today(), time1) - datetime.combine(date.today(), time2)

def dt_weekday2pyspark_weekday(weekday):
    """ This function fixes the issue of python datetime package and
        pyspark day_of_week not having the same integer representation. """
    return ((weekday + 1) % 7)+ 1

def time_to_string(time):
    """ This function converts a time object to a string. """
    if(time.minute >9):
        return '{}:{}'.format(time.hour, time.minute)
    else:
        return '{}:0{}'.format(time.hour, time.minute)
    

#--------------------------------------------- Get properties function ---------------------------------------------#


def get_distinct_lines(df_coords):
    """ This function returns a dataframe consisting of distinct lines. """
    df = df_coords.dropna(axis=0)\
                  .sort_values('DEPARTURE_TIME', ascending=True)
    df_counts = df_coords.groupby(["LINE_ID", "LINE_TEXT", "TRIP_ID"]).size().to_frame("count")
    df_lines = df_coords.groupby(["LINE_ID", "LINE_TEXT", "TRIP_ID"]).agg(lambda x: tuple(x)).join(df_counts)
    df_lines_unique = df_lines.loc[df_lines.groupby(["LINE_TEXT"])["count"].idxmax()]
    return df_lines_unique

def get_name_to_nodes(G):
    """ This function ... . """
    name2nodes = dict()
    for n in G.nodes:
        (stop_name, week_num, time, lineid, linetext, linetype) = n

        if stop_name in name2nodes:
            name2nodes[stop_name].append(n)
        else :
            name2nodes[stop_name] = [n]
    return name2nodes

def get_nodes_time(stop_name, departure_time, arrival_time, weekday, name2nodes):
    """ This function returns node with this stop_name between
        departure_time and arrival_time. """
    return [node for node in name2nodes[stop_name] if departure_time <= node[2] <= arrival_time and weekday == node[1]]


#--------------------------------------------- Plotting function ---------------------------------------------#


def print_trip(success, trip):
    """ This functions prints the trip properly. """
    if(trip != -1):
        # Print the result
        trip = remove_dup_ends(trip)
        time = datetime.combine(date.today(), trip[-1][2]) - datetime.combine(date.today(), trip[0][2])
        print('Probability of success: {:.2f}%'.format(success*100))
        print('Travel time: {} '.format(time))
        print('-----------------------------------------------------------------')
        trip_id = trip[0][3]
        for i, node in enumerate(trip):
            if(node[3] == trip_id):
                print('{}\t\t{}\t\t{}\t\t'.format(node[0].ljust(50)[:25], time_to_string(node[2]), node[5]))
            else:
                trip_id = node[3]
                print('-----------------------------------------------------------------')
                print('{}\t\t{}\t\t{}\t\t'.format(node[0].ljust(50)[:25], time_to_string(node[2]), node[5]))
        print('-----------------------------------------------------------------')
    else:
        print('Sorry, no connections found.')
        
def remove_dup_ends(trip):
    """ This function removes the stations that loop over themselves at the end
        of the itinerary. """
    last_station = trip[-1][0]
    second_last = trip[len(trip)-2][0]
    duplicates = (last_station == second_last)
    while duplicates:
        trip = trip[:-1]
        last_station = trip[-1][0]
        second_last = trip[len(trip)-2][0]
        duplicates = (last_station == second_last)
    return trip

def plot_itinerary(trip, metadata):
    """ This functions plots the itinerary on the map. """
    zurich_map = folium.Map(prefer_canvas=True)
    points = []
    for node in trip:
        lat = metadata[metadata['STOP_NAME'] == node[0]][['y']].values[0][0]
        long = metadata[metadata['STOP_NAME'] == node[0]][['x']].values[0][0]
        points.append((lat, long))
        folium.CircleMarker(location=[lat, long], radius=3, weight=6, color='red', popup=node[0]).add_to(zurich_map)
    folium.PolyLine(points, color='red', weight=2).add_to(zurich_map)
    zurich_map.fit_bounds(zurich_map.get_bounds())
    return zurich_map

def plot_all_lines(df_lines_unique):
    """ This function plots all different lines on the map. """
    zurich_map = folium.Map(prefer_canvas=True)
    cmap = matplotlib.cm.get_cmap('nipy_spectral')
    colors = [matplotlib.colors.rgb2hex(cmap(x)) for x in np.linspace(0, 1, len(df_lines_unique))]
    counter = 0
    for index, row in df_lines_unique.iterrows():
        points = []
        lats, longs = row['y'], row['x']
        for i in range (len(lats)):
            lat, long = lats[i], longs[i]
            if((lat, long) not in points):
                points.append((lat, long))
                folium.CircleMarker(location=[lat, long], radius=2, weight=3, color=colors[counter],
                                    popup=row['STOP_NAME'][i]).add_to(zurich_map)
        folium.PolyLine(points, color=colors[counter], weight=1).add_to(zurich_map)
        counter += 1
    zurich_map.fit_bounds(zurich_map.get_bounds())
    return zurich_map

def plot_isochronous(points_trip_l, trip_lengths, metadata):
    """ This function plots the isochronous points. """
    zurich_map = folium.Map(prefer_canvas=True)
    cmap = matplotlib.cm.get_cmap('autumn_r')
    colors = [matplotlib.colors.rgb2hex(cmap(x)) for x in np.linspace(0, 1, len(trip_lengths))]
    all_plotted_points = []
    for i, points_l in enumerate(points_trip_l):
        for point in points_l:
            lat, long = point[0], point[1]
            if((lat, long) not in all_plotted_points):
                folium.CircleMarker(location=[lat, long], radius=3, weight=6,
                                    color=colors[i], title=trip_lengths[i], 
                                    popup=metadata[(metadata['x']==long) & (metadata['y']==lat)]['STOP_NAME'].values[0])\
                      .add_to(zurich_map)
                all_plotted_points.append((lat, long))
    zurich_map.fit_bounds(zurich_map.get_bounds())
    return zurich_map