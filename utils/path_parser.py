import geojson
import pandas as pd

def routeParser(gps_paths):
    gps_paths["Timestamp"] = pd.to_datetime(gps_paths.Timestamp)
    gps_paths["path_id"] = gps_paths.Timestamp.diff().dt.seconds.ge(120).cumsum()
    gps_paths["Timestamp"] = gps_paths.Timestamp.dt.strftime('%Y-%m-%d %H:%M:%S')
    features = []
    gps_paths_groupby = gps_paths.groupby("path_id")
    for path_id, path_group in gps_paths_groupby:
        features.append(geojson.Feature(
            geometry=geojson.LineString(
                coordinates=path_group.reset_index()[['long', 'lat']].values.astype(float).tolist()),
            properties={
                "path_id": path_id
            }))
    feature_collection = geojson.FeatureCollection(features)
    return feature_collection

def carRouteParser(gps):
    # if gps.empty:
    #     return "error"
    gps["Timestamp"] = pd.to_datetime(gps.Timestamp)
    gps_by_car = gps.groupby("id")
    car_paths = dict()
    for car_id, car_group in gps_by_car:
        car_group["path_id"] = car_group.Timestamp.diff().dt.seconds.ge(120).cumsum()
        car_group['Timestamp'] = car_group.Timestamp.dt.strftime('%Y-%m-%d %H:%M:%S')
        car_paths[str(car_id)] = dict()
        car_group_by_path = car_group.groupby("path_id")
        for path_id, path_group in car_group_by_path:
            car_paths[str(car_id)][str(path_id)] = list(zip(*map(path_group.get, ["long", "lat", "Timestamp"])))
    gps_car_paths = dict()
    for car_id, car_vals in car_paths.items():
        gps_car_paths[car_id] = list()
        features= list()
        for path_id, path_vals in car_vals.items():
            features.append(geojson.Feature(
                geometry=geojson.LineString(
                    coordinates=[list(map(float, coord[:2])) for coord in path_vals]),
                properties={
                    "path_id": path_id,
                    "car_id": car_id,
                    "time": [min(path_vals, key = lambda t: t[2])[2], max(path_vals, key = lambda t: t[2])[2]]
                }))
        feature_collection = geojson.FeatureCollection(features)
        gps_car_paths[car_id].append(feature_collection)
    return gps_car_paths