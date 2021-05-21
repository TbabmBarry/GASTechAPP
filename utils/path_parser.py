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
            properties={"path_id": path_id}))
    feature_collection = geojson.FeatureCollection(features)
    return feature_collection