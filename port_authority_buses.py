import dataclasses

import pandas as pd
import zipfile

PORT_AUTHORITY_STOP_NAME = "PORT AUTHORITY BUS TERMINAL"


@dataclasses.dataclass
class GtfsData:
    agency: pd.DataFrame
    calendar_dates: pd.DataFrame
    routes: pd.DataFrame
    shapes: pd.DataFrame
    stop_times: pd.DataFrame
    stops: pd.DataFrame
    trips: pd.DataFrame


def read_gtfs_file(path) -> GtfsData:
    """Reads a GTFS file and a struct of DataFrames."""
    with zipfile.ZipFile(path) as z:
        return GtfsData(
            agency=pd.read_csv(z.open("agency.txt")),
            calendar_dates=pd.read_csv(z.open("calendar_dates.txt")),
            routes=pd.read_csv(z.open("routes.txt")),
            shapes=pd.read_csv(z.open("shapes.txt")),
            stop_times=pd.read_csv(z.open("stop_times.txt")),
            stops=pd.read_csv(z.open("stops.txt")),
            trips=pd.read_csv(z.open("trips.txt")),
        )


def find_buses_from_port_authority(data: GtfsData) -> pd.DataFrame:
    """Find all buses that depart from Port Authority Bus Terminal.

    Return a DataFrame with the following columns:
    - route_id
    - route_short_name
    - trip_id
    - departure_time
    - trip_headsign
    """
    port_authority_stop_id = data.stops[
        data.stops.stop_name == PORT_AUTHORITY_STOP_NAME
    ].stop_id.iloc[0]
    port_authority_trips = data.stop_times[
        data.stop_times.stop_id == port_authority_stop_id
    ].trip_id
    port_authority_trips = port_authority_trips.drop_duplicates()
    port_authority_trips = port_authority_trips.reset_index(drop=True)

    port_authority_stop_times = data.stop_times[
        data.stop_times.trip_id.isin(port_authority_trips)
        & (data.stop_times.stop_id == port_authority_stop_id)
    ]
    # Retain only the columns trip_id, departure_time
    port_authority_stop_times = port_authority_stop_times[["trip_id", "departure_time"]]
    # Merge with trips to get route_id and trip_headsign

    # FIXME: Make sure the direction is correct (depart from port authority)
    port_authority_stop_times = port_authority_stop_times.merge(
        data.trips[["trip_id", "route_id", "trip_headsign"]], on="trip_id"
    )
    # Merge with routes to get route_short_name
    port_authority_stop_times = port_authority_stop_times.merge(
        data.routes[["route_id", "route_short_name"]], on="route_id"
    )
    # Sort by route_id and departure_time
    port_authority_stop_times = port_authority_stop_times.sort_values(
        ["route_id", "departure_time"]
    )
    # Reset the index
    port_authority_stop_times = port_authority_stop_times.reset_index(drop=True)

    return port_authority_stop_times


def main():
