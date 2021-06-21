"""
Module to allow change the footprint of an event when its timings changed

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import re

# Import entities of the datamodel
from eboa.datamodel.events import EventObject, EventGeometry

# Import GEOalchemy entities
from geoalchemy2.shape import to_shape

# Import ingestion helpers
import s1boa.ingestions.functions as functions

def replicate_event_values(query, from_event_uuid, to_event_uuid, to_event, list_values_to_be_created):
    """
    Method to replicate the values associated to events that were overwritten partially by other events

    :param from_event_uuid: original event where to get the associated values from
    :type from_event_uuid: uuid
    :param to_event_uuid: new event to associate the values
    :type to_event_uuid: uuid
    :param list_values_to_be_created: list of values to be stored later inside the DDBB
    :type list_values_to_be_created: list
    """

    # Get values from the previous events
    values = query.get_event_values(event_uuids = [str(from_event_uuid)])

    # Check if the footprints can be updated using the satellite and the orbit prediction information
    satellite_values = [value for value in values if value.name == "satellite"]
    footprint_values = [value for value in values if type(value) in (EventGeometry, EventObject) and re.match(".*footprint.*", value.name)]
    imaging_mode_values = [value for value in values if value.name == "imaging_mode"]
    
    # Get the values to be directly copied (all but those related to the footprints)
    values_to_copy = values
    if len(footprint_values) > 0 and len(satellite_values):
        values_to_copy = [value for value in values if not(type(value) in (EventGeometry, EventObject) and re.match(".*footprint.*", value.name))]
    # end if
    position_footprints = 0
    for value in values_to_copy:
        if not type(value) in list_values_to_be_created:
            list_values_to_be_created[type(value)] = []
        # end if
        value_to_insert = {"event_uuid": to_event_uuid,
                           "name": value.name,
                           "position": value.position,
                           "parent_level": value.parent_level,
                           "parent_position": value.parent_position
        }
        if not type(value) in (EventObject, EventGeometry):
            value_to_insert["value"] = value.value
        elif type(value) == EventGeometry:
            value_to_insert["value"] = to_shape(value.value).wkt
        # end if

        # Increment position for footprints
        if value.parent_level == -1:
            position_footprints += 1
        # end if
        list_values_to_be_created[type(value)].append(value_to_insert)
    # end for

    # Update footprints
    if len(footprint_values) > 0 and len(satellite_values) > 0 and len(imaging_mode_values) > 0:
        satellite = satellite_values[0].value
        imaging_mode = imaging_mode_values[0].value
        events_to_obtain_footprint = {
            imaging_mode: [{
                "start": to_event["start"].isoformat(),
                "stop": to_event["stop"].isoformat(),
                "values": []
            }]
        }
        events = functions.associate_footprints(events_to_obtain_footprint, satellite, return_polygon_format = True)

        if not EventGeometry in list_values_to_be_created:
            list_values_to_be_created[EventGeometry] = []
        # end if
        if not EventObject in list_values_to_be_created:
            list_values_to_be_created[EventObject] = []
        # end if
        
        iterator = 0
        for footprint_object in events[0]["values"]:
            list_values_to_be_created[EventObject].append(
                {"event_uuid": to_event_uuid,
                 "name": "footprint_details_" + str(iterator),
                 "position": position_footprints,
                 "parent_level": -1,
                 "parent_position": 0
                 })
            list_values_to_be_created[EventGeometry].append(
                {"event_uuid": to_event_uuid,
                 "name": "footprint",
                 "position": 0,
                 "parent_level": 0,
                 "parent_position": position_footprints,
                 "value": footprint_object["values"][0]["value"]
                 })
            iterator += 1
            position_footprints += 1
        # end for
    # end if

    return
