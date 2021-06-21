"""
Helper module for the ingestion_functions of files of Sentinel-1

Written by DEIMOS Space S.L. (dibb)

module s1boa
"""

# Import python utilities
import math
import datetime
from dateutil import parser
import subprocess
import os
from tempfile import mkstemp
import re
import glob
from itertools import chain

# Import xml parser
from lxml import etree

# Import astropy
from astropy.time import Time

# Import helpers
import eboa.ingestion.functions as ingestion_functions
import eboa.engine.functions as eboa_functions
from eboa.engine.functions import get_resources_path
import siboa.ingestions.functions as siboa_functions

# Import eboa query
from eboa.engine.query import Query

# Import debugging
from eboa.debugging import debug

# Import logging
from eboa.logging import Log
import logging

# Import errors
from s1boa.ingestions.errors import WrongDate, WrongSatellite

# Import ingestion_functions.helpers
import eboa.ingestion.functions as eboa_ingestion_functions

logging_module = Log(name = __name__)
logger = logging_module.logger

#########
# EOP CFI
#########
swath_definition = {
    "S1": "SDF_SAR1SM.S1",
    "S1_WO_CAL": "SDF_SAR1SM.S1",
    "NS1": "SDF_SAR1SM.S1",
    "S2": "SDF_SAR2SM.S1",
    "S2_WO_CAL": "SDF_SAR2SM.S1",
    "NS2": "SDF_SAR2SM.S1",
    "S3": "SDF_SAR3SM.S1",
    "S3_WO_CAL": "SDF_SAR3SM.S1",
    "NS3": "SDF_SAR3SM.S1",
    "S4": "SDF_SAR4SM.S1",
    "S4_WO_CAL": "SDF_SAR4SM.S1",
    "NS4": "SDF_SAR4SM.S1",
    "S4": "SDF_SAR4SM.S1",
    "S5N": "SDF_SAR5SM.S1",
    "S5S": "SDF_SAR5SM.S1",
    "S5N_WO_CAL": "SDF_SAR5SM.S1",
    "S5S_WO_CAL": "SDF_SAR5SM.S1",
    "NS5N": "SDF_SAR5SM.S1",
    "NS5S": "SDF_SAR5SM.S1",
    "S6": "SDF_SAR6SM.S1",
    "S6_WO_CAL": "SDF_SAR6SM.S1",
    "NS6": "SDF_SAR6SM.S1",
    "IW": "SDF_SARWIW.S1",
    "NIW": "SDF_SARWIW.S1",
    "WV": "SDF_SAR1WV.S1",
    "NWV": "SDF_SAR1WV.S1",
    "EW": "SDF_SARWEW.S1",
    "NEW": "SDF_SARWEW.S1",
    "RFC": "SDF_SAR1WV.S1",
}

def insert_event(event, events_per_imaging_mode, imaging_mode, source = None):

    if imaging_mode not in events_per_imaging_mode.keys():
        events_per_imaging_mode[imaging_mode] = []
    # end if
    
    if source:
        eboa_ingestion_functions.insert_event_for_ingestion(event, source, events_per_imaging_mode[imaging_mode])
    else:
        events_per_imaging_mode[imaging_mode].append(event)
    # end if

def build_orbpre_file_from_reference(start, stop, satellite):
    """
    Method to generate an orbpre file using the orbit reference file
    :param start: start date in ISO 8601 of the window to cover with the ORBPRE
    :type start: str
    :param stop: stop date in ISO 8601 of the window to cover with the ORBPRE
    :type stop: str

    """

    # Check parameters
    if not eboa_functions.is_datetime(start):
        raise WrongDate("The received start is not a valid date. Received date: {}".format(start))
    # end if

    if not eboa_functions.is_datetime(stop):
        raise WrongDate("The received stop is not a valid date. Received date: {}".format(stop))
    # end if

    if satellite == "S1A":
        satellite_for_orbpre = "SENTINEL_1A"
    elif satellite == "S1B":
        satellite_for_orbpre = "SENTINEL_1B"
    else:
        raise WrongSatellite("The received satellite is not recognized. Received satellite: {}".format(satellite))
    # end if

    # Create the ORBPRE file
    (_, orbpre_file_path) = mkstemp()

    # Fill the ORBPRE file
    start_window = (parser.parse(start) - datetime.timedelta(minutes=200)).isoformat(timespec="microseconds")
    stop_window = (parser.parse(stop) + datetime.timedelta(minutes=200)).isoformat(timespec="microseconds")

    orbit_reference_file_path = glob.glob(eboa_functions.get_resources_path() + "/{}*MPL_ORBSCT*".format(satellite))[0]
    orbpre_dir_path = os.path.split(orbpre_file_path)[0]
    orbpre_file_name = os.path.split(orbpre_file_path)[1]
    gen_pof_command = "gen_pof -sat {} -tref TAI -osvloc 0 -reftyp OSF -ref {} -poftyp POF -dir {} -pof {} -tastart '{}' -tastop '{}'".format(satellite_for_orbpre, orbit_reference_file_path, orbpre_dir_path, orbpre_file_name, start_window, stop_window)

    subprocess.check_output(gen_pof_command, shell=True)
    
    return orbpre_file_path

# Uncomment for debugging reasons
# @debug
def associate_footprints(events_per_imaging_mode, satellite, orbpre_events = None, return_polygon_format = False):
    FNULL = open(os.devnull, 'w')
    
    if not type(events_per_imaging_mode) == dict:
        raise EventsStructureIncorrect("The parameter events_per_imaging_mode has to be a list. Received events {}".format(events_per_imaging_mode))
    # end if

    if len(events_per_imaging_mode.keys()) == 0:
        logger.debug("There are no events for associating footprints")
        return []
    # end if
    all_events = list(chain.from_iterable([events_per_imaging_mode[imaging_mode] for imaging_mode in events_per_imaging_mode.keys()]))
    all_events.sort(key=lambda x:x["start"])    
    
    logger.debug("There are {} events for associating footprints".format(len(all_events)))

    logger.debug("The events for associating footprints cover from {} to {}".format(all_events[0]["start"], all_events[-1]["stop"]))
    
    events_with_footprint = []
    
    t0 = Time("2000-01-01T00:00:00", format='isot', scale='utc')

    orbpre_file_path = build_orbpre_file_from_reference(all_events[0]["start"], all_events[-1]["stop"], satellite)

    for imaging_mode in events_per_imaging_mode:
        events = events_per_imaging_mode[imaging_mode]
        swath_definition_file_path = eboa_functions.get_resources_path() + "/{}".format(swath_definition[imaging_mode])

        for event in events:

            if not type(event) == dict:
                os.remove(orbpre_file_path)
                raise EventsStructureIncorrect("The items of the events list has to be a dict. Received item {}".format(event))
            # end if
            footprint_details = []
            if "values" in event.keys():
                footprint_details = [value for value in event["values"] if re.match("footprint_details.*", value["name"])]
            # end if
            event_with_footprint = event.copy()

            if len(footprint_details) == 0:
                start = Time(event["start"], format='isot', scale='utc')
                stop = Time(event["stop"], format='isot', scale='utc')
                start_mjd = start.mjd - t0.mjd
                stop_mjd = stop.mjd - t0.mjd
                # The footprint is created if the segment duration is less than 100 minutes (other segments are discarded as they are not interesting)
                if (stop_mjd - start_mjd) < 0.0695:
                    # The step between coordinates is fixed to 3.608 as for S2 (which is the duration of a scene) demonstrates a good step value
                    iterations = int(((stop_mjd - start_mjd) * 24 * 60 * 60) / 3.608) + 1
                    if iterations > 200:
                        iterations = 200
                    # end if
                    get_footprint_command = "get_footprint -b {} -e {} -o '{} {}' -s {} -n {}".format(start_mjd, stop_mjd, orbpre_file_path, orbpre_file_path, swath_definition_file_path, iterations)
                    try:
                        footprint = subprocess.check_output(get_footprint_command, shell=True, stderr=FNULL)

                        # Prepare footprint
                        coordinates = footprint.decode("utf-8").replace(" \n", "")
                        footprints = siboa_functions.correct_footprint(coordinates)

                        for i, footprint in enumerate(footprints):

                            if not ("values" in event_with_footprint.keys() and len(event_with_footprint["values"]) > 0):
                                event_with_footprint["values"] = []
                            # end if

                            footprint_object_name = "footprint_details_" + str(i)

                            if return_polygon_format:
                                footprint = siboa_functions.obtain_polygon_format(footprint)
                            # end if

                            footprint_object = [{"name": "footprint",
                                                 "type": "geometry",
                                                 "value": footprint}]
                            event_with_footprint["values"].append({
                                "name": footprint_object_name,
                                "type": "object",
                                "values": footprint_object
                            })

                            if logger.getEffectiveLevel() == logging.DEBUG:
                                footprint_object.append({"name": "get_footprint_command",
                                                         "type": "text",
                                                         "value": get_footprint_command})
                            # end if
                        # end for
                    except subprocess.CalledProcessError:
                        logger.error("The footprint of the events could not be built because the command {} ended in error".format(get_footprint_command))
                    # end if
                else:
                    logger.info("The event with start {} and stop {} is too large".format(event["start"], event["stop"]))
                # end if
            # end if
            events_with_footprint.append(event_with_footprint)

        # end for
    # end for

    os.remove(orbpre_file_path)
        
    FNULL.close()

    logger.info("The number of events generated after associating the footprint is {}".format(len(events_with_footprint)))
    
    return events_with_footprint
