import csv
import argparse
import os
from datetime import datetime, timedelta
from decimal import Decimal
import re
from functools import lru_cache
from shapely.geometry import Point
from models import Checklist, Country, County, Locality, Location, Observation, Observer, Species, StateProvince, SubSpecies
from sqlalchemy import create_engine 
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import ClauseElement
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import  NoResultFound
from geoalchemy2.shape import from_shape

Base = declarative_base()
DBSession = scoped_session(sessionmaker())
engine = None

# How many TSV lines to batch up together. 10,000 seemed to be a good balance between db and parsing time in testing.
COMMIT_BATCH = 1000

# What version of the eBird metadata does this import script support?
EBIRD_METADATA_VERSION = "1.12"


def init_sqlalchemy(connection_url):
    global engine
    engine = create_engine(connection_url, echo=False)
    DBSession.remove()
    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

def get_or_create(session, model, defaults=None, **kwargs):
    """
    Code to emulate Django's get_or_create, adapted from: https://stackoverflow.com/questions/2546207/does-sqlalchemy-have-an-equivalent-of-djangos-get-or-create
    If the instance is already in the DB, we return the existing one.
    If it isn't, insert it into the DB and return the new one.
    Args:
        session (Session): SQLalchemy session
        model (Model): SQLalchemy model we want to use.
        defaults ([type], optional): [description]. Defaults to None.
    Returns:
        Returns a model instance and whether or not the object existed already or was created.
    """
    try:
        return session.query(model).filter_by(**kwargs).one(), False
    except NoResultFound:
        if defaults is not None:
            kwargs.update(defaults)
        try:
            with session.begin_nested():
                instance = model(**kwargs)
                session.add(instance)
                # session.commit()  # This doesn't work here, and I don't know why.
                return instance, True
        except IntegrityError:
            return session.query(model).filter_by(**kwargs).one(), False


def parse_ebird_taxonomy(file_path):
    """
    Used to parse the eBird_Taxonomy_*.csv to create species and subspecies database entries.
    Args:
        file_path (str): path of the csv file to open and parse.
    Returns:
        Two dictionaries:
            Species, with the key being a Decimal representation of the taxonomy order id, 
            and the values being dictionaries with the common_name and scientific_name.
            Subspecies, A dictionary with the key being a Decimal representation of the taxonomy order id, 
            and the values being dictionaries with the common_name and scientific_name, its category and its parent's scientific name.
            - spuh, issf and hybrid never have parents.
            - slash and intergrade will always have a parent.
            - form and domestic can both have and not have parents.
    """

    species = {}
    subspecies = {}
    species_codes = {}
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            common_name = row['PRIMARY_COM_NAME']
            scientific_name = row['SCI_NAME']
            taxonomic_order = row['TAXON_ORDER']
            category = row['CATEGORY']
            parent_code = row['REPORT_AS']
            species_code = row['SPECIES_CODE']
            species_codes[species_code] = (scientific_name, taxonomic_order)
            taxa = Decimal(taxonomic_order)
            # Don't check for existence because these all seem to be properly ordered.
            parent_scientific_name = None
            if parent_code != '':
                parent_scientific_name = species_codes[parent_code][0]
            if category == "species":
                species[taxa] = {"common_name": common_name, "scientific_name": scientific_name, "species_code": species_code}
            else:
                subspecies[taxa] = {"common_name": common_name, "scientific_name": scientific_name,
                                    "category": category, "parent_scientific_name": parent_scientific_name, "subspecies_code":  species_code}
    return species, subspecies


def parsed_taxa_csv_to_db(taxa_csv_file_path):
    """
    Creates species and subspecies instances in the database from the eBird taxonomy CSV file, after performing some much needed fixes.
    This creation is idempotent through the use of get_or_create() and can be run multiple times.
    Args:
        taxa_csv_file_path (str):  path of the csv file to open and parse.
    """
    cat = {'issf': 0, 'form': 1, 'domestic': 2, 'slash': 3, 'intergrade': 4, 'spuh': 5, 'hybrid': 6}

    species, subspecies = parse_ebird_taxonomy(taxa_csv_file_path)
    for k, v in species.items():
        scientific_name = v['scientific_name']
        common_name = v['common_name']
        taxonomic_order = k
        species_code =  v["species_code"]
        species_data = {'common_name':common_name, 'taxonomic_order': taxonomic_order, "species_code": species_code}
        _ = get_or_create(DBSession, Species, species_data, scientific_name=scientific_name)
    DBSession.commit()

    for k, v in subspecies.items():
        scientific_name = v['scientific_name']
        common_name = v['common_name']
        parent = v['parent_scientific_name']
        taxonomic_order = k
        category = cat[v['category']]
        subspecies_code =  v["subspecies_code"]
        supspecies_data = {'common_name': common_name, 'taxonomic_order': taxonomic_order, 'parent_species_id': parent, 'category':  category, "subspecies_code":  subspecies_code}
        _ = get_or_create(DBSession, SubSpecies, supspecies_data, scientific_name=scientific_name)
    DBSession.commit()



def parse_ebird_dump(file_path, start_row, taxa_csv_path=None):
    # Caching some common database ids so we don't have to do a SELECT every time we get them.
    country_code_cache = {}
    print(f"Start time: {curr_time()}")
    # Creates the species and subspecies entries in the database.
    if taxa_csv_path is not None:
        parsed_taxa_csv_to_db(taxa_csv_path)
        print(f"{curr_time()} Species and SubSpecies data added to database from taxonomy CSV.")

    species_sci_names = {x.scientific_name for x in DBSession.query(Species).all()}
    subspecies_sci_names = {x.scientific_name for x in DBSession.query(SubSpecies).all()}

    with open(file_path, 'r') as f:
        # QUOTE_NONE could be dangerous if there are tabs inside a field. For now, this assumes there isn't.
        reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
        count = 0
        err = None
        # Batch our database inserts/updates to keep from having a commit() every single call.
        # This could potentially lead to problems if we need to look up something that hasn't been committed yet, but it seems that the caching takes care of this. This could be a problem, in general.
        try:
            for row in reader:
                err = row
                if count < start_row:
                    count += 1
                    continue
                # Observation
                # ID in the data has the form of URN:CornellLabOfOrnithology:EBIRD:OBS######, and we want just the #s at the end for the id.
                observation_id = int(row['GLOBAL UNIQUE IDENTIFIER'].split(':')[-1][3:])
                # ID in the data has form 'S########' and we want just the #s at the end.
                checklist_id = int(row['SAMPLING EVENT IDENTIFIER'][1:])
                obs_count = row['OBSERVATION COUNT']
                if obs_count == 'X':
                    number_observed = None
                    is_x = True
                else:
                    number_observed = obs_count
                    is_x = False
                age_sex = row['AGE/SEX']
                species_comments = row['SPECIES COMMENTS']
                # breeding_atlas_code = row['BREEDING BIRD ATLAS CODE']
                # Species
                # taxonomic_order = decimal_or_none(row['TAXONOMIC ORDER'])
                species_category = row['CATEGORY']
                # common_name = row['COMMON NAME']
                scientific_name = row['SCIENTIFIC NAME']
                # subspecies_common_name = row['SUBSPECIES COMMON NAME']
                subspecies_scientific_name = row['SUBSPECIES SCIENTIFIC NAME']
                if subspecies_scientific_name == '':
                    subspecies_scientific_name = None
                # Conceptually, anything that isn't a 'species' is stored in the the SubSpecies model.
                # This differs from the ebird data where, for example, 'spuhs' are top-level species.
                # This is to massage the data to be more in line with that schema.
                if species_category in ('spuh', 'slash', 'hybrid'):
                    subspecies_scientific_name = scientific_name
                    scientific_name = None
                elif species_category in ('domestic', 'form'):
                    if scientific_name in subspecies_sci_names:
                        # subspecies_scientific_name = scientific_name
                        scientific_name = None
                # Checklist
                checklist_date = row['OBSERVATION DATE']
                checklist_time = row['TIME OBSERVATIONS STARTED']
                checklist_duration = row['DURATION MINUTES']
                start, duration = parse_start_duration(checklist_date, checklist_time, checklist_duration)
                checklist_comments = row['TRIP COMMENTS']
                distance = decimal_or_none(row['EFFORT DISTANCE KM'])
                area = decimal_or_none(row['EFFORT AREA HA'])
                number_of_observers = int_or_none(row['NUMBER OBSERVERS'])
                complete_checklist = bool(int(row['ALL SPECIES REPORTED']))
                # group id in the form of 'G######' but we want just #s.
                group_id = int_or_none(row['GROUP IDENTIFIER'][1:])
                approved = bool(int(row['APPROVED']))
                reviewed = bool(int(row['REVIEWED']))
                reason = row['REASON']
                protocol = row['PROTOCOL TYPE']
                proto = protocol_words_to_code(protocol)
                project_code = row['PROJECT CODE']
                # Observer
                # The is is in the form of 'obsr######' but we want just the #s.
                observer_id = int(row['OBSERVER ID'][4:])
                # Location
                lat = float(row['LATITUDE'])
                lon = float(row['LONGITUDE'])
                coords = from_shape(Point(lon, lat), srid=4326)  # PostGIS and SQLalchemy both expect this as a longitude/latitude pair.
                locality_name = row['LOCALITY']
                # In data in the form of 'L#######' but we want just #s.
                locality_id = int(row['LOCALITY ID'][1:])
                locality_type = row['LOCALITY TYPE']
                state_code = row['STATE CODE']
                county_code = row['COUNTY CODE']
                county = row['COUNTY']
                state_province = row['STATE']
                country = row['COUNTRY']
                country_code = row['COUNTRY CODE']
                has_media = bool(int(row['HAS MEDIA']))
                edit = row['LAST EDITED DATE']
                if edit != '':
                    last_edit = datetime.strptime(edit, "%Y-%m-%d %H:%M:%S")
                else:
                    last_edit = None
                # Start with the models that don't depend on other models and have single attributes.
                # All of these fields can potentially be blank.
                sp, _ = state_lru_cache_stub(state_province, state_code)

                cnty, _ = county_lru_cache_stub(county, county_code)

                local, _ = locality_lru_cache_stub(locality_id, locality_type, locality_name)

                fn = get_or_create
                kwargs = {'session': DBSession, 'model': Country, 'defaults': {'country': country}, 'country_code': country_code}
                cntry = create_or_cache(country_code_cache, fn, kwargs, country_code)

                if observer_id == '':
                    obs = None
                else:
                    obs, _ = observer_lru_cache_stub(observer_id)
                # Then continue with the models that only depend on the ones we've got.
                loc, _ = get_or_create(DBSession, Location,
                    defaults={'locality_id': locality_id, 'country_id': country_code, 'state_province_id': state_code, 'county_id': county_code},
                    coords=coords)
                # Next the checklist model
                check, _ = get_or_create(
                    DBSession,
                    Checklist,
                    defaults={
                        'location_id': loc.id, 'start_date_time': start, 'checklist_comments': checklist_comments,
                        'duration': duration, 'distance': distance, 'area': area,
                        'number_of_observers': number_of_observers, 'complete_checklist': complete_checklist,
                        'group_id': group_id, 'approved': approved, 'reviewed': reviewed, 'reason': reason,
                        'protocol': proto,
                        'project_code': project_code},
                    checklist=checklist_id
                    )
                # Finally the remaining models that depend on all the previous ones.
                # We don't care about the result here because get_or_create is being used to be idempotent.
                _, _ = get_or_create(
                    DBSession,
                    Observation,
                    defaults={
                        'number_observed': number_observed,
                        'is_x': is_x,
                        'age_sex': age_sex,
                        'species_comments': species_comments,
                        'species_id': scientific_name,
                        'subspecies_id': subspecies_scientific_name, #'breeding_atlas_code': breeding_atlas_code,
                        'date_last_edit': last_edit,
                        'has_media': has_media,
                        'checklist_id': check.checklist,
                        'observer_id': obs.observer_id},
                    observation=observation_id,
                    )

                count += 1
                if count % COMMIT_BATCH == 0:
                    DBSession.commit()
                    dt_stamp = curr_time()
                    print(f"{dt_stamp} Commit:  {count}")
                    cache_sizes = f"country: {len(country_code_cache)}"
                    print(cache_sizes)
                    lru_cache_stats = f"state: {state_lru_cache_stub.cache_info()}, county: {county_lru_cache_stub.cache_info()}, locality: {locality_lru_cache_stub.cache_info()}, obs: {observer_lru_cache_stub.cache_info()}"
                    print(lru_cache_stats)
        except KeyError as ex:
            print(f"Encountered unknown column {ex} in input data.")
            print(f"This importer only supports version {EBIRD_METADATA_VERSION}, please ensure your data is of this version as eBird makes changes to the dataset frequently.")
            raise ex
        except KeyboardInterrupt:
            print(f"Breaking due to crtl-c.")
            DBSession.commit()
        except Exception as ex:
            print(f"{curr_time()} Entries: {count}, CSV Line Number: {reader.line_num}.")
            print(err)
            DBSession.commit()
            raise ex
        # Making sure everything is definitely comitted.
        DBSession.commit()
        print(f"Final count: {count}, End time: {curr_time()}")


@lru_cache(maxsize=65536)
def state_lru_cache_stub(state, code):
    """
    This is just a wrapper around the get_or_create for State to be able to use lru_cache to cache the result. 
    """
    return get_or_create(DBSession, StateProvince, defaults={"state_province": state}, state_code=code)


@lru_cache(maxsize=65536)
def county_lru_cache_stub(county, code):
    """
    This is just a wrapper around the get_or_create for County to be able to use lru_cache to cache the result. 
    """
    return get_or_create(DBSession, County, defaults={"county": county}, county_code=code)


@lru_cache(maxsize=262144)
def locality_lru_cache_stub(l_id, l_type, name):
    """
    This is just a wrapper around the get_or_create for Locality to be able to use lru_cache to cache the result. 
    """
    return get_or_create(DBSession, Locality, defaults={'locality_type': l_type, 'locality_name': name}, locality_id=l_id)


@lru_cache(maxsize=262144)
def observer_lru_cache_stub(observer_id):
    """
    This is just a wrapper around the get_or_create for Observer to be able to use lru_cache to cache the result. 
    """
    return get_or_create(DBSession, Observer, defaults={}, observer_id=observer_id)


def curr_time():
    """
    Convenience function that returns the current date and time.
    """
    now = datetime.now()
    now_format = "%Y-%m-%d %H:%M:%S"
    return now.strftime(now_format)


def create_or_cache_or_none(cache, fn, kwargs, val):
    """
    Takes a cache, a get_or_create call and the value we want to create or get the id for.
    Returns the object's primary key either from the cache or from a newly created object.
    Will return None if val is ''.
    """
    if val == '':
        return None
    else:
        return create_or_cache(cache, fn, kwargs, val)


def create_or_cache(cache, fn, kwargs, val):
    """
    Takes a cache, a get_or_create call and the value we want to create or get the id for.
    Returns the object's primary key either from the cache or from a newly created object.
    """
    try:
        obj_id = cache[val]
    except KeyError:
        t, _ = fn(**kwargs)
        if len(kwargs) == 1:
            pk_attr = str(list(kwargs.keys())[0])
        else:
            pk_attr = [x for x in kwargs.keys() if x not in ("defaults", "model", "session")][0]
        obj_id = getattr(t, pk_attr)
        cache[val] = obj_id
    return obj_id


def parse_start_duration(checklist_date, checklist_time, checklist_duration):
    """
    Parses a checklist's start date and time and duration in minutes.
    Returns a datetime object for the start date and time and a timedelta for the duration
    Will return None in cases where we don't have enough information to determine these.
    """
    # No time and no date in ebird data.
    if checklist_duration == '':
        duration = None
    else:
        duration_mins = int(checklist_duration)
        duration = timedelta(minutes=duration_mins)
    # empty date and time
    if checklist_date == '' and checklist_time == '':
        start = None
    # Non-empty date but empty start time.
    elif checklist_date != '' and checklist_time == '':
        year, month, day = parse_date(checklist_date)
        start = datetime(year, month, day, 0, 0, 0, 0)
    else:
        year, month, day = parse_date(checklist_date)
        hour, minute, second = parse_time(checklist_time)
        start = datetime(year, month, day, hour, minute, second, 0)
    return start, duration


def parse_date(date_str):
    """
    Takes a string of the form mm-dd-yyyy (where the delimeter can be /, \ or - and returns year, month and day tuple.
    """
    year, month, day = [int(x) for x in re.split(r'[/\-]', date_str)]
    return year, month, day


def parse_time(time_str):
    """
    Takes a string of the form hh:mm:ss and returns (hours, minutes, seconds) tuple.
    """
    h, m, s = [int(x) for x in time_str.split(':')]
    return h, m, s


def decimal_or_none(d):
    if d == '':
        return None
    else:
        return Decimal(d)


def int_or_none(i):
    if i == '':
        return None
    else:
        return int(i)


def protocol_words_to_code(protocol):
    """
    Converts a protocol in words to the (arbitrary) 2 letter codes in the Django choices field.
    Args:
        protocol (str): eEbird textual description of protocol, such as 'Historical'
    Returns:
        A two character string that is used as the key for the choices field, for example 'HI'.
    """
    conversion = {
        'Incidental': '20',
        'Stationary': '21',
        'Traveling': '22',
        'Area': '23',
        'Trail Tracker': '30',
        'Banding': '33',
        'Waterbird Count': '34',
        'RMBO Early Winter Waterbird Count': '34',  # This is apparently an alias.
        'My Yard Counts': '35',
        'LoonWatch': '39',
        'Standardized Yard Count': '40',
        'Rusty Blackbird Spring Migration Blitz': '41',
        'Yellow-billed Magpie Survey - General Observations': '44',
        'Yellow-billed Magpie Survey - Traveling Count': '45',
        'CWC Point Count': '46',
        'CWC Area Search': '47',
        'Random': '48',
        'Coastal Shorebird Survey': '49',
        'Caribbean Martin Survey': '50',
        'Greater Gulf Refuge Waterbird Count': '51',
        'Oiled Birds': '52',
        'Nocturnal Flight Call Count': '54',
        'Heron Stationary Count*': '55',
        'Heron Area Count': '56',
        'Great Texas Birding Classic': '57',
        'Audubon Coastal Bird Survey': '58',
        'TNC California Waterbird Count': '59',
        'eBird Pelagic Protocol': '60',
        'IBA Canada (protocol)': '61',
        'Historical': '62',
        'Traveling - Property Specific': '64',
        'Breeding Bird Atlas': '65',
        "Birds 'n' Bogs Survey": '66',
        'CAC--Common Bird Survey': '67',
        'RAM--Iberian Seawatch Network': '68',
        'California Brown Pelican Survey': '69',
        'BirdLife Australia 20min-2ha survey': '70',
        'BirdLife Australia 500m radius search': '71',
        'BirdLife Australia 5 km radius search': '72',
        'PROALAS': '73',
        'International Shorebird Survey (ISS)': '74',
        'Tricolored Blackbird Winter Survey': '75',
        }
    return conversion[protocol]


def parse_command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', dest="input_file", help="Path to ebird datafile.", metavar="INFILE",
                        required=True)
    parser.add_argument('-r', '--row', dest="start_row", help="Start parsing at this row.", metavar="STARTROW",
                        required=False, default=0)
    parser.add_argument('-c', '--csv', dest="csv_path", help="Path to the ebird taxonomy csv.", metavar="CSVPATH",
                        required=False, default=None)
    parser.add_argument('-s', '--sqlalchemy', dest="connection_url", help="SQLAlchemy connection URL.", metavar="URL",
                        required=True)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    options = parse_command_line()
    input_file = options.input_file
    start_row = int(options.start_row)
    csv_path = options.csv_path
    connection_url = options.connection_url
    init_sqlalchemy(connection_url)
    parse_ebird_dump(input_file, start_row, csv_path)
