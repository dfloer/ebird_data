from sqlalchemy import ARRAY, Boolean, CheckConstraint, Column, DateTime, Float, ForeignKey, Integer, Numeric, SmallInteger, String, Table, Text, UniqueConstraint, text
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()
metadata = Base.metadata


class Country(Base):
    __tablename__ = 'country'

    country_code = Column(Text, primary_key=True, index=True)
    country = Column(Text, nullable=False, unique=True)


class County(Base):
    __tablename__ = 'county'

    county = Column(Text, primary_key=True, index=True)
    county_code = Column(Text, nullable=False, unique=True)


class Locality(Base):
    __tablename__ = 'locality'

    locality_name = Column(Text, nullable=False)
    locality_id = Column(Integer, primary_key=True)
    locality_type = Column(String(2), nullable=False)


class Observer(Base):
    __tablename__ = 'observer'

    observer_id = Column(Integer, primary_key=True)
    first_name = Column(Text, nullable=False)
    last_name = Column(Text, nullable=False)


class Species(Base):
    __tablename__ = 'species'

    common_name = Column(Text, nullable=False)
    scientific_name = Column(Text, primary_key=True, index=True)
    taxonomic_order = Column(Numeric(20, 10), nullable=False)


class StateProvince(Base):
    __tablename__ = 'stateprovince'

    state_province = Column(Text, primary_key=True, index=True)
    state_code = Column(Text, nullable=False, unique=True)


class Location(Base):
    __tablename__ = 'location'

    id = Column(Integer, primary_key=True)
    coords = Column(Geometry(geometry_type='POINT', srid=4326), nullable=False, index=True)
    country_id = Column(ForeignKey('country.country_code', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    county_id = Column(ForeignKey('county.county', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    locality_id = Column(ForeignKey('locality.locality_id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    state_province_id = Column(ForeignKey('stateprovince.state_province', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    country = relationship('Country')
    county = relationship('County')
    locality = relationship('Locality')
    state_province = relationship('StateProvince')


class subspecies(Base):
    __tablename__ = 'subspecies'

    common_name = Column(Text, nullable=False)
    scientific_name = Column(Text, primary_key=True, index=True)
    taxonomic_order = Column(Numeric(20, 10), nullable=False)
    category = Column(Integer)
    parent_species_id = Column(ForeignKey('species.scientific_name', deferrable=True, initially='DEFERRED'), index=True)

    parent_species = relationship('Species')


class Checklist(Base):
    __tablename__ = 'checklist'

    start_date_time = Column(DateTime(True))
    checklist_comments = Column(Text, nullable=False)
    checklist = Column(Integer, primary_key=True)
    duration = Column(INTERVAL)
    distance = Column(Numeric(16, 6))
    area = Column(Numeric(16, 6))
    number_of_observers = Column(Integer)
    complete_checklist = Column(Boolean, nullable=False)
    group_id = Column(Integer)
    approved = Column(Boolean, nullable=False)
    reviewed = Column(Boolean, nullable=False)
    reason = Column(Text, nullable=False)
    protocol = Column(String(2))
    location_id = Column(ForeignKey('location.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    location = relationship('Location')


class Observation(Base):
    __tablename__ = 'observation'

    observation = Column(Integer, primary_key=True)
    number_observed = Column(Integer)
    is_x = Column(Boolean, nullable=False)
    age_sex = Column(Text, nullable=False)
    species_comments = Column(Text)
    breeding_atlas_code = Column(String(2))
    date_last_edit = Column(DateTime(True))
    has_media = Column(Boolean, nullable=False)
    checklist_id = Column(ForeignKey('checklist.checklist', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    observer_id = Column(ForeignKey('observer.observer_id', deferrable=True, initially='DEFERRED'), index=True)
    species_id = Column(ForeignKey('species.scientific_name', deferrable=True, initially='DEFERRED'), index=True)
    subspecies_id = Column(ForeignKey('subspecies.scientific_name', deferrable=True, initially='DEFERRED'), index=True)

    checklist = relationship('Checklist')
    observer = relationship('Observer')
    species = relationship('Species')
    subspecies = relationship('SubSpecies')
