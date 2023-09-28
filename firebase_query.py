#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 13 16:50:15 2023

@author: abbeyknobel

take in parse as a list of terms
location == vermont and summit_height > 2000 ===> ['location','==','vermont','and','summit_height','>',2000]
need to talk to group about changing the json for all features being one word 
when we parse "Summit height" will turn into ['Summit', 'height] which may make it  difficult to use in firebase
if we rerun the code to upload to firestore- will it duplicate everything or just overwrite it
if overwritten then need to manually go through the json 
"""
from google.cloud.firestore_v1.base_query import FieldFilter, Or

from firebase import get_firestore_client, initialize_firebase

locations = ['vermont', 'new-york', 'colorado', 'california', 'utah', 'montana', 'michigan', 'west-virginia',
             'washington', 'quebec', 'new-mexico', 'alberta', 'maine', 'idaho', 'british-columbia', 'oregon',
             'new-hampshire', 'wyoming', 'ontario']

subcollection_fields = ['type', 'difficulty']

initialize_firebase()
db = get_firestore_client()
collection_ref = db.collection('Ski Resorts')

# Create a reference to the cities collection
ski_resorts_ref = db.collection("Ski Resorts")


def process_input(parse_output):
    output_length = len(parse_output)
    subcollection = False

    # if the output length is three then it must be a simple query
    if output_length == 3:
        field = parse_output[0]
        # check if query field is specific to sub-collections
        if field in subcollection_fields:
            subcollection = True
        operator = parse_output[1]
        value = parse_output[2]
        value = digit(value)
        # ex. "popular trails of vermont"
        if operator == "of" and value in locations:
            query = query_subcollection("Location", value)
        # ex. "popular trails of Killington
        elif operator == "of" and value not in locations:
            query = query_subcollection("Resort", value)
        # ex. "snowfall > 200"
        else:
            query = simple_query(field, operator, value, subcollection)
        return query

    # if the output has a length equal to 7 - it must be a compound input
    if output_length == 7:
        field_1 = parse_output[0]
        operator_1 = parse_output[1]
        value_1 = digit(parse_output[2])  # converting to an int if it is a numeric value
        logical_operator = parse_output[3]
        field_2 = parse_output[4]
        operator_2 = parse_output[5]
        value_2 = digit(parse_output[6])

        # if both fields are of sub-collection
        # ex. "type == groomer" and "difficulty == advanced"
        if (field_1 in subcollection_fields) and (field_2 in subcollection_fields):
            query = compound_query(field_1, operator_1, value_1, logical_operator, field_2, operator_2, value_2,
                                   subcollection=True)

        # if both fields are Snowfall or Summit
        # ex. "Snowfall > 200" and "Summit > 2000"
        elif field_1 in ["Snowfall", "Summit"] and field_2 in ["Snowfall", "Summit"] and field_1 != field_2:
            query = query_double_inequality(field_1, operator_1, value_1, logical_operator, field_2, operator_2,
                                            value_2)

        # if both statements are querying based on location
        # ex. "popular trails of vermont" or "popular trails of utah"
        elif operator_1 == "of" and operator_2 == "of":
            # loc_type is either "Location" or "Resort", set after checking if value is in locations
            loc_type = "Location" if value_1 in locations else "Resort"
            query = query_subcollection(loc_type, value_1, logical_operator, loc_type, "==", value_2)

        # if one statement is "of" and the other is a sub-collection field
        # ex. "Popular trails of vermont" and "difficulty == advanced"
        elif operator_1 == "of" and field_2 in subcollection_fields:
            loc_type = "Location" if value_1 in locations else "Resort"
            query = query_sub_field(loc_type, value_1, field_2, value_2)

        # reverse order of the previously handled query type
        # ex. "difficulty == advanced" and "popular trails of vermont"
        elif operator_2 == "of" and field_1 in subcollection_fields:
            loc_type = "Location" if value_2 in locations else "Resort"
            # we reverse the order of the query, so the query function will only ever get
            # "Popular trails of vermont" and "difficulty == advanced"
            # in that order, regardless of the original query order
            query = query_sub_field(loc_type, value_2, field_1, value_1)

        # if one statement is of, and other is not a sub-collection field
        # ex. "Popular trails of vermont" and "snowfall > 200"
        elif operator_1 == "of" and field_2 not in subcollection_fields:
            loc_type = "Location" if value_1 in locations else "Resort"
            query = query_subcollection(loc_type, value_1, logical_operator, field_2, operator_2, value_2)

        # reverse order of the previously handled query type
        # ex. "snowfall > 200" and "popular trails of vermont"
        elif operator_2 == "of" and field_1 not in subcollection_fields:
            loc_type = "Location" if value_2 in locations else "Resort"
            # similarly to above, we reverse the order of the query that is sent to the function for simplicity
            query = query_subcollection(loc_type, value_2, logical_operator, field_1, operator_1, value_1)

        # else it will be a compound query not involving the sub-collection
        # ex. "Location == vermont" and "snowfall > 200"
        # ex. "Location == vermont" or "Location == "utah"
        else:
            query = compound_query(field_1, operator_1, value_1, logical_operator, field_2, operator_2, value_2)
        return query


# function to take a query result and convert it to a list of dicts
def to_dict(query_result):
    query_list = []
    for doc in query_result:
        query_list.append(doc.to_dict())
    return query_list


# checks if a str is numeric and casts to int if true
# since the parser returns a list of strings, and the snowfall field requires a numeric value,
# we convert to an int when needed
def digit(string):
    if string.isdigit():
        return int(string)
    else:
        return string


# for queries with only one field, either super or sub-collection
# ex. "Location == vermont"
# ex. "type == groomer" (sub-collection query)
def simple_query(field_1, operator_1, value_1, subcollection=False):
    filter = FieldFilter(field_1, operator_1, value_1)
    collection = ski_resorts_ref if not subcollection else db.collection_group("Popular Trails")
    query = collection.where(filter=filter)
    docs = query.stream()
    if subcollection:
        docs = get_parent_collection(docs)
    return docs


# for queries with two fields, either super or sub-collection
# ex. "Popular trails of vermont" and "difficulty == advanced"
# ex. "Popular trails of vermont" and "snowfall > 200"
# ex. "Location == vermont" or "Location == "utah"
def compound_query(field_1, operator_1, value_1, logical_op, field_2, operator_2, value_2, subcollection=False):
    # creating filters
    filter_1 = FieldFilter(field_1, operator_1, value_1)
    filter_2 = FieldFilter(field_2, operator_2, value_2)
    # determining which collection/collection_group to use based on the subcollection flag
    collection = ski_resorts_ref if not subcollection else db.collection_group("Popular Trails")

    # call queries
    if logical_op == "and":
        query = collection.where(filter=filter_1).where(filter=filter_2)
    elif logical_op == "or":
        or_filter = Or(filters=[filter_1, filter_2])
        query = collection.where(filter=or_filter)
    else:
        return None
    docs = query.stream()
    if subcollection:
        docs = get_parent_collection(docs)
    return docs


# given a sub-collection query result, get parent collection data
def get_parent_collection(subcoll_query_stream):
    parent_list = []
    for doc in subcoll_query_stream:
        collection = doc.reference.parent
        collection_ref = collection.parent
        resort = collection_ref.get()
        parent_list.append(resort.to_dict())
    return parent_list


# specifically for sub-collection queries, determine which query function to use (simple or compound)
def query_subcollection(field_1, value_1, logical_operator=None, field_2=None, operator=None, value_2=None):
    query_list = []
    if logical_operator is None:
        query = simple_query(field_1, "==", value_1)
    else:
        query = compound_query(field_1, "==", value_1, logical_operator, field_2, operator, value_2)
    # get popular trails from query results
    for document in query:
        sub_collection = ski_resorts_ref.document(document.id).collection("Popular Trails")
        sub_collection_stream = sub_collection.stream()
        for doc in sub_collection_stream:
            query_list.append(doc.to_dict())
    return query_list


# for sub-collection queries with form "popular trails of vermont" and "difficulty == advanced"
def query_sub_field(field_1, value_1, field_2, value_2):
    sub_list = []
    # first get all popular trails of vermont
    query_list = query_subcollection(field_1, value_1)
    for dictionary in query_list:
        # then search all popular trails of vermont to get those with difficulty == advanced
        if dictionary[field_2] == value_2:
            sub_list.append(dictionary)
    return sub_list


# if both fields are either Snowfall or Summit (firebase doesn't have a native function to query both)
# ex. "Snowfall > 400" and "Summit > 8000"
def query_double_inequality(field_1, operator_1, value_1, logical_operator, field_2, operator_2, value_2):
    # querying based on each individual field first
    query_1 = to_dict(simple_query(field_1, operator_1, value_1))
    query_2 = to_dict(simple_query(field_2, operator_2, value_2))
    # combining based on passed logical operator
    if logical_operator == "and":
        # list comprehension to get all elements present in both query results
        return [item for item in query_1 if item in query_2]
    elif logical_operator == "or":
        # list comprehension to get all elements in both lists, without duplicates
        return query_1 + [item for item in query_2 if item not in query_1]
    else:
        return None


def test_cases():
    print("SIMPLE QUERIES\n")
    print("TEST ONE: 'Location == Vermont'")
    print(to_dict(process_input(["Location", "==", "vermont"])))

    print("TEST TWO: 'Snowfall > 200'")
    print(to_dict(process_input(["Snowfall", ">", "200"])))

    print("TEST THREE: 'difficulty == advanced'")
    print(process_input(["difficulty", "==", "Beginner"]))

    print("TEST FOUR: 'type == groomer'")
    print(process_input(["type", "==", "Groomer"]))

    print("TEST FIVE: 'Popular Trails of vermont'")
    print(process_input(["Popular Trails", "of", "vermont"]))

    print("TEST SIX: 'Popular Trails of Killington'")
    print(process_input(["Popular Trails", "of", "Killington"]))

    print("COMPOUND QUERIES\n")
    print("TEST SEVEN: 'difficulty == Advanced AND type == Glades'")
    print(process_input(["difficulty", "==", "Advanced", "and", "type", "==", "Glades"]))

    print("TEST EIGHT: 'difficulty == Advanced OR type == Glades'")
    print(process_input(["difficulty", "==", "Advanced", "or", "type", "==", "Glades"]))

    print("TEST NINE: 'Popular Trails of vermont AND type == Glades'")
    print(process_input(["Popular Trails", "of", "vermont", "and", "type", "==", "Glades"]))

    print("TEST TEN: 'type == Glades AND Popular Trails of vermont'")
    print(process_input(["type", "==", "Glades", "and", "Popular Trails", "of", "vermont"]))

    print("TEST ELEVEN: 'Popular Trails of montana OR Popular Trails of vermont'")
    print(process_input(["Popular Trails", "of", "montana", "or", "Popular Trails", "of", "vermont"]))

    print("TEST TWELVE: 'Popular Trails of vermont AND snowfall > 200'")
    print(process_input(["Popular Trails", "of", "vermont", "and", "Snowfall", ">", "200"]))

    print("TEST THIRTEEN: 'Snowfall > 200 AND Popular Trails of montana'")
    print(process_input(["Snowfall", ">", "200", "and", "Popular Trails", "of", "vermont"]))

    print("TEST FOURTEEN: 'Snowfall > 200 OR Popular Trails of montana'")
    print(process_input(["Snowfall", ">", "200", "or", "Popular Trails", "of", "vermont"]))


#test_cases()
