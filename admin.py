#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 17 16:15:34 2023

@author: annikamore
"""

from firebase import get_firestore_client, initialize_firebase
import json

# Call firebase functions to initialize and authorize firebase
initialize_firebase()
db = get_firestore_client()


# SkiResorts class will represent a top level collection item and upload the resorts top level collection
# information to firebase.
class SkiResorts:
    def __init__(self, uuid, location, resort, summit, ave_snow):
        self.uuid = uuid
        self.location = location
        self.resort = resort
        self.summit = summit
        self.ave_snow = ave_snow

    def upload_to_firestore(self):
        ski_resorts_ref = db.collection("Ski Resorts")
        document = ski_resorts_ref.document(self.uuid)

        # Check if the document already exists
        if document.get().exists:
            # Delete the existing document
            document.delete()

        document.set(
            {'Location': self.location, 'Resort': self.resort, 'Summit': self.summit, 'Snowfall': self.ave_snow})


# PopularTrails class will represent a sub collection item and upload the resorts sub collection
# information to firebase.
class PopularTrails:
    def __init__(self, uuid, trail_name, lov, t_type):
        self.uuid = uuid
        self.trail_name = trail_name
        self.lov = lov
        self.t_type = t_type

    def upload_to_firestore(self):
        parent_doc_ref = db.collection("Ski Resorts").document(self.uuid)

        parent_doc_ref.collection('Popular Trails').add(
            {'name': self.trail_name, 'difficulty': self.lov, 'type': self.t_type})


def main():
<<<<<<< HEAD
    
    while True:
        # Prompt the user for the name of the JSON file
        print("Provide the name of the JSON file: ")
        json_file = input()
        
        try:
            # Attempt to open and load the JSON file
            with open(json_file, 'r') as file:
                data = json.load(file)
            break  # Exit the loop if the file was successfully loaded
            
        except FileNotFoundError:
            print("File not found. Please provide a valid file name.")
            
        except json.JSONDecodeError:
            print("Invalid JSON format. Please provide a valid JSON file.")
    
  
    # Iterate through the data and upload to Firebase
    for item in data:
=======
    # Prompt the user for the name of the json file being used
    print("Provide the name of the JSON file. ")
    json_file = input()

    # Load data from JSON file
    with open(json_file, 'r') as json_file:
        data = json.load(json_file)

    # Iterate through the data and upload to Firebase
    for item in data:

>>>>>>> refs/remotes/origin/main
        ski_resort = SkiResorts(item['uuid'], item['Location'], item['Resort'], item['Summit'], item.get('Snowfall'))
        ski_resort.upload_to_firestore()

        # Check if 'Popular Trails' data is present
        if 'Popular Trails' in item:
            parent_uuid = item['uuid']  # Get the UUID of the ski resort

            # Delete the existing 'Popular Trails' subcollection once at the beginning
            parent_doc_ref = db.collection("Ski Resorts").document(parent_uuid)
            subcollection_ref = parent_doc_ref.collection('Popular Trails')
            docs = subcollection_ref.stream()
            for doc in docs:
                doc.reference.delete()

            for trail in item['Popular Trails']:
                popular_trail = PopularTrails(item['uuid'], trail['name'], trail['difficulty'], trail['type'])
                popular_trail.upload_to_firestore()
 

main()
