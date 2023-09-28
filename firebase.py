import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


# initialize_firebase function is used to initialize the firebase app with our private SDK.
# Our other programs use this function to initialize the app
def initialize_firebase():
    cred = credentials.Certificate("software-eng-warmup-firebase-adminsdk-7iuf6-eb56ed9295.json")
    firebase_admin.initialize_app(cred)


# get_firestore_client function is used to retrive the client used to initialize db in other functions.
def get_firestore_client():
    return firestore.client()
