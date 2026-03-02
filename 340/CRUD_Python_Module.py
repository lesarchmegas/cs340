from pymongo import MongoClient
from pymongo.errors import PyMongoError

class CRUD:
    """
    CRUD operations for a MongoDB collection.
    """

    def __init__(self, db_name="grazioso", username=None, password=None, host="localhost", port=27017):
        """
        Initialize MongoDB connection.
        If username/password are provided, use authentication.
        """
        try:
            if username and password:
                self.client = MongoClient(f"mongodb://{username}:{password}@{host}:{port}/")
            else:
                self.client = MongoClient(f"mongodb://{host}:{port}/")
            self.db = self.client[db_name]
            print(f"Connected to database '{db_name}' successfully.")
        except PyMongoError as e:
            print(f"Connection failed: {e}")
            self.db = None

    def create(self, collection_name, document):
        """
        Insert a single document into a collection.
        Returns True if successful, False otherwise.
        """
        try:
            self.db[collection_name].insert_one(document)
            return True
        except PyMongoError as e:
            print(f"Insert failed: {e}")
            return False

    def read(self, collection_name, query):
        """
        Query documents from a collection.
        Returns a list of documents matching the query.
        """
        try:
            cursor = self.db[collection_name].find(query)
            return list(cursor)
        except PyMongoError as e:
            print(f"Read failed: {e}")
            return []

    def update(self, collection_name, query, new_values, many=False):
        """
        Update documents in a collection.
        If many=True, update all matching documents; else update first match only.
        Returns the number of modified documents.
        """
        try:
            if many:
                result = self.db[collection_name].update_many(query, {"$set": new_values})
            else:
                result = self.db[collection_name].update_one(query, {"$set": new_values})
            return result.modified_count
        except PyMongoError as e:
            print(f"Update failed: {e}")
            return 0

    def delete(self, collection_name, query, many=False):
        """
        Delete documents from a collection.
        If many=True, delete all matching documents; else delete first match only.
        Returns the number of deleted documents.
        """
        try:
            if many:
                result = self.db[collection_name].delete_many(query)
            else:
                result = self.db[collection_name].delete_one(query)
            return result.deleted_count
        except PyMongoError as e:
            print(f"Delete failed: {e}")
            return 0
